import os
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "tokens.env"))

import random
import datetime
import re
import json
import base64
import asyncio
import logging
import aiohttp

from llm import ask_model, init_supabase, push_to_cache, push_to_group_cache

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    filters,
    ContextTypes
)

# --- Настройки Telegram ---
TOKEN = os.environ["TELEGRAM_TOKEN"]
CHANNEL_ID = "@sharapjoker"
CHAT_ID = -4261289815

ALL_EMOJIS = [
    "😡","😐","❤️","🤣","✍️","💩","🤡","🔥","😭","🤓",
    "😎","🤯","👍","🤔","😈","🤮","🌚","💅","🏆","🤨",
    "🖕","😢","🥴"
]

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hilarious_stuff")
os.makedirs(DATA_DIR, exist_ok=True)

MSK = datetime.timezone(datetime.timedelta(hours=3))

# --- Supabase настройки ---
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

init_supabase(SUPABASE_URL, SUPABASE_KEY)

# --- Grafana Loki ---
LOKI_URL = "https://logs-prod-025.grafana.net/loki/api/v1/push"
LOKI_USER = os.environ["LOKI_USER"]
LOKI_API_TOKEN = os.environ["LOKI_API_TOKEN"]
auth_b64 = base64.b64encode(f"{LOKI_USER}:{LOKI_API_TOKEN}".encode()).decode()
LOKI_HEADERS = {
    "Authorization": f"Basic {auth_b64}",
    "Content-Type": "application/json"
}

# --- Глобальные переменные ---
jokes: list[str] = []
_http_session: aiohttp.ClientSession | None = None


def get_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        connector = aiohttp.TCPConnector(force_close=True)
        _http_session = aiohttp.ClientSession(connector=connector)
    return _http_session


# ------------------- Логирование -------------------

class AsyncGrafanaLokiHandler(logging.Handler):
    def __init__(self, loki_url, headers):
        super().__init__()
        self.loki_url = loki_url
        self.headers = headers

    async def _send(self, log_entry, metadata: dict | None = None):
        ts = str(int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1e9))
        value = [ts, log_entry, metadata] if metadata else [ts, log_entry]
        payload = {
            "streams": [{
                "stream": {"job": "telegram_bot"},
                "values": [value]
            }]
        }
        try:
            session = get_session()
            async with session.post(self.loki_url, headers=self.headers, data=json.dumps(payload, ensure_ascii=False)) as resp:
                if resp.status >= 400:
                    print(f"Loki error {resp.status}: {await resp.text()}")
        except Exception as e:
            print(f"Ошибка отправки лога в Grafana Loki: {e}")
            global _http_session
            if _http_session and not _http_session.closed:
                try:
                    await _http_session.close()
                except Exception:
                    pass
            _http_session = None

    def emit(self, record):
        log_entry = self.format(record)
        metadata = getattr(record, "metadata", None)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._send(log_entry, metadata))
        except RuntimeError:
            print(log_entry)


logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)
handler = AsyncGrafanaLokiHandler(LOKI_URL, LOKI_HEADERS)
handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(handler)

logger.info("Бот стартует...")


# ------------------- Supabase: upsert справочников -------------------

async def upsert_user(user_id: int, username: str | None) -> None:
    payload = {
        "user_id": user_id,
        "username": username or "",
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    try:
        session = get_session()
        async with session.post(
            f"{SUPABASE_URL}/rest/v1/users",
            headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.error(f"Supabase upsert_user error: {resp.status} {await resp.text()}")
    except Exception as e:
        logger.error(f"Ошибка upsert_user: {e}")


async def upsert_chat(chat_id: int, chat_name: str | None) -> None:
    payload = {
        "chat_id": chat_id,
        "chat_name": chat_name or "",
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    try:
        session = get_session()
        async with session.post(
            f"{SUPABASE_URL}/rest/v1/chats",
            headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.error(f"Supabase upsert_chat error: {resp.status} {await resp.text()}")
    except Exception as e:
        logger.error(f"Ошибка upsert_chat: {e}")


# ------------------- Supabase: сохранение статистики -------------------

async def save_stat(user_id, chat_id, command_name, response: str | None = None, sent_at: str | None = None):
    payload = [{
        "user_id": user_id,
        "chat_id": chat_id,
        "command_name": command_name,
        "response": response,
        "sent_at": sent_at
    }]
    try:
        session = get_session()
        async with session.post(
            f"{SUPABASE_URL}/rest/v1/joke_stats",
            headers=SUPABASE_HEADERS,
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.error(f"Supabase joke_stats error: {resp.status} {await resp.text()}")
    except Exception as e:
        logger.error(f"Ошибка сохранения joke_stats: {e}")


async def save_emoji(user_id, chat_id, emoji, message_id, message_text, sent_at: str | None = None):
    payload = [{
        "user_id": user_id,
        "chat_id": chat_id,
        "emoji": emoji,
        "message_id": message_id,
        "message_text": message_text,
        "sent_at": sent_at,
        "reacted_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }]
    try:
        session = get_session()
        async with session.post(
            f"{SUPABASE_URL}/rest/v1/emoji_stats",
            headers=SUPABASE_HEADERS,
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.error(f"Supabase emoji_stats error: {resp.status} {await resp.text()}")
    except Exception as e:
        logger.error(f"Ошибка сохранения emoji_stats: {e}")


# ------------------- Supabase: сохранение переписок -------------------

async def save_chat_private(user_id, message, response, sent_at: str | None = None):
    payload = [{
        "user_id": user_id,
        "message": message,
        "response": response,
        "sent_at": sent_at
    }]
    try:
        session = get_session()
        async with session.post(
            f"{SUPABASE_URL}/rest/v1/private_chat_history",
            headers=SUPABASE_HEADERS,
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.error(f"Supabase private_chat error: {resp.status} {await resp.text()}")
            else:
                logger.info(f"Saved private chat: UserID={user_id}")
    except Exception as e:
        logger.error(f"Ошибка сохранения личной переписки: {e}")


async def save_chat_group(user_id, chat_id, message, response, sent_at: str | None = None):
    payload = [{
        "user_id": user_id,
        "chat_id": chat_id,
        "message": message,
        "response": response,
        "sent_at": sent_at
    }]
    try:
        session = get_session()
        async with session.post(
            f"{SUPABASE_URL}/rest/v1/group_chat_history",
            headers=SUPABASE_HEADERS,
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.error(f"Supabase group_chat error: {resp.status} {await resp.text()}")
            else:
                logger.info(f"Saved group chat: UserID={user_id}, ChatID={chat_id}")
    except Exception as e:
        logger.error(f"Ошибка сохранения групповой переписки: {e}")


# ------------------- Supabase: утро и канал -------------------

async def save_morning_log(chat_id, greeting_text, photo_filename: str | None, audio_filename: str | None):
    payload = [{
        "chat_id": chat_id,
        "greeting_text": greeting_text,
        "photo_filename": photo_filename,
        "audio_filename": audio_filename,
        "sent_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }]
    try:
        session = get_session()
        async with session.post(
            f"{SUPABASE_URL}/rest/v1/morning_log",
            headers=SUPABASE_HEADERS,
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.error(f"Supabase morning_log error: {resp.status} {await resp.text()}")
    except Exception as e:
        logger.error(f"Ошибка сохранения morning_log: {e}")


async def save_channel_log(channel_id, text):
    payload = [{
        "channel_id": channel_id,
        "text": text,
        "sent_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }]
    try:
        session = get_session()
        async with session.post(
            f"{SUPABASE_URL}/rest/v1/channel_log",
            headers=SUPABASE_HEADERS,
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.error(f"Supabase channel_log error: {resp.status} {await resp.text()}")
    except Exception as e:
        logger.error(f"Ошибка сохранения channel_log: {e}")


# ------------------- Supabase: управление утренней рассылкой -------------------

async def set_morning_enabled(chat_id: int, enabled_by: int, enabled: bool) -> None:
    payload = {
        "morning_enabled": enabled,
        "morning_enabled_by": enabled_by,
        "morning_enabled_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    try:
        session = get_session()
        async with session.patch(
            f"{SUPABASE_URL}/rest/v1/chats?chat_id=eq.{chat_id}",
            headers=SUPABASE_HEADERS,
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.error(f"Supabase set_morning error: {resp.status} {await resp.text()}")
    except Exception as e:
        logger.error(f"Ошибка set_morning_enabled: {e}")


async def get_morning_chats() -> list[int]:
    try:
        session = get_session()
        async with session.get(
            f"{SUPABASE_URL}/rest/v1/chats?morning_enabled=eq.true&select=chat_id",
            headers=SUPABASE_HEADERS
        ) as resp:
            if resp.status == 200:
                rows = await resp.json()
                return [r["chat_id"] for r in rows]
    except Exception as e:
        logger.error(f"Ошибка get_morning_chats: {e}")
    return []


# ------------------- Загрузка анекдотов -------------------

def clean_block_lines(block: str) -> str:
    lines = []
    for line in block.splitlines():
        line = line.strip()
        if not line or line.startswith(">>") or line.startswith("---") or "|" in line:
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def is_valid_joke(block: str) -> bool:
    forbidden_patterns = [r"\[(photo|poll|file|link|audio|video)\]", r"\b(широков|божок|белоконь|штамм)\b"]
    return not any(re.search(p, block, re.IGNORECASE) for p in forbidden_patterns)


def load_jokes() -> list[str]:
    file_path = os.path.join(DATA_DIR, "all_jokes.txt")
    if not os.path.exists(file_path):
        logger.warning(f"Файл с анекдотами не найден: {file_path}")
        return []

    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()

    result = []
    for block in re.split(r"=+\n", text):
        clean = clean_block_lines(block)
        if not clean or not is_valid_joke(clean):
            continue
        if clean.startswith("#") or "Очень смешные анекдоты" in clean:
            continue
        if len(clean.split()) < 3:
            continue
        result.append(clean)

    logger.info(f"Загружено анекдотов: {len(result)}")
    return result


# ------------------- Эмодзи реакции с 20% шансом -------------------

MAX_PROCESSED = 10_000
processed_messages: set[tuple] = set()
processed_messages_list: list[tuple] = []


async def add_random_reaction(chat_id, message_id, message_text="", user_id=None, chat_type="private", sent_at=None):
    key = (chat_id, message_id)
    if key in processed_messages:
        return

    if random.random() >= 0.2:
        return

    try:
        emoji = random.choice(ALL_EMOJIS)
        api_url = f"https://api.telegram.org/bot{TOKEN}/setMessageReaction"
        session = get_session()
        async with session.post(api_url, json={
            "chat_id": chat_id,
            "message_id": message_id,
            "reaction": [{"type": "emoji", "emoji": emoji}]
        }) as resp:
            if resp.status == 200:
                if user_id:
                    await save_emoji(user_id, chat_id, emoji, message_id, message_text, sent_at=sent_at)

                if len(processed_messages) >= MAX_PROCESSED:
                    oldest = processed_messages_list.pop(0)
                    processed_messages.discard(oldest)
                processed_messages.add(key)
                processed_messages_list.append(key)

                logger.info(f"Emoji '{emoji}' in ChatID={chat_id}, MessageID={message_id}")
            else:
                logger.warning(f"Не удалось поставить emoji: {resp.status} {await resp.text()}")
    except Exception as e:
        logger.error(f"Ошибка реакции: {e}")


# ------------------- Команды анекдотов -------------------

def _push_command_context(update: Update, command: str, text: str) -> None:
    """Сохраняет анекдот в контекст диалога чтобы бот помнил его при реплае."""
    user = update.message.from_user
    chat_type = update.message.chat.type
    user_message = f"/{command}"
    if chat_type == "private":
        push_to_cache(user.id, user_message, text)
    else:
        push_to_group_cache(update.message.chat.id, user.id, user_message, text)


async def joke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not jokes:
        await update.message.reply_text("Не удалось найти анекдоты")
        return
    text = random.choice(jokes)
    await update.message.reply_text(text)
    sent_at = update.message.date.isoformat() if update.message.date else None
    user = update.message.from_user
    await upsert_user(user.id, user.username)
    await save_stat(user.id, update.message.chat.id, "joke", response=text, sent_at=sent_at)
    _push_command_context(update, "joke", text)
    logger.info(f"Command: joke | UserID={user.id} | ChatID={update.message.chat.id}")


async def yasno(update: Update, context: ContextTypes.DEFAULT_TYPE):
    filtered = [j for j in jokes if re.sub(r"[\s\.\!\?\)\]\}]+$", "", j.lower()).endswith("ясно")]
    if not filtered:
        await update.message.reply_text("Не удалось найти анекдоты на 'ясно'")
        return
    text = random.choice(filtered)
    await update.message.reply_text(text)
    sent_at = update.message.date.isoformat() if update.message.date else None
    user = update.message.from_user
    await upsert_user(user.id, user.username)
    await save_stat(user.id, update.message.chat.id, "yasno", response=text, sent_at=sent_at)
    _push_command_context(update, "yasno", text)
    logger.info(f"Command: yasno | UserID={user.id} | ChatID={update.message.chat.id}")


async def chips(update: Update, context: ContextTypes.DEFAULT_TYPE):
    filtered = [j for j in jokes if re.search(r"\bщеп\w*", j.lower())]
    if not filtered:
        await update.message.reply_text("Не удалось найти анекдоты со словом 'щепка'")
        return
    text = random.choice(filtered)
    await update.message.reply_text(text)
    sent_at = update.message.date.isoformat() if update.message.date else None
    user = update.message.from_user
    await upsert_user(user.id, user.username)
    await save_stat(user.id, update.message.chat.id, "chips", response=text, sent_at=sent_at)
    _push_command_context(update, "chips", text)
    logger.info(f"Command: chips | UserID={user.id} | ChatID={update.message.chat.id}")


async def spy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    spy_file = os.path.join(DATA_DIR, "spy.txt")
    all_jokes = jokes.copy()
    if os.path.exists(spy_file):
        with open(spy_file, "r", encoding="utf-8") as f:
            all_jokes += [j.strip() for j in f.read().split("\n\n") if j.strip()]
    filtered = [j for j in all_jokes if re.search(r"\bштирл\w*", j.lower())]
    if not filtered:
        await update.message.reply_text("Не удалось найти анекдоты про Штирлица")
        return
    text = random.choice(filtered)
    await update.message.reply_text(text)
    sent_at = update.message.date.isoformat() if update.message.date else None
    user = update.message.from_user
    await upsert_user(user.id, user.username)
    await save_stat(user.id, update.message.chat.id, "spy", response=text, sent_at=sent_at)
    _push_command_context(update, "spy", text)
    logger.info(f"Command: spy | UserID={user.id} | ChatID={update.message.chat.id}")


async def roll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = random.randint(0, 10)
    response = f"🎲 {result}"
    await update.message.reply_text(response)
    sent_at = update.message.date.isoformat() if update.message.date else None
    user = update.message.from_user
    await upsert_user(user.id, user.username)
    await save_stat(user.id, update.message.chat.id, "roll", response=response, sent_at=sent_at)
    logger.info(f"Command: roll | UserID={user.id} | ChatID={update.message.chat.id}")


# ------------------- Команды утренней рассылки -------------------

async def morning_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    chat = update.message.chat
    chat_name = chat.title or chat.full_name or str(chat.id)

    await upsert_user(user.id, user.username)
    await upsert_chat(chat.id, chat_name)
    await set_morning_enabled(chat.id, user.id, True)

    await update.message.reply_text("🌅 Утренние приветствия включены!")
    logger.info(f"Morning ON | UserID={user.id} | ChatID={chat.id}")


async def morning_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    chat = update.message.chat

    await upsert_user(user.id, user.username)
    await set_morning_enabled(chat.id, user.id, False)

    await update.message.reply_text("🌙 Утренние приветствия отключены.")
    logger.info(f"Morning OFF | UserID={user.id} | ChatID={chat.id}")


# ------------------- Ответ на сообщения -------------------

async def reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    chat_type = update.message.chat.type
    user = update.message.from_user
    sent_at = update.message.date.isoformat() if update.message.date else None

    await upsert_user(user.id, user.username)

    asyncio.create_task(add_random_reaction(
        update.message.chat.id,
        update.message.message_id,
        message_text=text,
        user_id=user.id,
        chat_type=chat_type,
        sent_at=sent_at
    ))

    if chat_type == "private":
        try:
            full_text = text
            if update.message.reply_to_message:
                replied = update.message.reply_to_message.text or update.message.reply_to_message.caption
                if replied:
                    full_text = f"Вот сообщение:\n{replied}\n\nВопрос: {text}"

            answer = await ask_model(
                full_text,
                user_id=user.id,
                chat_id=user.id,
                username=user.username or user.first_name or "Пользователь",
                is_group=True,
            )
            await update.message.reply_text(answer)
            asyncio.create_task(save_chat_private(
                user.id, text, answer, sent_at=sent_at
            ))
            logger.info(f"Private reply | UserID={user.id}", extra={"metadata": {"user_message": text, "bot_reply": answer}})
        except Exception as e:
            logger.error(f"Ошибка AI в личке: {e}")
        return

    # В группе — отвечаем при @упоминании ИЛИ при реплае на сообщение бота
    should_reply = False
    cleaned = text

    # 1. @упоминание бота
    if update.message.entities:
        for entity in update.message.entities:
            if entity.type == "mention":
                mention = update.message.text[entity.offset:entity.offset + entity.length]
                if mention.lower() == f"@{context.bot.username.lower()}":
                    cleaned = update.message.text.replace(mention, "").strip()

                    post_text = None
                    if update.message.reply_to_message:
                        post_text = update.message.reply_to_message.text or update.message.reply_to_message.caption

                    if post_text:
                        if cleaned:
                            cleaned = f"Вот пост:\n{post_text}\n\nВопрос: {cleaned}"
                        else:
                            cleaned = f"Вот пост:\n{post_text}\n\nЧто думаешь об этом?"
                    else:
                        cleaned = cleaned or "Ответь что-нибудь"

                    should_reply = True
                    break

    # 2. Реплай на сообщение бота
    if (not should_reply
            and update.message.reply_to_message
            and update.message.reply_to_message.from_user
            and update.message.reply_to_message.from_user.id == context.bot.id):
        should_reply = True
        cleaned = text or "Ответь что-нибудь"

    if should_reply:
        chat = update.message.chat
        await upsert_chat(chat.id, chat.title or "")
        try:
            answer = await ask_model(
                cleaned,
                user_id=user.id,
                chat_id=chat.id,
                username=user.username or user.first_name or "Пользователь",
                is_group=True,
            )
            await update.message.reply_text(answer)
            asyncio.create_task(save_chat_group(
                user.id, chat.id, cleaned, answer, sent_at=sent_at
            ))
            logger.info(f"Group reply | UserID={user.id} | ChatID={chat.id}", extra={"metadata": {"user_message": cleaned, "bot_reply": answer}})
        except Exception as e:
            logger.error(f"Ошибка AI в группе: {e}")
        return


# ------------------- JobQueue -------------------

async def morning_job(context):
    import glob

    now = datetime.datetime.now(MSK)
    if now.weekday() in (5, 6):
        prompt = "Пожелай хороших выходных в своём стиле — кратко, язвительно. Не повторяйся."
    else:
        prompt = "Пожелай доброго рабочего утра группе в своём стиле — кратко, язвительно. Не повторяйся."

    chat_ids = await get_morning_chats()
    if not chat_ids:
        logger.info("Morning job: нет подписанных чатов")
        return

    pics = glob.glob(os.path.join(BASE_DIR, "rooster", "pics", "*.jpg"))
    audio = glob.glob(os.path.join(BASE_DIR, "rooster", "audio", "*.mp3"))

    logger.info(f"Morning job started | chats={len(chat_ids)} | pics={len(pics)} | audio={len(audio)}")

    photo_path = random.choice(pics) if pics else None
    audio_path = random.choice(audio) if audio else None

    for chat_id in chat_ids:
        try:
            greeting = await ask_model(prompt)

            if photo_path:
                with open(photo_path, "rb") as f:
                    await context.bot.send_photo(
                        chat_id=chat_id,
                        photo=f,
                        caption=greeting,
                        read_timeout=60,
                        write_timeout=60
                    )

            if audio_path:
                for attempt in range(3):
                    try:
                        with open(audio_path, "rb") as f:
                            await context.bot.send_voice(
                                chat_id=chat_id,
                                voice=f,
                                read_timeout=60,
                                write_timeout=60
                            )
                        break
                    except Exception as e:
                        logger.error(f"Morning audio attempt {attempt+1} failed for ChatID={chat_id}: {e}")
                        if attempt < 2:
                            await asyncio.sleep(2)

            asyncio.create_task(save_morning_log(
                chat_id,
                greeting,
                os.path.basename(photo_path) if photo_path else None,
                os.path.basename(audio_path) if audio_path else None
            ))
            logger.info(f"Morning job sent -> ChatID={chat_id}")

        except Exception as e:
            logger.error(f"Morning job failed for ChatID={chat_id}: {e}")


async def joke_job(context):
    if jokes:
        text = random.choice(jokes)
        await context.bot.send_message(chat_id=CHANNEL_ID, text=text)
        asyncio.create_task(save_channel_log(CHANNEL_ID, text))
        logger.info(f"Joke job sent to Channel={CHANNEL_ID}")


# ------------------- Инициализация при старте -------------------

async def on_startup(app):
    await upsert_chat(CHAT_ID, "афк 17 минут")
    payload = {
        "morning_enabled": True,
        "morning_enabled_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    try:
        session = get_session()
        async with session.patch(
            f"{SUPABASE_URL}/rest/v1/chats?chat_id=eq.{CHAT_ID}&morning_enabled=eq.false",
            headers=SUPABASE_HEADERS,
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            logger.info(f"Default chat morning init: {resp.status}")
    except Exception as e:
        logger.error(f"Ошибка on_startup: {e}")


# ------------------- Завершение работы -------------------

async def on_shutdown(app):
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
        logger.info("HTTP session закрыта")


# ------------------- Главная -------------------

if __name__ == "__main__":
    jokes = load_jokes()

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .post_init(on_startup)
        .post_stop(on_shutdown)
        .build()
    )

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, reply))
    app.add_handler(CommandHandler("roll", roll))
    app.add_handler(CommandHandler("joke", joke))
    app.add_handler(CommandHandler("yasno", yasno))
    app.add_handler(CommandHandler("chips", chips))
    app.add_handler(CommandHandler("spy", spy))
    app.add_handler(CommandHandler("morning_on", morning_on))
    app.add_handler(CommandHandler("morning_off", morning_off))

    jq = app.job_queue
    jq.run_daily(morning_job, time=datetime.time(hour=8, minute=0, tzinfo=MSK))
    for hour, minute in [(9, 0), (12, 0), (13, 41), (15, 2), (18, 0)]:
        jq.run_daily(joke_job, time=datetime.time(hour=hour, minute=minute, tzinfo=MSK))

    logger.info("Бот запущен | анекдотов: %d", len(jokes))
    app.run_polling()
