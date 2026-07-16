import os
from dotenv import load_dotenv
from agent import generate_and_pick
from feedback import init_feedback, save_quote_feedback

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
import traceback
import time
import tempfile
import subprocess

from quote_prompts import get_prompt
from morning_prompts import MORNING_ROLES

from llm import ask_model, init_supabase, push_to_cache, push_to_group_cache, _strip_thinking, _strip_markdown, OLLAMA_URL, OLLAMA_MODEL, start_http_server, SYSTEM_PROMPT, push_to_chat_feed

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
CHANNEL_ID = "example_text"
QUOTE_CHANNEL_ID = "example_text"
CHAT_ID = -example_text


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
init_feedback(SUPABASE_URL, SUPABASE_HEADERS)

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
        connector = aiohttp.TCPConnector(limit=50, use_dns_cache=True)
        _http_session = aiohttp.ClientSession(connector=connector, trust_env=True)
    return _http_session

_last_quote_data: dict | None = None

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


# ------------------- Скачивание и конвертация изображений -------------------

async def get_image_b64(context, file_id: str, is_webm: bool = False) -> str | None:
    try:
        tg_file = await context.bot.get_file(file_id)
        image_bytes = await tg_file.download_as_bytearray()

        if is_webm:
            with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as tmp_in:
                tmp_in.write(image_bytes)
                tmp_in_path = tmp_in.name
            tmp_out_path = tmp_in_path.replace(".webm", ".png")
            try:
                subprocess.run(
                    ["ffmpeg", "-y", "-i", tmp_in_path, "-frames:v", "1", tmp_out_path],
                    capture_output=True, check=True
                )
                with open(tmp_out_path, "rb") as f:
                    image_bytes = f.read()
            finally:
                os.unlink(tmp_in_path)
                if os.path.exists(tmp_out_path):
                    os.unlink(tmp_out_path)

        return base64.b64encode(image_bytes).decode()
    except Exception as e:
        logger.error(f"get_image_b64 error: {e}")
        return None


# ------------------- Эмодзи реакции с 20% шансом -------------------

MAX_PROCESSED = 10_000
processed_messages: set[tuple] = set()
processed_messages_list: list[tuple] = []

_reaction_semaphore = asyncio.Semaphore(5)


async def add_random_reaction(chat_id, message_id, message_text="", user_id=None, chat_type="private", sent_at=None):
    key = (chat_id, message_id)
    if key in processed_messages:
        return

    if random.random() >= 0.2:
        return

    # ДИАГНОСТИКА
    session = get_session()
    connector = session.connector
    if hasattr(connector, '_acquired'):
        logger.info(f"[DIAG] reaction: acquired={len(connector._acquired)}, limit={connector._limit}")

    async with _reaction_semaphore:
        try:
            emoji = random.choice(ALL_EMOJIS)
            api_url = f"https://api.telegram.org/bot{TOKEN}/setMessageReaction"
            async with session.post(
                api_url,
                json={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "reaction": [{"type": "emoji", "emoji": emoji}]
                },
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
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
            logger.error(f"Ошибка реакции: {type(e).__name__}: {e}\n{traceback.format_exc()}")


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


async def morning_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    role = random.choice(MORNING_ROLES)
    greeting = await ask_model(
        f"{role}. Образец роли доброго утра"
    )
    await update.message.reply_text(greeting)
    _push_command_context(update, "mrng", greeting)


# ------------------- Ответ на сообщения -------------------

async def reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    # Текст из сообщения или подписи к медиа
    text = (update.message.text or update.message.caption or "").strip()
    chat_type = update.message.chat.type
    user = update.message.from_user
    sent_at = update.message.date.isoformat() if update.message.date else None

    # Собираем image_b64 если есть медиа
    image_b64 = None
    file_id = None
    is_webm = False
    if update.message.photo:
        file_id = update.message.photo[-1].file_id
    elif update.message.sticker:
        if getattr(update.message.sticker, "is_video", False) or getattr(update.message.sticker, "mime_type", "") == "video/webm":
            file_id = update.message.sticker.file_id
            is_webm = True
        elif not update.message.sticker.is_animated:
            file_id = update.message.sticker.file_id
    elif update.message.animation:
        file_id = update.message.animation.file_id
        mime = getattr(update.message.animation, "mime_type", "")
        is_webm = mime == "video/webm" or getattr(update.message.animation, "file_name", "").lower().endswith(".webm")
    elif update.message.video:
        file_id = update.message.video.file_id
        mime = getattr(update.message.video, "mime_type", "")
        is_webm = mime == "video/webm" or getattr(update.message.video, "file_name", "").lower().endswith(".webm")
    elif update.message.document:
        mime = getattr(update.message.document, "mime_type", "")
        filename = getattr(update.message.document, "file_name", "").lower()
        is_webm = mime == "video/webm" or filename.endswith(".webm")
        if is_webm or mime.startswith("image/"):
            file_id = update.message.document.file_id

    if file_id:
        image_b64 = await get_image_b64(context, file_id, is_webm=is_webm)

    # Проверяем, есть ли реальный текст/подпись от пользователя
    has_text = bool(text)

    if not has_text and not image_b64:
        return

    # Для логирования и истории, если текста нет, дадим понятную заглушку
    log_text = text if has_text else "[Безмолвная реакция картинкой/стикером]"

    if chat_type != "private":
        push_to_chat_feed(
            update.message.chat.id,
            user.username or user.first_name or "аноним",
            text if has_text else "[Реакция картинкой/стикером]"
        )

    asyncio.create_task(add_random_reaction(
        update.message.chat.id,
        update.message.message_id,
        message_text=text if has_text else "[Реакция картинкой/стикером]",
        user_id=user.id,
        chat_type=chat_type,
        sent_at=sent_at
    ))

    if chat_type == "private":
        try:
            full_text = text if has_text else "Что думаешь об этой картинке/стикере?"
            if update.message.reply_to_message:
                replied = update.message.reply_to_message.text or update.message.reply_to_message.caption
                reply_author = update.message.reply_to_message.from_user
                media_note = " (пользователь прислал картинку/стикер)" if image_b64 else ""
                if replied:
                    if reply_author and reply_author.id == context.bot.id:
                        if has_text:
                            full_text = f"Ты сам написал это сообщение:\n{replied}\n\nПользователь отвечает на него{media_note}: {text}"
                        else:
                            full_text = f"Ты сам написал это сообщение:\n{replied}\n\nПользователь прислал тебе эту картинку/стикер в качестве безмолвной реакции на него. Отреагируй на неё в своём стиле."
                    else:
                        if has_text:
                            full_text = f"Вот сообщение:\n{replied}\n\nВопрос{media_note}: {text}"
                        else:
                            full_text = f"Вот сообщение:\n{replied}\n\nПользователь переслал его тебе вместе с картинкой/стикером в качестве реакции."
                elif reply_author and reply_author.id == context.bot.id:
                    if has_text:
                        full_text = f"Пользователь ответил на твоё предыдущее сообщение{media_note}: {text}"
                    else:
                        full_text = f"Пользователь прислал картинку/стикер в ответ на твоё предыдущее сообщение."

            await upsert_user(user.id, user.username)
            answer = await ask_model(
                full_text,
                user_id=user.id,
                chat_id=user.id,
                username=user.username or user.first_name or "Пользователь",
                is_group=False,
                image_b64=image_b64,
            )
            await update.message.reply_text(answer)
            asyncio.create_task(save_chat_private(
                user.id, log_text, answer, sent_at=sent_at
            ))
            logger.info(f"Private reply | UserID={user.id}", extra={"metadata": {"user_message": log_text, "bot_reply": answer}})
        except Exception as e:
            logger.error(f"Ошибка AI в личке: {e}")
        return

    # В группе — отвечаем при @упоминании ИЛИ при реплае на сообщение бота
    should_reply = False
    cleaned = text if has_text else ""
    search_query = text if has_text else None

    # 1. @упоминание бота
    entities = update.message.entities or update.message.caption_entities or []
    if entities:
        for entity in entities:
            if entity.type == "mention":
                src = update.message.text or update.message.caption or ""
                mention = src[entity.offset:entity.offset + entity.length]
                if mention.lower() == f"@{context.bot.username.lower()}":
                    cleaned = src.replace(mention, "").strip()
                    search_query = cleaned
                    post_text = None

                    if update.message.reply_to_message:
                        reply_msg = update.message.reply_to_message
                        post_text = reply_msg.text or reply_msg.caption
                        if post_text:
                            fwd_chat = getattr(reply_msg, "forward_from_chat", None)
                            sender_chat = getattr(reply_msg, "sender_chat", None)
                            if (fwd_chat and getattr(fwd_chat, "username", None) == "padique") or \
                                (sender_chat and getattr(sender_chat, "username", None) == "padique"):
                                post_text = f"Это твой собственный пост из канала @padique: {post_text}"
                            elif reply_msg.from_user:
                                author = reply_msg.from_user.username or reply_msg.from_user.first_name or "кто-то"
                                post_text = f"{author}: {post_text}"

                    if post_text:
                        if cleaned:
                            cleaned = f"Вот пост:\n{post_text}\n\nВопрос: {cleaned}"
                        else:
                            if image_b64:
                                cleaned = f"Вот пост:\n{post_text}\n\nПользователь прислал картинку/стикер в качестве реакции на этот пост. Оцени её."
                            else:
                                cleaned = f"Вот пост:\n{post_text}\n\nЧто думаешь об этом?"
                    else:
                        media_note = " (картинку/стикер)" if image_b64 else ""
                        if not cleaned:
                            cleaned = f"Пользователь прислал тебе{media_note}, ответь"

                    should_reply = True
                    break

    # 2. Реплай на сообщение бота
    if (not should_reply
            and update.message.reply_to_message
            and update.message.reply_to_message.from_user
            and update.message.reply_to_message.from_user.id == context.bot.id):
        should_reply = True
        replied_text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
        media_note = " (пользователь прислал картинку/стикер)" if image_b64 else ""
        if replied_text:
            if has_text:
                cleaned = f"Ты сам написал это сообщение:\n{replied_text}\n\nПользователь отвечает на него{media_note}: {text}"
            else:
                cleaned = f"Ты сам написал это сообщение:\n{replied_text}\n\nПользователь прислал тебе эту картинку/стикер в качестве безмолвной реакции (эмоции) на него. Оцени эту реакцию и ответь в своем характере."
        else:
            if has_text:
                cleaned = f"Пользователь ответил на твоё предыдущее сообщение{media_note}: {text}"
            else:
                cleaned = f"Пользователь прислал тебе эту картинку/стикер в качестве реакции на твоё предыдущее сообщение."

    if should_reply:
        chat = update.message.chat
        await upsert_chat(chat.id, chat.title or "")
        await upsert_user(user.id, user.username)
        try:
            answer = await ask_model(
                cleaned,
                search_query=search_query if search_query else None,
                user_id=user.id,
                chat_id=chat.id,
                username=user.username or user.first_name or "Пользователь",
                is_group=True,
                image_b64=image_b64,
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
        day_context = "сегодня выходной день"
    else:
        day_context = "сегодня рабочий день"

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
            role = random.choice(MORNING_ROLES)
            greeting = await ask_model(
                f"{role}. Образец роли доброго утра. {day_context}."
            )

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
            push_to_chat_feed(chat_id, "Шарап", greeting)
            logger.info(f"Morning job sent -> ChatID={chat_id}")

        except Exception as e:
            logger.error(f"Morning job failed for ChatID={chat_id}: {e}")


async def joke_job(context):
    if jokes:
        text = random.choice(jokes)
        await context.bot.send_message(chat_id=CHANNEL_ID, text=text)
        asyncio.create_task(save_channel_log(CHANNEL_ID, text))
        logger.info(f"Joke job sent to Channel={CHANNEL_ID}")


# ------------------- Цитаты -------------------

async def generate_quote() -> str | None:
    prompt, topic, perspective, mode, n_padique, n_history = get_prompt()

    start_time = time.time()

    try:
        session = get_session()
        async with session.post(
                OLLAMA_URL,
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "keep_alive": -1,
                    "options": {
                        "temperature": 1.3,
                        "repeat_penalty": 2,
                        "seed": random.randint(0, 999999),
                    }
                },
                timeout=aiohttp.ClientTimeout(total=120)
            ) as resp:

                duration = time.time() - start_time

                if resp.status != 200:
                    logger.error(
                        f"quote_generated_error | "
                        f"mode={mode} | topic={topic} | perspective={perspective} | "
                        f"status={resp.status} | duration={duration:.2f}s"
                    )
                    return None

                data = await resp.json()
                output = (data.get("response") or "").strip()
                if not output:
                    return None

                output = _strip_thinking(output)
                output = _strip_markdown(output)

                prompt_tokens = data.get("prompt_eval_count") or 0
                response_tokens = data.get("eval_count") or 0

                logger.info(
                    "quote_generated | "
                    f"topic={topic} | "
                    f"perspective={perspective} | "
                    f"duration={duration:.2f}s | "
                    f"mode={mode} | "
                    f"prompt_tokens={prompt_tokens} | "
                    f"response_tokens={response_tokens} | "
                    f"padique={n_padique} | history={n_history} | "
                    f"text={output[:300]}"
                )

                return output or None, topic, perspective, mode, duration, n_padique, n_history

    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            f"quote_exception | topic={topic} | perspective={perspective} | "
            f"duration={duration:.2f}s | error={e}"
        )
        return None


async def save_quote_log(text: str) -> None:
    payload = [{
        "channel_id": QUOTE_CHANNEL_ID,
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
                logger.error(f"Supabase quote_log error: {resp.status}")
    except Exception as e:
        logger.error(f"Ошибка save_quote_log: {e}")


async def quote_job(context) -> None:
    chosen, agent_result = await generate_and_pick(generate_quote, get_session(), n=5)
    if not chosen:
        logger.error("Quote generation failed, skipping")
        return

    quote, topic, perspective, mode, duration, n_padique, n_history = (
        chosen["text"], chosen["topic"], chosen["perspective"],
        chosen["mode"], chosen["duration"], chosen["n_padique"], chosen["n_history"]
    )

    try:
        await context.bot.send_message(chat_id=QUOTE_CHANNEL_ID, text=quote)
        logger.info(
            "quote_sent | "
            f"topic={topic} | "
            f"perspective={perspective} | "
            f"mode={mode} | "
            f"duration={duration:.2f}s | "
            f"padique={n_padique} | history={n_history} | "
            f"text={quote[:300]}"
        )
        await save_quote_log(quote)
    except Exception as e:
        logger.error(f"Quote send failed: {e}")


async def test_quote(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.message.from_user
    sent_at = update.message.date.isoformat() if update.message.date else None

    chosen, agent_result = await generate_and_pick(generate_quote, get_session(), n=5) 

    if not chosen:
        await update.message.reply_text("Не сгенерировал")
        logger.error(f"Command: quote failed | UserID={user.id} | ChatID={update.message.chat.id}")
        return

    quote = chosen["text"]

    await update.message.reply_text(quote)
    await upsert_user(user.id, user.username)
    await save_stat(
        user.id,
        update.message.chat.id,
        "quote",
        response=quote,
        sent_at=sent_at
    )

    _push_command_context(update, "quote", quote)
    logger.info(f"Command: quote | UserID={user.id} | ChatID={update.message.chat.id}")


#-------------------------------------Фидбэк-----------------------------------------------


async def quote_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _last_quote_data
    user = update.message.from_user

    if context.args and _last_quote_data:
        try:
            score = int(context.args[0])
            if score not in (1, 2, 3):
                await update.message.reply_text("Оценка: 1, 2 или 3")
                return
            comment = " ".join(context.args[1:]) if len(context.args) > 1 else None
            await save_quote_feedback(
                get_session(),
                text=_last_quote_data["text"],
                topic=_last_quote_data["topic"],
                perspective=_last_quote_data.get("perspective"),
                mode=_last_quote_data["mode"],
                score=score,
                comment=comment
            )
            await update.message.reply_text(f"Сохранил: {score}/3")
            _last_quote_data = None
        except ValueError:
            await update.message.reply_text("Оценка: 1, 2 или 3")
        return

    result = await generate_quote()
    if not result:
        await update.message.reply_text("Не сгенерировал")
        return

    quote, topic, perspective, mode, duration, n_padique, n_history = result
    if not quote:
        await update.message.reply_text("Не сгенерировал")
        return


    _last_quote_data = {
        "text": quote, "topic": topic,
        "perspective": perspective, "mode": mode,
    }

    await update.message.reply_text(
        f"{quote}\n\n"
        f"Тема: {topic}\n"
        f"Перспектива: {perspective or '—'}\n"
        f"Режим: {mode}\n\n"
        f"Оценка: /quote_data 1|2|3 [комментарий]"
    )
    logger.info(f"Command: quote_data | UserID={user.id}")


# ------------------- Инициализация при старте -------------------

async def on_startup(app):
    start_http_server(8001)
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

    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.Sticker.ALL | filters.ANIMATION | filters.VIDEO | filters.Document.ALL) & ~filters.COMMAND,
        reply
    ))
    app.add_handler(CommandHandler("roll", roll))
    app.add_handler(CommandHandler("joke", joke))
    app.add_handler(CommandHandler("yasno", yasno))
    app.add_handler(CommandHandler("chips", chips))
    app.add_handler(CommandHandler("spy", spy))
    app.add_handler(CommandHandler("morning_on", morning_on))
    app.add_handler(CommandHandler("morning_off", morning_off))
    app.add_handler(CommandHandler("quote", test_quote))
    app.add_handler(CommandHandler("mrng", morning_now))
    app.add_handler(CommandHandler("quote_data", quote_data))

    jq = app.job_queue
    jq.run_daily(morning_job, time=datetime.time(hour=8, minute=0, tzinfo=MSK))
    for hour, minute in [(9, 0), (13, 41), (18, 0)]:
        jq.run_daily(joke_job, time=datetime.time(hour=hour, minute=minute, tzinfo=MSK))
    for hour, minute in [(7, 30), (10, 0), (12, 30), (15, 0), (17, 30), (20, 00)]:
        jq.run_daily(quote_job, time=datetime.time(hour=hour, minute=minute, tzinfo=MSK))

    logger.info("Бот запущен | анекдотов: %d", len(jokes))
    app.run_polling()