import asyncio
import logging
import re
import os
import time
import aiohttp
from collections import deque

from prometheus_client import Histogram, Counter, start_http_server

ollama_duration = Histogram('ollama_request_duration_seconds', 'Время ответа Ollama')
ollama_tokens_prompt = Counter('ollama_tokens_prompt_total', 'Токены промпта')
ollama_tokens_response = Counter('ollama_tokens_response_total', 'Токены ответа')
ollama_errors = Counter('ollama_errors_total', 'Ошибки Ollama', ['type'])

logger = logging.getLogger("bot")

# Глобальная HTTP сессия
_llm_session: aiohttp.ClientSession | None = None


def _get_session() -> aiohttp.ClientSession:
    global _llm_session
    if _llm_session is None or _llm_session.closed:
        connector = aiohttp.TCPConnector(limit=20, use_dns_cache=True)
        _llm_session = aiohttp.ClientSession(connector=connector)
    return _llm_session


async def close_session() -> None:
    global _llm_session
    if _llm_session and not _llm_session.closed:
        await _llm_session.close()


OLLAMA_MODEL = "gemma4:31b-cloud"
OLLAMA_URL = "http://localhost:11434/api/generate"

TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "")
TAVILY_URL = "https://api.tavily.com/search"

SYSTEM_PROMPT = """
Образец системного промпта
"""

# Триггеры для автопоиска
SEARCH_TRIGGERS = re.compile(
    r"\b(кто такой|расскажи про|кто это|найди|погугли|загугли|поищи|"
    r"последние новости|почём|сколько стоит|узнай про)\b",
    re.IGNORECASE
)

# ---------------------------------------------------------------------------
# Контекст диалога
# ---------------------------------------------------------------------------

MAX_HISTORY = 5

# Личный кэш: user_id → deque
_history: dict[int, deque] = {}
_loaded: set[int] = set()

# Групповой кэш: (chat_id, user_id) → deque
_group_history: dict[tuple, deque] = {}

# Лента сообщений чата: chat_id → deque (все сообщения группы)
CHAT_FEED_SIZE = 7
_chat_feed: dict[int, deque] = {}

_supabase_url: str = ""
_supabase_key: str = ""


def init_supabase(url: str, key: str) -> None:
    global _supabase_url, _supabase_key
    _supabase_url = url
    _supabase_key = key


# ---------------------------------------------------------------------------
# Личный кэш
# ---------------------------------------------------------------------------

def _get_cache(user_id: int) -> deque:
    if user_id not in _history:
        _history[user_id] = deque(maxlen=MAX_HISTORY * 2)
    return _history[user_id]


def _push_to_cache(user_id: int, user_message: str, bot_reply: str) -> None:
    cache = _get_cache(user_id)
    cache.append(("user", user_message))
    cache.append(("bot",  bot_reply))


async def _load_from_supabase(user_id: int) -> None:
    headers = {
        "apikey": _supabase_key,
        "Authorization": f"Bearer {_supabase_key}",
        "Content-Type": "application/json",
    }
    url = (
        f"{_supabase_url}/rest/v1/private_chat_history"
        f"?user_id=eq.{user_id}"
        f"&order=sent_at.desc"
        f"&limit={MAX_HISTORY}"
    )
    try:
        session = _get_session()
        async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    logger.warning(f"Context load failed: {resp.status} {await resp.text()}")
                    return
                rows = await resp.json()

        rows.reverse()
        cache = _get_cache(user_id)
        for row in rows:
            msg   = (row.get("message")  or "").strip()
            reply = (row.get("response") or "").strip()
            if msg:   cache.append(("user", msg))
            if reply: cache.append(("bot",  reply))
        logger.info(f"Context loaded from Supabase: user_id={user_id}, {len(rows)} pairs")
    except Exception as e:
        logger.error(f"Ошибка загрузки контекста из Supabase: {e}")


async def _ensure_context_loaded(user_id: int) -> None:
    if user_id not in _loaded:
        _loaded.add(user_id)
        await _load_from_supabase(user_id)


# ---------------------------------------------------------------------------
# Групповой кэш
# ---------------------------------------------------------------------------

def _get_group_cache(chat_id: int, user_id: int) -> deque:
    key = (chat_id, user_id)
    if key not in _group_history:
        _group_history[key] = deque(maxlen=MAX_HISTORY * 2)
    return _group_history[key]


def _push_to_group_cache(chat_id: int, user_id: int, user_message: str, bot_reply: str) -> None:
    cache = _get_group_cache(chat_id, user_id)
    cache.append(("user", user_message))
    cache.append(("bot",  bot_reply))


# ---------------------------------------------------------------------------
# Лента сообщений группового чата
# ---------------------------------------------------------------------------

def _get_chat_feed(chat_id: int) -> deque:
    if chat_id not in _chat_feed:
        _chat_feed[chat_id] = deque(maxlen=CHAT_FEED_SIZE)
    return _chat_feed[chat_id]


def _push_to_chat_feed(chat_id: int, username: str, text: str) -> None:
    _get_chat_feed(chat_id).append((username, text))


# ---------------------------------------------------------------------------
# Построение промптов
# ---------------------------------------------------------------------------

def _build_prompt(user_id: int, user_message: str, username: str = "Пользователь") -> str:
    cache = _get_cache(user_id)
    lines = [SYSTEM_PROMPT.strip(), ""]
    for role, text in cache:
        prefix = "Пользователь" if role == "user" else "Бот"
        lines.append(f"{prefix}: {text}")
    lines.append(f"{username}: {user_message}")
    lines.append("(Говори от первого лица. Не описывай свои действия.)")
    lines.append("Бот:")
    return "\n".join(lines)


def _build_group_prompt(chat_id: int, user_id: int, username: str, user_message: str) -> str:
    cache = _get_group_cache(chat_id, user_id)
    feed = _get_chat_feed(chat_id)

    lines = [SYSTEM_PROMPT.strip(), ""]

    if feed:
        lines.append("[Последние сообщения в чате]:")
        for uname, text in feed:
            lines.append(f"{uname}: {text}")
        lines.append("")

    if cache:
        lines.append("[Твоя переписка с этим пользователем]:")
        for role, text in cache:
            prefix = username if role == "user" else "Бот"
            lines.append(f"{prefix}: {text}")
        lines.append("")

    lines.append(f"{username}: {user_message}")
    lines.append("(Говори от первого лица. Не описывай свои действия.)")
    lines.append("Бот:")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Поиск через Tavily
# ---------------------------------------------------------------------------

async def _tavily_search(query: str) -> str | None:
    if not TAVILY_API_KEY:
        return None
    try:
        session = _get_session()
        async with session.post(TAVILY_URL, json={
                "api_key": TAVILY_API_KEY,
                "query": query,
                "max_results": 3,
                "search_depth": "basic",
            }) as resp:
                if resp.status != 200:
                    logger.warning(f"Tavily error: {resp.status}")
                    return None
                data = await resp.json()

        results = data.get("results", [])
        if not results:
            return None

        snippets = [f"- {r['title']}: {r['content'][:200]}" for r in results]
        return "\n".join(snippets)

    except Exception as e:
        logger.error(f"Tavily exception: {e}")
        return None


# ---------------------------------------------------------------------------
# Стрип thinking-блоков и markdown
# ---------------------------------------------------------------------------

def _strip_thinking(text: str) -> str:
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    if "...done thinking." in text:
        text = text.split("...done thinking.")[-1]
    lines = [
        l for l in text.splitlines()
        if not re.fullmatch(r"\s*Thinking\.+\s*", l, re.IGNORECASE)
    ]
    text = "\n".join(lines).strip()

    THINKING_MARKERS = (
        "пользователь явно",
        "пользователь спрашивает",
        "нужно признать",
        "стоит избегать",
        "можно обыграть",
        "хм,",
        "да, действительно",
        "итак,",
    )
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    clean = []
    for p in paragraphs:
        low = p.lower()
        if any(low.startswith(m) for m in THINKING_MARKERS):
            continue
        clean.append(p)

    return "\n\n".join(clean).strip() if clean else text


def _strip_markdown(text: str) -> str:
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__(.+?)__",     r"\1", text)
    text = re.sub(r"\*(.+?)\*",     r"\1", text)
    text = re.sub(r"_(.+?)_",       r"\1", text)
    text = re.sub(r"`(.+?)`",       r"\1", text)
    text = re.sub(r"^#{1,6}\s+",    "",    text, flags=re.MULTILINE)
    return text.strip()


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------

async def ask_model(
    user_message: str,
    search_query: str | None = None,
    user_id: int | None = None,
    chat_id: int | None = None,
    username: str = "Пользователь",
    is_group: bool = False,
    image_b64: str | None = None,
) -> str:

    search_context = ""
    if TAVILY_API_KEY and SEARCH_TRIGGERS.search(search_query or user_message):
        result = await _tavily_search(search_query or user_message)
        if result:
            search_context = f"\n\n[Данные из поиска]:\n{result}"
            logger.info(f"Tavily search for: {user_message[:60]}")

    full_message = user_message + search_context

    if is_group and chat_id is not None and user_id is not None:
        prompt = _build_group_prompt(chat_id, user_id, username, full_message)
    elif user_id is not None:
        await _ensure_context_loaded(user_id)
        prompt = _build_prompt(user_id, full_message, username)
    else:
        prompt = f"{SYSTEM_PROMPT}\n\nПользователь: {full_message}\nБот:"

    start = time.time()
    try:
        session = _get_session()
        payload = {
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "keep_alive": -1,
            "options": {
                "temperature": 1,
                "repeat_penalty": 1.3,
                "top_p": 0.95,
                "top_k": 64,
            }
        }
        if image_b64:
            payload["images"] = [image_b64]

        async with session.post(
            OLLAMA_URL,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=120)
        ) as resp:
            if resp.status != 200:
                ollama_errors.labels(type="http_error").inc()
                logger.error(f"Ollama HTTP error: {resp.status}")
                return "Ошибка модели"
            data = await resp.json()
            duration = time.time() - start
            prompt_tokens = data.get("prompt_eval_count") or 0
            response_tokens = data.get("eval_count") or 0
            ollama_duration.observe(duration)
            ollama_tokens_prompt.inc(prompt_tokens)
            ollama_tokens_response.inc(response_tokens)
            logger.info(
                f"ollama_request | duration={duration:.2f}s | "
                f"prompt_tokens={prompt_tokens} | "
                f"response_tokens={response_tokens} | "
                f"message={user_message[:50]}"
            )
            output = (data.get("response") or data.get("thinking") or "").strip()
            if not output:
                return "Ошибка модели"
            cleaned = _strip_thinking(output)
            cleaned = _strip_markdown(cleaned)
            response = cleaned or "Ошибка модели"
    except asyncio.TimeoutError:
        ollama_errors.labels(type="timeout").inc()
        logger.error("Ollama HTTP timed out")
        return "Ошибка ИИ (таймаут)"
    except Exception as e:
        logger.error(f"Ollama exception: {e}")
        return "Ошибка ИИ"

    if not response.startswith("Ошибка"):
        if is_group and chat_id is not None and user_id is not None:
            _push_to_group_cache(chat_id, user_id, user_message, response)
        elif user_id is not None:
            _push_to_cache(user_id, user_message, response)

    return response


# ---------------------------------------------------------------------------
# Публичные функции для записи контекста извне
# ---------------------------------------------------------------------------

def push_to_cache(user_id: int, user_message: str, bot_reply: str) -> None:
    _push_to_cache(user_id, user_message, bot_reply)


def push_to_group_cache(chat_id: int, user_id: int, user_message: str, bot_reply: str) -> None:
    _push_to_group_cache(chat_id, user_id, user_message, bot_reply)


def push_to_chat_feed(chat_id: int, username: str, text: str) -> None:
    _push_to_chat_feed(chat_id, username, text)