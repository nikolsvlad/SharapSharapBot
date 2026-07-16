import datetime
import json
import logging
import random

logger = logging.getLogger("bot")

_supabase_url: str = ""
_supabase_headers: dict = {}


def init_feedback(supabase_url: str, supabase_headers: dict) -> None:
    global _supabase_url, _supabase_headers
    _supabase_url = supabase_url
    _supabase_headers = supabase_headers


async def save_quote_feedback(
    session,
    text: str,
    topic: str,
    perspective: str | None,
    mode: str,
    score: int,
    comment: str | None = None,
) -> None:
    payload = [{
        "text":        text,
        "topic":       topic,
        "perspective": perspective,
        "mode":        mode,
        "score":       score,
        "comment":     comment,
        "created_at":  datetime.datetime.now(datetime.timezone.utc).isoformat()
    }]
    try:
        async with session.post(
            f"{_supabase_url}/rest/v1/quote_feedback",
            headers=_supabase_headers,
            data=json.dumps(payload, ensure_ascii=False)
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.error(f"Supabase quote_feedback error: {resp.status}")
            else:
                logger.info(f"quote_feedback saved | score={score} | text={text[:60]}")
    except Exception as e:
        logger.error(f"Ошибка save_quote_feedback: {e}")


async def load_feedback_examples(session, n: int = 5) -> tuple[list[str], list[str]]:
    good, bad = [], []
    try:
        async with session.get(
            f"{_supabase_url}/rest/v1/quote_feedback"
            f"?score=eq.3&select=text&order=created_at.desc&limit=100",
            headers=_supabase_headers
        ) as resp:
            if resp.status == 200:
                rows = await resp.json()
                good = [r["text"] for r in rows]

        async with session.get(
            f"{_supabase_url}/rest/v1/quote_feedback"
            f"?score=eq.1&select=text&order=created_at.desc&limit=100",
            headers=_supabase_headers
        ) as resp:
            if resp.status == 200:
                rows = await resp.json()
                bad = [r["text"] for r in rows]

    except Exception as e:
        logger.error(f"Ошибка load_feedback_examples: {e}")

    return good, bad