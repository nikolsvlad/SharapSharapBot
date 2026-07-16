import asyncio
import json
import logging
import random
import re
import time

from llm import _strip_thinking, _strip_markdown, OLLAMA_URL, OLLAMA_MODEL
from feedback import load_feedback_examples

logger = logging.getLogger("bot")


# ---------------------------------------------------------------------------
# Построение системного промпта с динамическими примерами
# ---------------------------------------------------------------------------

def build_agent_system(good_examples: list[str], bad_examples: list[str]) -> str:
    good_block = "\n".join(f'- "{e}"' for e in good_examples) if good_examples else "- (нет примеров)"
    bad_block  = "\n".join(f'- "{e}"' for e in bad_examples)  if bad_examples  else "- (нет примеров)"
    return f"""пример задания
Оцени каждый пост по 10-балльной шкале.

Примеры ХОРОШИХ постов (8-10) — реальные оценки редактора паблика:
{good_block}

Примеры ПЛОХИХ постов (1-4) — реальные оценки редактора паблика:
{bad_block}

Критерии хорошего поста:
-пример 1

Критерии плохого поста:
-пример 2

Используй весь диапазон от 1 до 10. Не бойся ставить 1-3 откровенно слабым и 9-10 реально сильным.
Оценивай каждый пост независимо. Если все посты плохие — ставь всем низкие оценки. Не обязан кого-то выделять.

Отвечай ТОЛЬКО валидным JSON без лишнего текста:
{{"scores": [{{"n": 1, "score": <1-10>, "reason": "<одно предложение>"}}]}}"""


# ---------------------------------------------------------------------------
# Генерация N кандидатов параллельно
# ---------------------------------------------------------------------------

async def generate_candidates(generate_quote_fn, n: int = 5) -> list[dict]:
    tasks = [generate_quote_fn() for _ in range(n)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    candidates = []
    for r in results:
        if r is None or isinstance(r, Exception):
            continue
        if not r[0]:
            continue
        text, topic, perspective, mode, duration, n_padique, n_history = r
        candidates.append({
            "text":        text,
            "topic":       topic,
            "perspective": perspective,
            "mode":        mode,
            "duration":    duration,
            "n_padique":   n_padique,
            "n_history":   n_history,
        })

    return candidates


# ---------------------------------------------------------------------------
# Агент оценивает и выбирает лучшую
# ---------------------------------------------------------------------------

async def pick_best(candidates: list[dict], session, agent_system: str) -> tuple[dict | None, dict | None]:
    if not candidates:
        return None, None

    if len(candidates) == 1:
        return candidates[0], {"scores": [{"n": 1, "score": 10, "reason": "единственный кандидат"}]}

    numbered = "\n\n".join(
        f"{i+1}. {c['text']}" for i, c in enumerate(candidates)
    )
    prompt = f"пример промпта:\n\n{numbered}"

    start = time.time()
    try:
        async with session.post(
            OLLAMA_URL,
            json={
                "model":      OLLAMA_MODEL,
                "system":     agent_system,
                "prompt":     prompt,
                "stream":     False,
                "keep_alive": -1,
                "options": {
                    "temperature": 0.2,
                },
            },
        ) as resp:
            if resp.status != 200:
                logger.error(f"agent pick_best HTTP error: {resp.status}")
                return random.choice(candidates), None

            data     = await resp.json()
            duration = time.time() - start
            prompt_tokens   = data.get("prompt_eval_count") or 0
            response_tokens = data.get("eval_count") or 0
            raw      = (data.get("response") or "").strip()
            raw      = _strip_thinking(raw)
            raw      = _strip_markdown(raw)

            try:
                agent_result = json.loads(raw)
            except json.JSONDecodeError:
                m = re.search(r"\{.*\}", raw, re.DOTALL)
                if m:
                    agent_result = json.loads(m.group())
                else:
                    logger.error(f"agent pick_best: не смог распарсить JSON | raw={raw[:200]}")
                    return random.choice(candidates), None

            scores = agent_result.get("scores", [])
            if not scores:
                logger.error("agent pick_best: пустой scores")
                return random.choice(candidates), None

            # Выбираем с максимальной оценкой
            best = max(scores, key=lambda x: x.get("score", 0))
            chosen_idx = int(best.get("n", 1)) - 1
            if chosen_idx < 0 or chosen_idx >= len(candidates):
                chosen_idx = 0
            chosen = candidates[chosen_idx]

            # Логируем все оценки
            for s in sorted(scores, key=lambda x: x.get("score", 0), reverse=True):
                n   = int(s.get("n", 1)) - 1
                txt = candidates[n]["text"][:400] if 0 <= n < len(candidates) else "?"
                logger.info(
                    f"agent_score | "
                    f"n={s.get('n')} | "
                    f"score={s.get('score')} | "
                    f"text={txt} | "
                    f"reason={s.get('reason', '')}"
                )

            logger.info(
                f"agent_pick | "
                f"chosen={chosen_idx+1} | "
                f"score={best.get('score')} | "
                f"duration={duration:.2f}s | "
                f"prompt_tokens={prompt_tokens} | "
                f"response_tokens={response_tokens} | "
                f"text={chosen['text'][:60]}"
            )

            return chosen, agent_result

    except Exception as e:
        logger.error(f"agent pick_best exception: {e}")
        return random.choice(candidates), None


# ---------------------------------------------------------------------------
# Единая точка входа
# ---------------------------------------------------------------------------

async def generate_and_pick(generate_quote_fn, session, n: int = 5) -> tuple[dict | None, dict | None]:
    candidates = await generate_candidates(generate_quote_fn, n)

    if not candidates:
        logger.error("agent: все кандидаты упали")
        return None, None

    logger.info(f"agent_candidates | total={len(candidates)}")

    good_examples, bad_examples = await load_feedback_examples(session, n=5)
    agent_system = build_agent_system(good_examples, bad_examples)

    logger.info(f"agent_context | good={len(good_examples)} | bad={len(bad_examples)}")

    return await pick_best(candidates, session, agent_system)