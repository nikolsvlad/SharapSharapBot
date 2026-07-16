import random
from padique_examples import PADIQUE_EXAMPLES, HISTORY_EXAMPLES


# ---------------------------------------------------------------------------
# Константы промпта
# ---------------------------------------------------------------------------

QUOTE_PROMPT_TAIL = """\
[пример текста-инструкции для генерации поста]"""


# ---------------------------------------------------------------------------
# Данные
# ---------------------------------------------------------------------------

# Темы с весами: (тема, вес_original_write, вес_re_write)
TOPICS_WITH_WEIGHTS = [
    ("тема-пример 1", 1, 1),
    ("тема-пример 2", 1, 1),
]

# Перспективы с весами: (перспектива, вес) — только для original_write
PERSPECTIVES_WITH_WEIGHTS = [
    ("перспектива-пример 1", 1),
    ("перспектива-пример 2", 1),
]


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------

def get_prompt() -> tuple[str, str, str | None, str, int, int]:

    topics           = [t[0] for t in TOPICS_WITH_WEIGHTS]
    weights_original = [t[1] for t in TOPICS_WITH_WEIGHTS]
    weights_rewrite  = [t[2] for t in TOPICS_WITH_WEIGHTS]

    perspectives         = [p[0] for p in PERSPECTIVES_WITH_WEIGHTS]
    weights_perspectives = [p[1] for p in PERSPECTIVES_WITH_WEIGHTS]

    # ====== РЕЖИМ ГЕНЕРАЦИИ ======
    # original_write (80%): PADIQUE_EXAMPLES, мультикаст тем + перспективы
    # re_write (20%):       HISTORY_EXAMPLES, одна тема, без перспективы
    mode = random.choices(["original_write", "re_write"], weights=[80, 20], k=1)[0]

    if mode == "re_write":
        topic       = random.choices(topics, weights=weights_rewrite, k=1)[0]
        perspective = None
        n_padique   = 0
        n_history   = random.randint(20, 30)

        examples      = random.sample(HISTORY_EXAMPLES, min(n_history, len(HISTORY_EXAMPLES)))
        examples_text = "\n".join(f"- {e}" for e in examples)

        prompt = "\n".join([
            f"Задание: выбери цитату из примеров и адаптируй её на тему: {topic}.",
            "Сохраняй структуру и ритм оригинала. Только один пост.",
            "",
            "Примеры постов:",
            examples_text,
            "",
            QUOTE_PROMPT_TAIL,
        ])

    else:  # original_write
        n_history = 0
        n_padique = random.randint(20, 50)

        examples      = random.sample(PADIQUE_EXAMPLES, min(n_padique, len(PADIQUE_EXAMPLES)))
        examples_text = "\n".join(f"- {e}" for e in examples)

        # Темы: 50% одна, 30% две, 20% три
        n_topics        = random.choices([1, 2, 3], weights=[50, 30, 20], k=1)[0]
        selected_topics = list(dict.fromkeys(random.choices(topics, weights=weights_original, k=n_topics)))
        topic           = " + ".join(selected_topics)

        # Перспективы: 20% ноль, 50% одна, 20% две, 10% три
        n_perspectives        = random.choices([0, 1, 2, 3], weights=[20, 50, 20, 10], k=1)[0]
        selected_perspectives = list(dict.fromkeys(random.choices(perspectives, weights=weights_perspectives, k=n_perspectives)))
        perspective           = " + ".join(selected_perspectives) if selected_perspectives else None

        if len(selected_topics) == 1:
            topic_line = f"Задание: запости пост на тему: «{selected_topics[0]}»."
        else:
            topic_line = f"Задание: запости пост. Тема одна, но смотри на неё через призму всего сразу: {' и '.join(selected_topics)}."

        if not selected_perspectives:
            perspective_line = ""
        elif len(selected_perspectives) == 1:
            perspective_line = f"Раскрой тему через эту призму: {selected_perspectives[0]}."
        else:
            perspective_line = f"Раскрой тему через эту призму — всё сразу: {' и '.join(selected_perspectives)}."

        prompt = "\n".join([
            topic_line,
            perspective_line,
            "",
            "Примеры постов:",
            examples_text,
            "",
            QUOTE_PROMPT_TAIL,
        ]).replace("\n\n\n", "\n\n")

    return prompt, topic, perspective, mode, n_padique, n_history
