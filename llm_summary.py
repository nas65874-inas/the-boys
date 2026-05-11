import os

import requests


def fallback_summary(keywords):
    if not keywords:
        return "No strong news theme has appeared yet because the stream has not collected enough headlines."

    top_words = ", ".join(keywords[:8])

    return (
        "The current news pulse is led by these keywords: "
        f"{top_words}. The main storylines seem to be developing around the most repeated terms."
    )


def make_summary(keywords):
    if not keywords:
        return fallback_summary(keywords)

    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        return fallback_summary(keywords)

    prompt = (
        "Write one paragraph, no more than 80 words, summarising the news themes "
        "from these keywords. Mention at least three named storylines if possible. "
        f"Keywords: {', '.join(keywords[:15])}"
    )

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "max_tokens": 120,
            },
            timeout=15,
        )

        response.raise_for_status()
        data = response.json()

        return data["choices"][0]["message"]["content"].strip()

    except Exception:
        return fallback_summary(keywords)
