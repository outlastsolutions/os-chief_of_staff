"""
OSAIO LLM abstraction
Outlast Solutions LLC © 2026

Unified chat interface supporting Claude (Anthropic) and Gemini (Google).
Model is selected per agent via config/settings.py or passed directly.
Provider is inferred from the model ID prefix.
"""

from __future__ import annotations
import json
from typing import Optional
from config.settings import ANTHROPIC_API_KEY, GEMINI_API_KEY


def _is_claude(model: str) -> bool:
    return model.startswith("claude")


def chat(model: str, system: str, messages: list[dict],
         max_tokens: int = 4096, temperature: float = 0.2) -> str:
    """
    Send a chat request to Claude or Gemini.

    Args:
        model:       Model ID — e.g. 'claude-sonnet-4-6' or 'gemini-2.5-flash'
        system:      System prompt string
        messages:    List of {"role": "user"|"assistant", "content": str}
        max_tokens:  Max output tokens
        temperature: Sampling temperature (lower = more deterministic)

    Returns:
        Response text as a string.
    """
    if _is_claude(model):
        return _claude(model, system, messages, max_tokens, temperature)
    else:
        return _gemini(model, system, messages, max_tokens, temperature)


def chat_json(model: str, system: str, messages: list[dict],
              max_tokens: int = 4096) -> dict:
    """
    Like chat(), but parses the response as JSON.
    Appends an instruction to return valid JSON if not already in the system prompt.
    Raises ValueError if the response cannot be parsed.
    """
    if "json" not in system.lower():
        system += "\n\nRespond with valid JSON only. No markdown, no code fences, no commentary."

    raw = chat(model, system, messages, max_tokens, temperature=0.1)

    # Strip markdown code fences if the model added them anyway
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
        raw = raw.rsplit("```", 1)[0]

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Attempt automatic repair for common LLM JSON issues (missing brackets, trailing commas, etc.)
        from json_repair import repair_json
        repaired = repair_json(raw)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError as e:
            raise ValueError(f"LLM returned invalid JSON (repair failed): {e}\n\nRaw response:\n{raw}")


# ── Claude ────────────────────────────────────────────────────────────────

def _claude(model: str, system: str, messages: list[dict],
            max_tokens: int, temperature: float) -> str:
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    response = client.messages.create(
        model=model,
        system=system,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return response.content[0].text


# ── Gemini ────────────────────────────────────────────────────────────────

def _gemini(model: str, system: str, messages: list[dict],
            max_tokens: int, temperature: float) -> str:
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=GEMINI_API_KEY)

    # Convert messages to Gemini Content format
    contents = [
        types.Content(
            role="user" if m["role"] == "user" else "model",
            parts=[types.Part(text=m["content"])]
        )
        for m in messages
    ]

    response = client.models.generate_content(
        model=model,
        contents=contents,
        config=types.GenerateContentConfig(
            system_instruction=system,
            max_output_tokens=max_tokens,
            temperature=temperature,
        ),
    )
    return response.text
