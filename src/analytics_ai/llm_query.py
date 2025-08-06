import os
import openai
import traceback
import re


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

def get_sql_from_llm(prompt: str, model: str = "gpt-4.1", temperature: float = 0.1):
    """
    Calls the LLM to get a SQL query based on the given prompt.
    Returns (sql_query, raw_response).
    """
    if not OPENAI_API_KEY:
        print("[LLM ERROR] OPENAI_API_KEY not set")
        return None, None

    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are an expert backend SQL query generator."},
                {"role": "user", "content": prompt},
            ],
            temperature=temperature,
            max_tokens=512,
        )
        sql_query = response.choices[0].message.content.strip()
        print("[LLM DEBUG] Prompt sent:", prompt)
        print("[LLM DEBUG] Raw response:", response)
        print("[LLM DEBUG] SQL generated:", sql_query)
        return sql_query, response
    except Exception as e:
        print("[LLM EXCEPTION] Exception while calling LLM:", repr(e))
        print("[LLM TRACEBACK]")
        traceback.print_exc()
        return None, str(e)

import re

def clean_llm_sql_output(sql_text: str) -> str:
    """
    Strips code fences like ```sql ... ``` or ``` ... ```
    Returns only the raw SQL.
    """
    if not sql_text:
        return ""
    sql_text = sql_text.strip()
    # Remove triple-backtick blocks and leading 'sql' (case-insensitive)
    sql_text = re.sub(r"^```sql\s*", "", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r"^```\s*", "", sql_text)
    sql_text = re.sub(r"```$", "", sql_text)
    return sql_text.strip()
