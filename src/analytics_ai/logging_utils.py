import logging
analytics_logger = logging.getLogger("analytics_ai")
analytics_logger.setLevel(logging.INFO)
handler = logging.FileHandler("analytics_ai.log")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
analytics_logger.addHandler(handler)

def log_analytics_event(event_type, user_id, question, llm_prompt=None, sql_query=None, result=None, error=None):
    """
    Logs analytics events and errors for audit/debugging.
    """
    entry = {
        "event_type": event_type,
        "user_id": user_id,
        "question": question,
        "llm_prompt": llm_prompt,
        "sql_query": sql_query,
        "result": str(result)[:1000], 
        "error": str(error) if error else None
    }
    analytics_logger.info(entry)
