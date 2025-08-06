def build_llm_prompt(user_question: str, schema_str: str, instructions: str = None, examples: str = None):
    """
    Construct the full prompt for the LLM using the question, schema, and optional instructions/examples.
    """
    prompt_sections = []

    # Optional system instructions (helps set LLM context/behavior)
    if instructions:
        prompt_sections.append(instructions)
    else:
        prompt_sections.append(
            "You are a backend analytics agent. Generate an efficient SQL query using ONLY the tables and fields described below to answer the user's question. Do NOT make up any tables or columns.In this schema, 'agent' means either the 'cse_name' field in support_ticket (the support executive's name), "
    "or you can join support_ticket.assigned_to to auth.users.id to get agent details (e.g., name, email)."
        )

    # Add schema
    prompt_sections.append("Database schema:\n" + schema_str)

    # Add user question
    prompt_sections.append(f"User question: \"{user_question}\"")

    # Optionally add example Q&A pairs
    if examples:
        prompt_sections.append("Example queries:\n" + examples)

    # Final prompt: join sections with spacing
    prompt = "\n\n".join(prompt_sections)
    return prompt
