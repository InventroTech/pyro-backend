from django.apps import apps

def generate_schema_summary(app_labels=None):
    """
    Generates a schema summary for all models (or a subset) in the project.
    Adds explicit field descriptions for ambiguous fields like 'cse_name' and 'assigned_to' ONLY in the support_ticket table.
    """
    schema_lines = []
    models = apps.get_models()
    if app_labels:
        models = [m for m in models if m._meta.app_label in app_labels]
    for model in models:
        fields = []
        for field in model._meta.get_fields():
            if field.auto_created and not field.concrete:
                continue  # skip reverse rels
            fname = field.name
            ftype = field.get_internal_type()
            fdesc = getattr(field, 'help_text', '') or ''
            fields.append(f"- {fname} ({ftype})" + (f": {fdesc}" if fdesc else ""))
        
        if model._meta.db_table == "support_ticket":
            fields.append("- cse_name (CharField): Name of the agent (customer support executive) who handled the ticket.")
            fields.append("- assigned_to (UUIDField): User ID of the assigned agent; links to auth.users.id")
        table_block = f"""### Table: {model._meta.db_table}
{model.__doc__ or model.__name__}
Fields:
""" + "\n".join(fields)
        schema_lines.append(table_block)
    return "\n\n".join(schema_lines)
