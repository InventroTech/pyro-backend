# support_ticket/migrations/0005_managed_true.py
from django.db import migrations
class Migration(migrations.Migration):
    dependencies = [('support_ticket', '0004_alter_supportticket_options')]
    operations = [migrations.AlterModelOptions(name='supportticket', options={'managed': True})]
