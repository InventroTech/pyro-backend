from django.db import migrations
from django.conf import settings

def move_ct_and_perms(apps, schema_editor):
    ContentType = apps.get_model('contenttypes', 'ContentType')
    Permission  = apps.get_model('auth', 'Permission')

    # old -> new content type
    old_app, new_app, model = 'analytics', 'support_ticket', 'supportticket'
    old_ct = ContentType.objects.filter(app_label=old_app, model=model).first()
    if not old_ct:
        # Already moved or never existed; nothing to do.
        return

    new_ct, _ = ContentType.objects.get_or_create(app_label=new_app, model=model)

    # Build maps
    old_perms = {p.codename: p for p in Permission.objects.filter(content_type=old_ct)}
    new_perms = {p.codename: p for p in Permission.objects.filter(content_type=new_ct)}

    # Through tables for group/user permissions (works with custom user models)
    Group = apps.get_model('auth', 'Group')
    GroupPermThrough = Group.permissions.through

    user_app_label, user_model_name = settings.AUTH_USER_MODEL.split('.')
    User = apps.get_model(user_app_label, user_model_name)
    UserPermThrough = User.user_permissions.through

    # For any codename present in BOTH CTs:
    # Repoint group/user m2m rows from the "new" perm to the "old" one, then delete the new dup.
    for code, new_perm in list(new_perms.items()):
        if code in old_perms:
            old_perm = old_perms[code]
            # move group links
            GroupPermThrough.objects.filter(permission_id=new_perm.id).update(permission_id=old_perm.id)
            # move user links
            UserPermThrough.objects.filter(permission_id=new_perm.id).update(permission_id=old_perm.id)
            # remove duplicate new perm row
            new_perm.delete()
            del new_perms[code]

    # Now it's safe to move the old perms to the new CT (no unique conflicts)
    Permission.objects.filter(content_type=old_ct).update(content_type=new_ct)

    # Finally drop the old content type
    old_ct.delete()

def noop(apps, schema_editor):
    pass

class Migration(migrations.Migration):
    dependencies = [
        ('support_ticket', '0001_supportticket_state'),
        ('contenttypes', '0002_remove_content_type_name'),
        ('auth', '0012_alter_user_first_name_max_length'),
    ]
    operations = [
        migrations.RunPython(move_ct_and_perms, reverse_code=noop),
    ]
