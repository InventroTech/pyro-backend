"""
Tests for core.soft_delete.SoftDeleteMixin / core.models.BaseModel and TenantMembership.

Rolling soft-delete out to additional models requires per-model migrations;
TenantMembership is the reference implementation.
"""

import uuid

import pytest
from django.db import IntegrityError

from authz.models import (
    GroupMembership,
    GroupPermission,
    GroupRole,
    Permission,
    TenantMembership,
    UserGroup,
    UserPermission,
)
from core.soft_delete import alive_q, not_deleted_q


@pytest.mark.django_db
class TestAliveQ:
    def test_alive_q_matches_not_deleted_alias(self):
        assert str(alive_q()) == str(not_deleted_q())


@pytest.mark.django_db
class TestTenantMembershipSoftDelete:
    def test_default_manager_excludes_soft_deleted(self, tenant):
        from tests.factories import RoleFactory, TenantMembershipFactory

        role = RoleFactory(tenant=tenant, key="AGENT", name="Agent")
        uid = str(uuid.uuid4())
        email = "soft-del-test@example.com"
        m = TenantMembershipFactory(
            tenant=tenant,
            user_id=uid,
            email=email,
            role=role,
            is_active=True,
        )
        m.delete()

        assert not TenantMembership.objects.filter(id=m.id).exists()
        assert TenantMembership.all_objects.filter(id=m.id).exists()
        row = TenantMembership.all_objects.get(id=m.id)
        assert row.is_deleted is True
        assert row.deleted_at is not None

    def test_queryset_delete_soft_deletes(self, tenant):
        from tests.factories import RoleFactory, TenantMembershipFactory

        role = RoleFactory(tenant=tenant, key="AGENT2", name="Agent2")
        m = TenantMembershipFactory(
            tenant=tenant,
            user_id=str(uuid.uuid4()),
            email="bulk-del@example.com",
            role=role,
            is_active=True,
        )
        TenantMembership.objects.filter(id=m.id).delete()
        assert not TenantMembership.objects.filter(id=m.id).exists()
        assert TenantMembership.all_objects.filter(id=m.id, is_deleted=True).exists()

    def test_hard_delete_removes_row(self, tenant):
        from tests.factories import RoleFactory, TenantMembershipFactory

        role = RoleFactory(tenant=tenant, key="AGENT3", name="Agent3")
        m = TenantMembershipFactory(
            tenant=tenant,
            user_id=str(uuid.uuid4()),
            email="hard-del@example.com",
            role=role,
            is_active=True,
        )
        m.hard_delete()
        assert not TenantMembership.all_objects.filter(id=m.id).exists()

    def test_restore_clears_flags(self, tenant):
        from tests.factories import RoleFactory, TenantMembershipFactory

        role = RoleFactory(tenant=tenant, key="AGENT4", name="Agent4")
        m = TenantMembershipFactory(
            tenant=tenant,
            user_id=str(uuid.uuid4()),
            email="restore@example.com",
            role=role,
            is_active=True,
        )
        m.delete()
        m.refresh_from_db()
        m.restore()
        assert TenantMembership.objects.filter(id=m.id).exists()
        m.refresh_from_db()
        assert m.is_deleted is False
        assert m.deleted_at is None

    def test_unique_allows_recreate_after_soft_delete(self, tenant):
        """Same (tenant, role, email) can exist again after soft-delete."""
        from tests.factories import RoleFactory, TenantMembershipFactory

        role = RoleFactory(tenant=tenant, key="UNIQ", name="Uniq")
        email = "unique-recreate@example.com"
        m1 = TenantMembershipFactory(
            tenant=tenant,
            user_id=str(uuid.uuid4()),
            email=email,
            role=role,
            is_active=True,
        )
        m1.delete()
        m2 = TenantMembership.objects.create(
            tenant=tenant,
            role=role,
            email=email,
            user_id=str(uuid.uuid4()),
            is_active=True,
        )
        assert m2.id != m1.id
        assert TenantMembership.objects.filter(email=email).count() == 1

    def test_unique_still_enforced_for_two_alive_rows(self, tenant):
        from tests.factories import RoleFactory, TenantMembershipFactory

        role = RoleFactory(tenant=tenant, key="UNIQ2", name="Uniq2")
        email = "dup-alive@example.com"
        TenantMembershipFactory(
            tenant=tenant,
            user_id=str(uuid.uuid4()),
            email=email,
            role=role,
            is_active=True,
        )
        with pytest.raises(IntegrityError):
            TenantMembership.objects.create(
                tenant=tenant,
                role=role,
                email=email,
                user_id=str(uuid.uuid4()),
                is_active=True,
            )


@pytest.mark.django_db
class TestSoftDeleteCascade:
    def test_membership_cascades_user_permissions(self, tenant):
        from tests.factories import RoleFactory, TenantMembershipFactory

        role = RoleFactory(tenant=tenant, key="CASC_UP", name="CascUp")
        m = TenantMembershipFactory(
            tenant=tenant,
            user_id=str(uuid.uuid4()),
            email="cascade-up@example.com",
            role=role,
            is_active=True,
        )
        perm = Permission.objects.create(perm_key="perm.cascade.userperm")
        up = UserPermission.objects.create(membership=m, permission=perm)
        m.delete()
        assert not UserPermission.objects.filter(id=up.id).exists()
        assert UserPermission.all_objects.get(id=up.id).is_deleted

    def test_membership_cascades_direct_reports(self, tenant):
        from tests.factories import RoleFactory, TenantMembershipFactory

        role = RoleFactory(tenant=tenant, key="CASC_DR", name="CascDr")
        manager = TenantMembershipFactory(
            tenant=tenant,
            user_id=str(uuid.uuid4()),
            email="mgr-cascade@example.com",
            role=role,
            is_active=True,
        )
        report = TenantMembershipFactory(
            tenant=tenant,
            user_id=str(uuid.uuid4()),
            email="rpt-cascade@example.com",
            role=role,
            is_active=True,
            user_parent_id=manager,
        )
        manager.delete()
        assert not TenantMembership.objects.filter(id=report.id).exists()
        assert TenantMembership.all_objects.get(id=report.id).is_deleted

    def test_queryset_delete_cascades_user_permissions(self, tenant):
        from tests.factories import RoleFactory, TenantMembershipFactory

        role = RoleFactory(tenant=tenant, key="CASC_QS", name="CascQs")
        m = TenantMembershipFactory(
            tenant=tenant,
            user_id=str(uuid.uuid4()),
            email="cascade-qs@example.com",
            role=role,
            is_active=True,
        )
        perm = Permission.objects.create(perm_key="perm.cascade.qs")
        up = UserPermission.objects.create(membership=m, permission=perm)
        TenantMembership.objects.filter(id=m.id).delete()
        assert UserPermission.all_objects.get(id=up.id).is_deleted

    def test_hard_delete_cascades_user_permissions(self, tenant):
        from tests.factories import RoleFactory, TenantMembershipFactory

        role = RoleFactory(tenant=tenant, key="CASC_HD", name="CascHd")
        m = TenantMembershipFactory(
            tenant=tenant,
            user_id=str(uuid.uuid4()),
            email="cascade-hd@example.com",
            role=role,
            is_active=True,
        )
        perm = Permission.objects.create(perm_key="perm.cascade.hd")
        up = UserPermission.objects.create(membership=m, permission=perm)
        m.hard_delete()
        assert not UserPermission.all_objects.filter(id=up.id).exists()

    def test_user_group_cascades_children(self, tenant):
        from tests.factories import RoleFactory

        role = RoleFactory(tenant=tenant, key="UGR", name="UgRole")
        perm = Permission.objects.create(perm_key="perm.cascade.ug")
        g = UserGroup.objects.create(tenant=tenant, key="ug1", name="UG1")
        gm = GroupMembership.objects.create(group=g, user_id=uuid.uuid4())
        gp = GroupPermission.objects.create(group=g, permission=perm)
        gr = GroupRole.objects.create(group=g, role=role)
        g.delete()
        assert not GroupMembership.objects.filter(id=gm.id).exists()
        assert not GroupPermission.objects.filter(id=gp.id).exists()
        assert not GroupRole.objects.filter(id=gr.id).exists()
        assert GroupMembership.all_objects.get(id=gm.id).is_deleted
        assert GroupPermission.all_objects.get(id=gp.id).is_deleted
        assert GroupRole.all_objects.get(id=gr.id).is_deleted
