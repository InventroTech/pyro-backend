"""
Tests for core.soft_delete.SoftDeleteModel and TenantMembership integration.

Rolling soft-delete out to additional models requires per-model migrations;
TenantMembership is the reference implementation.
"""

import uuid

import pytest
from django.db import IntegrityError

from authz.models import TenantMembership
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
