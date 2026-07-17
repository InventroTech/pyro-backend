from unittest.mock import patch
import uuid

from django.core.cache import cache
from django.test import TestCase

from accounts.services.delete_user_everywhere import delete_user_everywhere
from authz.models import TenantMembership
from tests.factories import RoleFactory, SupabaseAuthUserFactory, TenantFactory, TenantMembershipFactory


class DeleteUserEverywhereForceLogoutTest(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()
        self.role = RoleFactory(tenant=self.tenant)
        self.uid = uuid.uuid4()
        self.email = "deleted-user@example.com"
        SupabaseAuthUserFactory(id=self.uid, email=self.email)
        self.membership = TenantMembershipFactory(
            tenant=self.tenant,
            role=self.role,
            email=self.email,
            user_id=self.uid,
            is_active=True,
        )
        cache.set(f"tenant:sub:{self.uid}", str(self.tenant.id), 60)

    @patch("accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally")
    def test_delete_revokes_supabase_sessions_and_clears_cache(self, mock_revoke):
        mock_revoke.return_value = {"user_id": str(self.uid), "revoked": True}

        report = delete_user_everywhere(
            tenant=self.tenant,
            email=self.email,
            role_id=str(self.role.id),
        )

        mock_revoke.assert_called_once_with(str(self.uid))
        self.assertEqual(report["deleted"]["tenant_memberships"], 1)
        self.assertEqual(report["deleted"]["auth_users"], 1)
        self.assertEqual(len(report["sessions_revoked"]), 1)
        self.assertTrue(report["sessions_revoked"][0]["revoked"])
        self.assertIsNone(cache.get(f"tenant:sub:{self.uid}"))
        self.assertFalse(
            TenantMembership.objects.filter(id=self.membership.id).exists()
        )
        archived = TenantMembership.all_objects.get(id=self.membership.id)
        self.assertTrue(archived.is_deleted)
        self.assertFalse(archived.is_active)
        self.assertIsNone(archived.user_id)

    @patch("accounts.services.delete_user_everywhere.revoke_supabase_sessions_globally")
    def test_delete_manager_reparents_reports_instead_of_deleting(self, mock_revoke):
        """Deleting a manager keeps their reports and promotes them to the grandparent."""
        mock_revoke.return_value = {"user_id": str(self.uid), "revoked": True}

        # self.membership is the GM (top-level). Add an ASM under it, and an RM under the ASM.
        asm = TenantMembershipFactory(
            tenant=self.tenant,
            role=self.role,
            email="asm@example.com",
            user_id=uuid.uuid4(),
            is_active=True,
            user_parent_id=self.membership,
        )
        rm = TenantMembershipFactory(
            tenant=self.tenant,
            role=self.role,
            email="rm@example.com",
            user_id=uuid.uuid4(),
            is_active=True,
            user_parent_id=asm,
        )

        report = delete_user_everywhere(
            tenant=self.tenant,
            email=self.email,
            role_id=str(self.role.id),
        )

        # The GM's direct report (ASM) is re-parented, not deleted.
        self.assertEqual(report["reports_reassigned"], 1)
        asm.refresh_from_db()
        self.assertFalse(asm.is_deleted)
        # GM had no parent, so the ASM becomes top-level.
        self.assertIsNone(asm.user_parent_id_id)
        # The RM further down is untouched and still reports to the ASM.
        rm.refresh_from_db()
        self.assertFalse(rm.is_deleted)
        self.assertEqual(rm.user_parent_id_id, asm.id)
