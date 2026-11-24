import json

from channels.generic.websocket import AsyncWebsocketConsumer

from .broadcast import (
    is_realtime_oversight_role,
    tenant_group_name,
    tenant_oversight_group_name,
    user_group_name,
)


class NotificationConsumer(AsyncWebsocketConsumer):
    """Tenant-scoped realtime notifications for authenticated users."""

    async def connect(self):
        user = self.scope.get("user")
        tenant_id = self.scope.get("tenant_id")
        role_key = self.scope.get("role_key")

        if user is None or user.is_anonymous or not tenant_id:
            await self.close(code=4401)
            return

        self.tenant_id = tenant_id
        self.user = user
        self.role_key = role_key
        self.tenant_group = tenant_group_name(tenant_id)
        self.user_group = user_group_name(user.pk)
        self.oversight_group = None
        self._groups = [self.tenant_group, self.user_group]

        await self.channel_layer.group_add(self.tenant_group, self.channel_name)
        await self.channel_layer.group_add(self.user_group, self.channel_name)

        # Managers/admins join oversight so they receive all record_updated events.
        if is_realtime_oversight_role(role_key):
            self.oversight_group = tenant_oversight_group_name(tenant_id)
            self._groups.append(self.oversight_group)
            await self.channel_layer.group_add(self.oversight_group, self.channel_name)

        await self.accept()

        await self.send(
            text_data=json.dumps(
                {
                    "event": "connected",
                    "tenant_id": str(tenant_id),
                    "role_key": role_key,
                    "oversight": bool(self.oversight_group),
                }
            )
        )

    async def disconnect(self, close_code):
        for group in getattr(self, "_groups", []):
            await self.channel_layer.group_discard(group, self.channel_name)

    async def receive(self, text_data=None, bytes_data=None):
        if text_data == "ping":
            await self.send(text_data=json.dumps({"event": "pong"}))

    async def notify(self, event):
        await self.send(text_data=json.dumps(event["data"]))
