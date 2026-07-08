import json

from channels.generic.websocket import AsyncWebsocketConsumer

from .broadcast import tenant_group_name, user_group_name


class NotificationConsumer(AsyncWebsocketConsumer):
    """Tenant-scoped realtime notifications for authenticated users."""

    async def connect(self):
        user = self.scope.get("user")
        tenant_id = self.scope.get("tenant_id")

        if user is None or user.is_anonymous or not tenant_id:
            await self.close(code=4401)
            return

        self.tenant_id = tenant_id
        self.user = user
        self.tenant_group = tenant_group_name(tenant_id)
        self.user_group = user_group_name(user.pk)

        await self.channel_layer.group_add(self.tenant_group, self.channel_name)
        await self.channel_layer.group_add(self.user_group, self.channel_name)
        await self.accept()

        await self.send(
            text_data=json.dumps(
                {
                    "event": "connected",
                    "tenant_id": str(tenant_id),
                }
            )
        )

    async def disconnect(self, close_code):
        if hasattr(self, "tenant_group"):
            await self.channel_layer.group_discard(self.tenant_group, self.channel_name)
        if hasattr(self, "user_group"):
            await self.channel_layer.group_discard(self.user_group, self.channel_name)

    async def receive(self, text_data=None, bytes_data=None):
        if text_data == "ping":
            await self.send(text_data=json.dumps({"event": "pong"}))

    async def notify(self, event):
        await self.send(text_data=json.dumps(event["data"]))
