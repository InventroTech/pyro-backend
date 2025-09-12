from rest_framework import serializers

class LinkUserUidSerializer(serializers.Serializer):
    email = serializers.EmailField()
    uid = serializers.CharField(max_length=64)