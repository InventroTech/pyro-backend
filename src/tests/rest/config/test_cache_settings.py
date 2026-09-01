from django.test import SimpleTestCase

from config.cache_settings import default_caches_config


class DefaultCachesConfigTests(SimpleTestCase):
    def test_uses_redis_when_redis_url_set(self):
        config = default_caches_config("redis://localhost:6379/0")

        self.assertEqual(
            config["default"]["BACKEND"],
            "django.core.cache.backends.redis.RedisCache",
        )
        self.assertEqual(config["default"]["LOCATION"], "redis://localhost:6379/0")

    def test_uses_locmem_when_redis_url_empty(self):
        config = default_caches_config("")

        self.assertEqual(
            config["default"]["BACKEND"],
            "django.core.cache.backends.locmem.LocMemCache",
        )
        self.assertNotIn("LOCATION", config["default"])
