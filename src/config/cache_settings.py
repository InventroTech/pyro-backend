"""Default Django CACHES config from REDIS_URL (testable without reloading settings)."""


def default_caches_config(redis_url: str) -> dict:
    if redis_url:
        return {
            "default": {
                "BACKEND": "django.core.cache.backends.redis.RedisCache",
                "LOCATION": redis_url,
            }
        }
    return {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        }
    }
