from drf_spectacular.extensions import OpenApiAuthenticationExtension

class SupabaseJWTAuthScheme(OpenApiAuthenticationExtension):
    target_class = 'config.supabase_auth.SupabaseJWTAuthentication' 
    name = 'BearerAuth' 

    def get_security_definition(self, auto_schema):
        return {
            'type': 'http',
            'scheme': 'bearer',
            'bearerFormat': 'JWT',
        }