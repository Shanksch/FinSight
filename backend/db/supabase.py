from supabase import create_client, Client
from core.config import settings

# Anon client — respects RLS (user-facing requests)
supabase: Client = create_client(
    settings.supabase_url,
    settings.supabase_publishable_key,
)

# Service role — bypasses RLS (admin/backend writes only)
supabase_admin: Client = create_client(
    settings.supabase_url,
    settings.supabase_secret_key,
)