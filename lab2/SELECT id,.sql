SELECT id,
       user_id,
       email,
       action,
       "timestamp"
FROM public.user_events_latest
LIMIT 1000;