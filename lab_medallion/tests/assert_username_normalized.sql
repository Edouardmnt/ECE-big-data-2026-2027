-- username_normalized must be lowercase and without surrounding whitespace
select user_id, username_normalized
from {{ ref('stg_users') }}
where username_normalized <> lower(trim(username_normalized))
