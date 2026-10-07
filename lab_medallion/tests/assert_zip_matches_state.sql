{{ config(severity='warn') }}

-- The 3 first digits of the ZIP code must belong to a range of the state
select u.user_id, u.state, u.zip_code
from {{ ref('stg_users') }} u
where u.zip_code is not null
  and not exists (
      select 1
      from {{ ref('zip_prefixes') }} z
      where z.state = u.state
        and cast(left(u.zip_code, 3) as integer) between z.prefix_min and z.prefix_max
  )
