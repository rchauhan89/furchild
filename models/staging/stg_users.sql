select
  id as user_id,
  customer_account_id as customer_id
from {{ source('bronze','users') }}
