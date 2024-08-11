with source as (

    select * from {{ source('jaffle_shop', 'jaffle_shop_orders')}}

),



transformed as (
      select 
        id as order_id,
        user_id as customer_id,
        order_date as orderdate,
        status as order_status,

        case 
            when order_status not in ('returned','return_pending') 
            then orderdate 
        end as valid_order_date,

        row_number() over (
            partition by user_id 
            order by orderdate, id
          ) as user_order_seq,
        *
      from source
)

select * from transformed