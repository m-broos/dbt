with a as (
    
select

    "ORDER_ID" , 
    "CUSTOMER_ID", 
    "SURNAME", 
    "GIVENNAME", 
    "FIRST_ORDER_DATE", 
    "ORDER_COUNT", 
    "TOTAL_LIFETIME_VALUE", 
    "ORDER_VALUE_DOLLARS", 
    "ORDER_STATUS", 
    "PAYMENT_STATUS" 

from DBT_MB.dbt_mbroos.customer_orders

),

b as (
    
select
  
    "ORDER_ID" , 
    "CUSTOMER_ID", 
    "SURNAME", 
    "GIVENNAME", 
    "FIRST_ORDER_DATE", 
    "ORDER_COUNT", 
    "TOTAL_LIFETIME_VALUE", 
    "ORDER_VALUE_DOLLARS", 
    "ORDER_STATUS", 
    "PAYMENT_STATUS" 
     
from DBT_MB.dbt_mbroos.fct_customer_orders

),

a_intersect_b as (

    select * from a
    
    intersect

    select * from b

),

a_except_b as (

    select * from a

    except

    select * from b

),

b_except_a as (

    select * from b

    except

    select * from a

),

all_records as (

    select
        *,
        true as in_a,
        true as in_b
    from a_intersect_b

    union all

    select
        *,
        true as in_a,
        false as in_b
    from a_except_b

    union all

    select
        *,
        false as in_a,
        true as in_b
    from b_except_a

),

summary_stats as (

    select

        in_a,
        in_b,
        count(*) as count

    from all_records
    group by 1, 2

),

final as (

    select * from all_records
where not (in_a and in_b)
order by order_id, in_a desc, in_b desc
)

select * from final