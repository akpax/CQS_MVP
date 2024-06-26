select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      with price_history as (
    select
        high,
        low
    from   
        `cqs-mvp`.`stocks`.`price-history`
)


select *
from 
    price_history
where 
    high < low
      
    ) dbt_internal_test