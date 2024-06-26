select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      with price_history as (
    select
        symbol,
        timestamp
    from   
        `cqs-mvp`.`stocks`.`stg_price_history_no_duplicates`
)


select *
from 
    price_history
group by    
    symbol, timestamp
having 
    count(*) > 1
      
    ) dbt_internal_test