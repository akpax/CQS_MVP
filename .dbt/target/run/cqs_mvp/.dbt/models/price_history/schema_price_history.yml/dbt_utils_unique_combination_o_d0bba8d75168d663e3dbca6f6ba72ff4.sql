select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        symbol, timestamp
    from `cqs-mvp`.`stocks`.`stg_price_history_no_duplicates`
    group by symbol, timestamp
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test