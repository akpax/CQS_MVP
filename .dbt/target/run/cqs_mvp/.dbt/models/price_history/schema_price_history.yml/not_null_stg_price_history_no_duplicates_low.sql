select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select low
from `cqs-mvp`.`stocks`.`stg_price_history_no_duplicates`
where low is null



      
    ) dbt_internal_test