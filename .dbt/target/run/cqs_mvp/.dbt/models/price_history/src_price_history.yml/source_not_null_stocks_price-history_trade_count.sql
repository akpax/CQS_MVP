select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select trade_count
from `cqs-mvp`.`stocks`.`price-history`
where trade_count is null



      
    ) dbt_internal_test