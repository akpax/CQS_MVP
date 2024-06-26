select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select symbol
from `cqs-mvp`.`stocks`.`price-history`
where symbol is null



      
    ) dbt_internal_test