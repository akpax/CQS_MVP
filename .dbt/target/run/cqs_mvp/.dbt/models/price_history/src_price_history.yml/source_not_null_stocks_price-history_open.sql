select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select open
from `cqs-mvp`.`stocks`.`price-history`
where open is null



      
    ) dbt_internal_test