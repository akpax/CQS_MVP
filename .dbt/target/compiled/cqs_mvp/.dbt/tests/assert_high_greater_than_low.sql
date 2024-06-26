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