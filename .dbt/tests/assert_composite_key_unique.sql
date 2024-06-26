with price_history as (
    select
        symbol,
        timestamp
    from   
        {{ ref('stg_price_history_no_duplicates') }}
)


select *
from 
    price_history
group by    
    symbol, timestamp
having 
    count(*) > 1

