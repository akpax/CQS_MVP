-- remove duplicate rows based on same symbol, timestamp
with cte as (
    SELECT *, 
        row_number() OVER(PARTITION BY symbol, timestamp) AS row_number
    FROM {{ source('stocks', 'price-history') }}
    ORDER BY symbol, timestamp
) 


select 
    symbol,
    timestamp,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    vwap
from 
    cte
where 
    row_number = 1


