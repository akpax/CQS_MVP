with price_history as (
    select *
    from {{ ref('stg_price_history_no_duplicates') }}
)


select 
    symbol, 
    timestamp,
    close,
    open,
    high,
    low,
    avg(close) over (partition by symbol order by timestamp rows between 6 preceding and current row) as moving_average_7_day,
    avg(close) over (partition by symbol order by timestamp rows between 20 preceding and current row) as moving_average_21_day,
    avg(close) over (partition by symbol order by timestamp rows between 55 preceding and current row) as moving_average_56_day,
    stddev(close) over (partition by symbol order by timestamp rows between 6 preceding and current row) as volatility_7_day,
    stddev(close) over (partition by symbol order by timestamp rows between 20 preceding and current row) as volatility_21_day,
    stddev(close) over (partition by symbol order by timestamp rows between 55 preceding and current row) as volatility_56_day,

from
    price_history 
