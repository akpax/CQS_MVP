"""
This currently runs as a script but I propose eventually writing fucntions that
we import into the strategy files and run them there. 
I think we need to play around with backtrader and test a wider variety of 
strategies before we develop these functions
"""
import os
from datetime import date
from dotenv import load_dotenv
import backtrader as bt


from dags.util.core.alpaca.alpaca_actions import pull_bars
from analysis.strategies.buy_dips import BuyDips
from analysis.strategies.periodic_baseline import PeriodicBaseline

TICKER = ["VOO"]
START_CASH = 25000

# load development env variables
load_dotenv(".env.development")

header = (
    os.getenv("ALPACA_API_KEY_ID"),
    os.getenv("ALPACA_API_SECRET_KEY")
    )


# get spy data frame 
start_date = date(2008,1,1)
end_date = date(2024,7,5)
spy_df = pull_bars(start_date, end_date, header, tickers=TICKER)
print(spy_df.head)




# Separate cerebro instances for each strategy such that the strategies are not 
# being traded concurrently (sharing same cash) 

# First Strategy Run
cerebro1 = bt.Cerebro()
cerebro1.broker.set_cash(START_CASH)
data = bt.feeds.PandasData(
    dataname=spy_df, 
    datetime=1,
    open=-1, 
    close=-1,
    low=-1,
    volume=-1
)
cerebro1.adddata(data)
cerebro1.addstrategy(BuyDips, buy_size=1, dip_size=0.2)

strategies1 = cerebro1.run()
# Access strategy attributes for reporting
spy_dips = strategies1[0]
buy_dips_val = cerebro1.broker.getvalue()
buy_count = spy_dips.buy_count
print(f"SPY Dips Buy Count: {buy_count}")

# Plot the Buy Dips strategy
cerebro1.plot(style="bar")

# Second Strategy Run with the buy_count from the first strategy
cerebro2 = bt.Cerebro()
cerebro2.broker.set_cash(START_CASH)
data2 = bt.feeds.PandasData(dataname=spy_df)
cerebro2.adddata(data)
cerebro2.addstrategy(PeriodicBaseline, buy_count=buy_count)
cerebro2.run()
baseline_val = cerebro2.broker.getvalue()

# Plot baseline strategy
cerebro2.plot(style="bar")

print(f"Buy Dips percent return on ${TICKER[0]}: {round((buy_dips_val-START_CASH)/START_CASH*100,2)}%")
print(f"Baseline percent return on ${TICKER[0]}: {round((baseline_val-START_CASH)/START_CASH*100,2)}%")