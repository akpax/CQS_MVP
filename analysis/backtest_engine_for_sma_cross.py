import os
from datetime import date
from dotenv import load_dotenv
import backtrader as bt


from dags.util.core.alpaca.alpaca_actions import pull_bars
from analysis.strategies.sma_cross import SMACross

TICKER = ["AAPL"]
START_CASH = 1000
# load development env variables
load_dotenv(".env.development")

header = (
    os.getenv("ALPACA_API_KEY_ID"),
    os.getenv("ALPACA_API_SECRET_KEY")
    )

# pull data frame 
start_date = date(2008,1,1)
end_date = date(2024,7,5)
df = pull_bars(start_date, end_date, header, tickers=TICKER)

cerebro = bt.Cerebro()
cerebro.broker.set_cash(START_CASH)
data = bt.feeds.PandasData(
    dataname=df, 
    datetime=1,
    open=-1, 
    close=-1,
    low=-1,
    volume=-1
)

cerebro.adddata(data)

cerebro.addstrategy(SMACross, fast_period=50, slow_period=200)

cerebro.run()


# Plot the Buy Dips strategy
cerebro.plot(style="bar")