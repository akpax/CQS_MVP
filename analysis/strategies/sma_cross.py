import backtrader as bt
import backtrader.indicators as btind


class SMACross(bt.Strategy):
    def log(self, txt, dt=None):
        """
        Logs a message with a timestamp.

        Args:
            txt (str): The message to log.
            dt (datetime, optional): The timestamp to include in the log.
        """
        dt = dt or self.data.datetime.date(0)
        print(dt.isoformat(), txt)

    def __init__(self, fast_period, slow_period):
        """


        Args:
            buy_count (int): The number of buy orders to execute.
        
        Note: Args are passed to Cerebro instance when using the "addstrategy" method.
        """
        self.dataclose = self.datas[0].close
        self.sma_fast = btind.MovingAverageSimple(self.dataclose, period=fast_period)
        self.sma_slow = btind.MovingAverageSimple(self.dataclose, period=slow_period)
        self.cross_over = btind.CrossOver(self.sma_fast, self.sma_slow)

    def next(self):
            """
            Defines the logic to be executed on each bar of the data feed.
            """
            # buy when short avg crosses above long and no open positions
            # Buy when short avg crosses above long and no open positions
            if self.cross_over > 0 and not self.position:
                self.log(f'BUY CREATE, {self.dataclose[0]}')
                self.buy(size=1)

            # Sell when short avg crosses below long and currently holding a position
            if self.cross_over < 0 and self.position:
                self.log(f'SELL CREATE, {self.dataclose[0]}')
                self.sell(size=1)
            