"""
PERIODIC BASELINE STRATEGY

This module implements a trading strategy that buys 'buy_count' number of assets
at evenly spaced intervals throughout the entire datafeed history.

It is intended to mimick a buy and hold strategy where the trader averages 
into a stock at at evenly spaced periods. The 'buy_count' parameter is typically
passed in from another strategy.  Note that this strategy will have different 
cash value.
"""
import backtrader as bt

class PeriodicBaseline(bt.Strategy):
    """
    A trading strategy that buys assets at evenly spaced intervals.

    Attributes:
        buy_count (int): The number of buy orders to execute.
        dataclose (LineSeries): The close prices of the data feed.
        data_length (int): The length of the data feed.
        buy_interval (int): The interval between buy orders.
        buy_counter (int): The number of buy orders executed.
    """
    def log(self, txt, dt=None):
        """
        Logs a message with a timestamp.

        Args:
            txt (str): The message to log.
            dt (datetime, optional): The timestamp to include in the log.
        """
        dt = dt or self.data.datetime.date(0)
        print(dt.isoformat(), txt)
    
    def __init__(self, buy_count):
        """
        Initializes the Baseline strategy.

        Args:
            buy_count (int): The number of buy orders to execute.
        
        Note: Args are passed to Cerebro instance when using the "addstrategy" method.
        """
        self.buy_count = buy_count
        self.dataclose = self.datas[0].close
        self.data_length = sum(1 for _ in self.datas[0])-1
        self.buy_interval = self.data_length//buy_count
        print(f"buy ineterval: {self.buy_interval}")
        print(f"data length: {self.data_length}")
        self.buy_counter = 0
        # Customizing the plot name for the strategy
        self.plotinfo.plotname = 'Periodic Baseline Strategy'


    
    def next(self):
        """
        Defines the logic to be executed on each bar of the data feed.
        Buys shares at evenly spaced intervals.
        """
        current_index = len(self.data)
        if (current_index % self.buy_interval == 0 and
            self.buy_counter < self.buy_count):

            print("________________")
            self.log(f"BUY CREATE {self.dataclose[0]}")
            self.log(f"CASH: {self.broker.get_cash()}")
            self.buy(size=1)
            self.buy_counter += 1
            self.log(f"CURRENT POSITION SIZE: {self.position.size}")