"""
BUY DIPS STRATEGY

This module implements a trading strategy that buys assets during price dips.
The strategy buys a specified number of shares when the closing price drops by
a certain percentage from the highest observed price.
"""
import backtrader as bt

class BuyDips(bt.Strategy):
    """
    A trading strategy that buys assets during price dips.

    Attributes:
        size (int): The number of shares to buy on each dip.
        dip_size (float): The percentage drop from the highest price to trigger a buy.
        dataclose (LineSeries): The close prices of the data feed.
        high (float): The highest price observed.
        buy_count (int): The number of times a buy order has been executed.
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
        

    def __init__(self, buy_size, dip_size):
        """
        Initializes the BuyDips strategy.


        Args:
            buy_size (int): The number of shares to buy on each dip.
            dip_size (float): The percentage drop from the highest price to 
                              trigger a buy.
        Note: Args are passed to Cerebro instance when using the 
            "addstrategy" method
        """
        self.size = buy_size
        self.dip_size = dip_size
        self.dataclose = self.datas[0].close
        self.high = None
        self.buy_count = 0
        # Customizing the plot name for the strategy
        self.plotinfo.plotname = 'Buy Dips Strategy'

    def next(self):
        """
        Defines the logic to be executed on each bar of the data feed.
        Buys shares when the closing price drops by the specified dip size from     
        the highest observed price. 
        The buy is placed as a market order which is executed at next day's open
        """
        # Update the highest observed price
        if not self.high or self.high<self.dataclose[0]:
            self.high = self.dataclose[0]
        # Check if the price has dipped enough to trigger a buy
        if self.dataclose[0] < self.high*(1-self.dip_size):
            print("________________")
            self.log(f"BUY CREATE {self.dataclose[0]}, CURRENT HIGH: {self.high}")
            self.buy_count += 1
            self.log(f"CASH: {round(self.broker.get_cash(),2)}")
            # defaults to a market order which will buy next day's close
            self.buy(size=self.size) 
            self.log(f"CURRENT POSITION SIZE: {self.position.size}")
        



        
