from model.base import BaseClass

class Tick(BaseClass):
    def __init__(self, symbol, date, close_px, high_px, low_px, open_px, vol):
        self.symbol = symbol
        self.date = date
        self.close_px = close_px
        self.high_px = high_px
        self.low_px = low_px
        self.open_px = open_px
        self.vol = vol
