from model.base import BaseClass

class Ticker(BaseClass):
    def __init__(self, symbol, name):
        self.symbol = symbol
        self.name = name
