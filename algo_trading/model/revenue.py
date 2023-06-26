from model.base import BaseClass

class Revenue(BaseClass):
    def __init__(self, symbol, year, quarter, type_revenue, amount, unit):
        self.symbol = symbol
        self.year = year
        self.quarter = quarter
        self.type_revenue = type_revenue
        self.amount = amount
        self.unit = unit
