import mysql.connector
from model.ticker import Ticker
import logging 

logger = logging.getLogger(__name__)
class MysqlDB:
    def __init__(self, host, user, password, db):
        self._host = host
        self._user = user
        self._password = password
        self._db = db

    def connect(self):
        self._connection = mysql.connector.connect(user=self._user,
                                                   database=self._db,
                                                   password=self._password,
                                                   host=self._host)

    
    def recalc_slopes(self, num_periods):
        cursor = self._connection.cursor()

        cursor.callproc('trading.CalcLatestSlopes', (num_periods,) )
        self._connection.commit()
        
        cursor.close()             
    
    def disconnect(self):
        self._connection.close()

    def get_all_tickers(self):
        tickers = []
        query = "select * from trading.Ticker order by Symbol;"
        cursor = self._connection.cursor()

        cursor.execute(query)

        for symbol, company_name in cursor:
            t = Ticker(symbol, company_name)
            tickers.append(t)

        cursor.close()                  
        return tickers
    
    def insert_tickers(self, tickers):
        add_ticker = ("INSERT INTO trading.Ticker "
              "(Symbol, CompanyName) "
              "VALUES (%(symbol)s, %(name)s)")
        cursor = self._connection.cursor()

        for ticker in tickers:
            cursor.execute(add_ticker, ticker)
        
        self._connection.commit()
        cursor.close()

    def insert_ticks(self, ticks):
        add_tick = ("INSERT INTO trading.Ticks "
              "(Symbol, Date, ClosePx, HighPx, LowPx, OpenPx, Vol)"
              "VALUES (%(symbol)s,%(date)s, %(close_px)s, %(high_px)s, %(low_px)s, %(open_px)s, %(vol)s)")
        cursor = self._connection.cursor()

        for tick in ticks:
            cursor.execute(add_tick, tick)
        
        self._connection.commit()
        cursor.close()

    def insert_revenues(self, revenues):
        add_revenue = ("INSERT INTO trading.Revenue "
              "(Symbol, Year, Quarter, Type, Amount, Unit)"
              "VALUES (%(symbol)s,%(year)s, %(quarter)s, %(type_revenue)s, %(amount)s, %(unit)s)")
        cursor = self._connection.cursor()

        for revenue in revenues:
            cursor.execute(add_revenue, revenue.__dict__)
        
        self._connection.commit()
        cursor.close()
