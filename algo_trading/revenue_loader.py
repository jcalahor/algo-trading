import logging
import yaml
from polygon import RESTClient
from repository.mysql_db import MysqlDB
from model.revenue import Revenue
import argparse
import os

logger = logging.getLogger(__name__)
logging.basicConfig(filename=f'revenue_loader_{str(os.getpid())}.log',
                    filemode = 'w', format='%(asctime)s -  %(levelname)s - %(message)s',
                    level=logging.INFO)

def load_config():
    cfg = None
    with open('config/config.yaml', 'r') as file:
        cfg = yaml.safe_load(file)
    return cfg

def store_ticker_revenues(my_sql, ticker_revenues):
    my_sql.insert_revenues(ticker_revenues)

def get_ticker_revenues(client, ticker, year, quarter):
    revenues = []
    year_str = str(year)
    financials = []
    for f in client.vx.list_stock_financials(ticker.symbol):
        financials.append(f)

    for financial in financials:
        if (financial.fiscal_year == year_str and financial.fiscal_period == quarter):
            if (financial.financials and
                financial.financials.income_statement and
                financial.financials.income_statement.gross_profit and 
                financial.financials.income_statement.revenues):
                revenues.append(Revenue(ticker.symbol,
                            year, 
                            quarter,
                            'REVENUE', 
                            financial.financials.income_statement.revenues.value,
                            financial.financials.income_statement.revenues.unit))
                revenues.append(Revenue(ticker.symbol,
                            year, 
                            quarter,
                            'GROSS_PROFIT', 
                            financial.financials.income_statement.gross_profit.value,
                            financial.financials.income_statement.gross_profit.unit))
    return revenues

def upload_revenues(cfg, client, year, quarter):
    logger.info("starting ...")
    db_cfg = cfg['db']
    db_cfg = db_cfg['mysql']
    my_sql = MysqlDB(db_cfg['host'], db_cfg['user'], db_cfg['password'], db_cfg['database'])
    my_sql.connect()
    
    tickers = my_sql.get_all_tickers()

    i = 1
    cumulated_ticker_revenues = []
    for ticker in tickers:
        ticker_revenues = get_ticker_revenues(client, ticker, year, quarter)
        if ticker_revenues:
            for ticker_revenue in ticker_revenues:
                cumulated_ticker_revenues.append(ticker_revenue)
        if i % 20 == 0:
            store_ticker_revenues(my_sql, cumulated_ticker_revenues)
            i = 1
            cumulated_ticker_revenues.clear()
        else:
            i = i + 1            

    store_ticker_revenues(my_sql, cumulated_ticker_revenues)
    my_sql.disconnect()
    logger.info("Finishing ...")

def run():
    cfg = load_config()
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int)
    parser.add_argument('--quarter')
    args = parser.parse_args()

    key = cfg['polygon']['key']
    client = RESTClient(key)    
    upload_revenues(cfg, client, args.year, args.quarter)



if __name__ == '__main__':
    run()
