import logging
import yaml
from polygon import RESTClient
from repository.mysql_db import MysqlDB
from datetime import date, timedelta
from model.tick import Tick
import argparse
import logging
import os


logger = logging.getLogger(__name__)
logging.basicConfig(filename=f'ticks_uploader_{str(os.getpid())}.log', filemode = 'w', format='%(asctime)s -  %(levelname)s - %(message)s', level=logging.INFO)

def date_range(start_dt, end_dt):

    # difference between current and previous date
    delta = timedelta(days=1)

    # store the dates between two dates in a list
    dates = []

    while start_dt <= end_dt:
        # add current date to list by converting  it to iso format
        dates.append(str(start_dt.isoformat()))
        # increment start date by timedelta
        start_dt += delta
    return dates


def load_config():
    cfg = None
    with open('config/config.yaml', 'r') as file:
        cfg = yaml.safe_load(file)
    return cfg



def upload_ticks(cfg, pol_client, low_limit, top_limit, from_date, to_date):
    j = 1
    def upload_ticks_by_date_range(my_sql, pol_client, ticker, dates):
        nonlocal j
        logger.info(f"processing ticker {ticker.symbol}")
        ticks = []
        for cur_date in dates:
            try:
                response = pol_client.get_daily_open_close_agg(
                    ticker.symbol,
                    cur_date,
                )
                tick = Tick(ticker.symbol, 
                            cur_date, 
                            response.close,
                            response.high,
                            response.low,
                            response.open,
                            response.volume
                            )
                ticks.append(tick.__dict__)

                if j % 50 == 0:
                    my_sql.insert_ticks(ticks)
                    logger.info(f"inserted tick block, cur symbol: {ticker.symbol}")
                    ticks.clear()
                j = j + 1
            except Exception as e:
                logger.error(e)
        my_sql.insert_ticks(ticks)    
    logger.info("starting ...")
    db_cfg = cfg['db']
    db_cfg = db_cfg['mysql']
    my_sql = MysqlDB(db_cfg['host'], db_cfg['user'], db_cfg['password'], db_cfg['database'])
    my_sql.connect()    
    tickers = my_sql.get_all_tickers()

    tickers = tickers[low_limit: top_limit]

    dates = date_range(from_date, to_date)
    for ticker in tickers:
        upload_ticks_by_date_range(my_sql, pol_client, ticker, dates)

    my_sql.disconnect()
    logger.info("Finishing ...")

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--low_limit', type=int)
    parser.add_argument('--top_limit', type=int)
    parser.add_argument('--from_date')
    parser.add_argument('--to_date')
    args = parser.parse_args()

    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)

    cfg = load_config()
    key = cfg['polygon']['key']
    client = RESTClient(key)
    upload_ticks(cfg, client, args.low_limit, args.top_limit, from_date, to_date)



if __name__ == '__main__':
    run()
