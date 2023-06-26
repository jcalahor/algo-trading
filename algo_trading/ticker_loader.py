import logging
import yaml
from polygon import RESTClient
from repository.mysql_db import MysqlDB

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def load_config():
    cfg = None
    with open('config/config.yaml', 'r') as file:
        cfg = yaml.safe_load(file)
    return cfg


def upload_tickers(cfg, tickers):
    logger.info("starting ...")
    db_cfg = cfg['db']
    db_cfg = db_cfg['mysql']
    my_sql = MysqlDB(db_cfg['host'], db_cfg['user'], db_cfg['password'], db_cfg['database'])
    my_sql.connect()
    
    insert_tickers = []
    i = 1
    for ticker in tickers:
        insert_tickers.append(
            {
                'symbol': ticker.ticker,
                'name': ticker.name
            }
        )

        if not i % 100:
            my_sql.insert_tickers(insert_tickers)
            insert_tickers.clear()
        i = i + 1

    my_sql.insert_tickers(insert_tickers)

    my_sql.disconnect()
    logger.info("Finishing ...")

def run():
    cfg = load_config()
    key = cfg['polygon']['key']
    client = RESTClient(key)
    tickers = []
    for t in client.list_tickers(market="stocks", type="CS", active=True, limit=1000):
        tickers.append(t)
    upload_tickers(cfg, tickers)



if __name__ == '__main__':
    run()
