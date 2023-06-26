import logging
from repository.mysql_db import MysqlDB
import logging
import yaml
import os

logger = logging.getLogger(__name__)
logging.basicConfig(filename=f'recal_slopes_{str(os.getpid())}.log', filemode = 'w', format='%(asctime)s -  %(levelname)s - %(message)s', level=logging.INFO)

def load_config():
    cfg = None
    with open('config/config.yaml', 'r') as file:
        cfg = yaml.safe_load(file)
    return cfg

def recalc_slopes(cfg):
    logger.info("starting ...")
    db_cfg = cfg['db']
    db_cfg = db_cfg['mysql']
    my_sql = MysqlDB(db_cfg['host'], db_cfg['user'], db_cfg['password'], db_cfg['database'])
    my_sql.connect()    

    logger.info("Calculating for (1) Period")
    my_sql.recalc_slopes(1)

    logger.info("Calculating for (3) Periods")
    my_sql.recalc_slopes(3)

    logger.info("Calculating for (7) Periods")
    my_sql.recalc_slopes(7)

    logger.info("Calculating for (14) Periods")
    my_sql.recalc_slopes(14)

    logger.info("Calculating for (21) Periods")
    my_sql.recalc_slopes(21)

    logger.info("Calculating for (30) Periods")
    my_sql.recalc_slopes(30)

    logger.info("Calculating for (60) Periods")
    my_sql.recalc_slopes(60)

    logger.info("Calculating for (90) Periods")
    my_sql.recalc_slopes(90)

    my_sql.disconnect()
    logger.info("Finishing ...")

def run():
    cfg = load_config()
    try:
        recalc_slopes(cfg)
    except Exception as e:
        logger.exception(e)

if __name__ == '__main__':
    run()
