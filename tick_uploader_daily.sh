#!/bin/sh

today=`TZ="EST" date +%Y-%m-%d`
cd /home/ubuntu/algo-trading/
echo 'running upload process'
python3 algo_trading/ticks_uploader.py --from_date $today --to_date $today --low_limit 0 --top_limit 6000
python3 algo_trading/recalc_slopes.py
