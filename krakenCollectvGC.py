"""Script to gather market data from OKCoin Spot Price API."""
from urllib2.request import Request, urlopen
import json
import time
import numpy as np
from pytz import utc
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
import krakenex

"""client = MongoClient()
database = client['okcoindb']
collection = database['historical_data']"""


def tick():
    """Gather market data from OKCoin Spot Price API and insert them into a
    MongoDB collection."""

    print("in tick")
    k = krakenex.API()
    data = k.query_public('Depth',{'pair': 'BTCEUR', 'count': '502'})
    print("got data")
    
    time_ = time.time()
    labels = ['price','volume','date']

    bids = pd.DataFrame(data['result']['XXBTZEUR']['bids'],columns=labels,dtype=float)
    bids['way'] = 'b'

    asks = pd.DataFrame(data['result']['XXBTZEUR']['asks'],columns=labels,dtype=float)
    asks['way'] = 'a'

    orders=pd.concat([bids, asks], axis=0)  
    orders['time'] = time_
    orders = orders.reset_index(drop=True)
    orders['time'].astype('float',copy=False)
    orders['way'].astype('str',copy=False,errors='ignore')
    orders.to_csv('KrakenOrderBook', mode='a', header=False)
    
    print("inserted")


def main():
    """Run tick() at the interval of every ten seconds."""
    scheduler = BlockingScheduler(timezone=utc)
    scheduler.add_job(tick, 'interval', seconds=60)
    try:
        print("start")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
