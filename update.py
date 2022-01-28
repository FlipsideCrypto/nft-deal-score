import os
import pandas as pd
from selenium import webdriver
from time import sleep
from copy import deepcopy
import random

os.chdir('/Users/kellenblumberg/git/nft-deal-score')
os.environ['PATH'] += os.pathsep + '/Users/kellenblumberg/shared/'

import scrape_sol_nfts as ssn
import load_data as ld
import solana_model as sm

browser = webdriver.Chrome()



if False:
    alerted = []
    for i in range(10):
        ssn.scrape_randomearth(browser)
        update_token_ids()
        listings = pd.read_csv('./data/listings.csv')
        listings = listings[listings.chain == 'Terra']
        listings.collection.unique()
        alerted = ssn.calculate_deal_scores(listings, alerted)
        sleep(10 * 60)



# sales = pd.read_csv('./data/sales.csv')
# pred_price = pd.read_csv('./data/pred_price.csv').sort_values('token_id')
# pred_price['rank'] = pred_price.groupby('collection').pred_price.rank(ascending=0)
# sales = sales.merge(pred_price[['collection','token_id','rank']])
# sales = sales[ sales.collection.isin(['Solana Monkey Business','Degen Apes','Aurory','Pesky PenguinsPesky Penguins','Thugbirdz']) ]
# sales = sales[sales['rank']<=10].sort_values('price', ascending=0)
# # sales = sales.sort_values('price', ascending=0).groupby('collection').head(3)[['collection','sale_date','token_id','price','rank']].sort_values('collection')
# d = {
#     'Solana Monkey Business':	140,
#     'Aurory':	18.5,
#     'Degen Apes':	34,
#     'Thugbirdz':	40,
# }
# sales['current_floor'] = sales.collection.apply(lambda x: d[x] )
# sales['floor_ratio'] = sales.price / sales.current_floor
# sales.to_csv('~/Downloads/tmp.csv', index=False)

# update sales
ssn.scrape_recent_smb_sales(browser)
ssn.scrape_recent_sales()
ld.add_terra_sales()

# update listings
ssn.scrape_randomearth(browser)
# ssn.scrape_listings(browser, ['smb','aurory'])
ssn.scrape_listings(browser)

# update model
# ssn.convert_collection_names()
# sm.train_model(True, False)
# sm.train_model(False, False)
# sm.train_model(False, True)

def update_token_ids():
    tokens = pd.read_csv('./data/tokens.csv')
    tokens.groupby('collection').token_id.count()
    tokens['token_id'] = tokens.token_id.astype(str)
    # df[ (df.collection == 'Pesky Penguins') & (df.token_id == '3362') ]
    tokens[ (tokens.collection == 'Pesky Penguins') & (tokens.token_id == '3362') ]
    tokens[ (tokens.collection == 'Pesky Penguins') & (tokens.token_id == 3362) ]
    # df.token_id.unique()
    c = 'listings'
    for c in [ 'attributes','sales','listings' ]:
        print(c)
        df = pd.read_csv('./data/{}.csv'.format(c))
        df['token_id'] = df.token_id.astype(str)
        if 'clean_token_id' in df.columns:
            del df['clean_token_id']
        
        df = df.merge(tokens[['collection','token_id','clean_token_id']], how='left')
        df[df.collection == 'Galactic Punks']
        df['clean_token_id'] = df.clean_token_id.fillna(df.token_id).astype(int)
        df[df.clean_token_id.isnull()].groupby('collection').token_id.count()
        df[df.clean_token_id.notnull()].groupby('collection').token_id.count()
        df['token_id'] = df.clean_token_id
        del df['clean_token_id']
        df.to_csv('./data/{}.csv'.format(c), index=False)
update_token_ids()

