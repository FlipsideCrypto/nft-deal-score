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
from utils import clean_name

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
ld.add_solana_sales()

# update listings
# ssn.scrape_listings(browser, ['smb'])
ssn.scrape_listings(browser)
ssn.scrape_randomearth(browser)
# ssn.scrape_listings(browser, ['smb','aurory'])

# update model
# ssn.convert_collection_names()
# sm.train_model(True, False)
# sm.train_model(False, False)

# sales = pd.read_csv('./data/sales.csv')
# listings = pd.read_csv('./data/listings.csv')
# listings.price.max()
# sales.price.max()

def add_model_sales():
	sales = pd.read_csv('./data/sales.csv').rename(columns={'sale_date':'block_timestamp'})
	print(sales.groupby('collection').token_id.count())
	sales.token_id.unique()
	sales.groupby('collection').token_id.count()
	sales[sales.collection == 'Galactic Punks']
	del sales['tx_id']
	old = pd.read_csv('./data/pred_price.csv').rename(columns={'rank':'nft_rank'})
	old = pd.read_csv('./data/pred_price copy.csv').rename(columns={'rank':'nft_rank'})
	old.groupby('collection').token_id.count()
	sales['token_id'] = sales.token_id.astype(int).astype(str)
	old['token_id'] = old.token_id.astype(str)
	sales = sales.merge( old[['collection','token_id','nft_rank']] )
	sales.head()
	sales['block_timestamp'] = sales.block_timestamp.apply(lambda x: str(x)[:19] )
	sales['price'] = sales.price.apply(lambda x: round(x, 2))
	print(sales.groupby('collection').token_id.count())
	sales.to_csv('./data/model_sales.csv', index=False)
	sales = pd.read_csv('./data/model_sales.csv')
	print(len(sales))
	sales = sales.drop_duplicates(subset=['collection','token_id','price'])
	print(len(sales))
	sales.to_csv('./data/model_sales.csv', index=False)


def update_token_ids():
	tokens = pd.read_csv('./data/tokens.csv')
	tokens['collection'] = tokens.collection.apply(lambda x: clean_name(x))
	tokens = tokens.drop_duplicates(subset=['collection','token_id'], keep='last')
	tokens.to_csv('./data/tokens.csv', index=False)
	tokens.groupby('collection').token_id.count()
	tokens['tmp'] = tokens.token_id.apply(lambda x: (int(float(x))) )
	tokens[tokens.token_id == 223838831896070003935953339589523931136]
	tokens[tokens.collection=='Galactic Punks']
	tokens['token_id'] = tokens.token_id.apply(lambda x: str(int(float(x))) )
	# tokens['tmp'] = tokens.token_id.apply(lambda x: len(x) )
	tokens.tmp.max()
	# df[ (df.collection == 'Pesky Penguins') & (df.token_id == '3362') ]
	tokens[ (tokens.collection == 'Pesky Penguins') & (tokens.token_id == '3362') ]
	tokens[ (tokens.collection == 'Pesky Penguins') & (tokens.token_id == 3362) ]
	# df.token_id.unique()
	c = 'sales'
	# for c in [ 'listings' ]:
	for c in [ 'attributes','sales','listings' ]:
		print(c)
		df = pd.read_csv('./data/{}.csv'.format(c))
		df['collection'] = df.collection.apply(lambda x: clean_name(x))
		# df.token_id.unique()
		df = df[df.token_id.notnull()]
		# df['token_id'] = df.token_id.apply(lambda x: None if x == 'nan' else str(int(float(x))) )
		df['token_id'] = df.token_id.apply(lambda x: None if x == 'nan' else str(int(float(x))) )
		# df['tmp'] = df.token_id.apply(lambda x: (str(x)[:5]))
		df['tmp'] = df.token_id.apply(lambda x: x[:10] )
		# tokens['tmp'] = tokens.token_id.apply(lambda x: x[:10] )
		# len(tokens)
		# len(tokens[['collection','token_id']].drop_duplicates())
		# len(tokens[['collection','tmp']].drop_duplicates())
		# df.to_csv('~/Downloads/tmp2.csv', index=False)
		if 'clean_token_id' in df.columns:
			del df['clean_token_id']

		# tokens[tokens.collection=='Galactic Punks']
		# len(tokens[tokens.collection=='Galactic Punks'])
		# tokens[(tokens.collection=='Galactic Punks') & (tokens.token_id=='25984997114855597728010029317878710272')]
		# 25984997114855639851202718743284654443
		# 25984997114855597728010029317878710272

		# a = set(tokens[tokens.collection=='Galactic Punks'].token_id.unique())
		# b = set(df[df.collection=='Galactic Punks'].token_id.unique())
		# len(a.intersection(b))
		# [ x for x in a if x in b ]
		# len([ x for x in a if x in b ])
		# df[(df.collection=='Galactic Punks')].token_id.values[0]
		df = df.merge(tokens[['collection','tmp','clean_token_id']], how='left', on=['collection','tmp'])
		# df[df.collection == 'Galactic Punks'].sort_values('clean_token_id')
		# print(df[ (df.clean_token_id.isnull()) & ( df.collection == 'Galactic Punks')])
		# print(len(df[ (df.clean_token_id.isnull()) & ( df.chain == 'Terra')]))
		# print(len(df[ (df.clean_token_id.isnull())]))
		# print(df[ (df.clean_token_id.isnull())].groupby('collection').token_id.count() )
		# print(df[ (df.clean_token_id.notnull())].groupby('collection').token_id.count() )
		# print(len(df[ (df.clean_token_id.notnull()) & ( df.collection == 'Galactic Punks')]))
		# min(df[df.collection == 'Galactic Punks'].token_id.values)
		# min(tokens[tokens.collection == 'Galactic Punks'].token_id.values)
		df['clean_token_id'] = df.clean_token_id.fillna(df.token_id).astype(float).astype(int).astype(str)
		# print(df[ (df.token_id.isnull()) & ( df.collection == 'Galactic Punks')])
		df[df.clean_token_id.isnull()].groupby('collection').token_id.count()
		df[df.clean_token_id.notnull()].groupby('collection').token_id.count()
		df['token_id'] = df.clean_token_id
		del df['clean_token_id']
		df[df.collection == 'Galactic Punks']
		print(df.groupby('collection').token_id.count() )
		df.to_csv('./data/{}.csv'.format(c), index=False)

# update_token_ids()
# add_model_sales()
sm.train_model()

