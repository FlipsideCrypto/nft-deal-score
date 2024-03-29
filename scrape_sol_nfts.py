import collections
from curses import meta
from lib2to3.pgen2.tokenize import tokenize
from operator import index
import os
import re
import json
import time
from secrets import token_bytes
import time
import requests
import functools
import pandas as pd
import urllib.request
from time import sleep
from scipy.stats import norm
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
from selenium.webdriver.common.keys import Keys
import cloudscraper

os.chdir('/Users/kellenblumberg/git/nft-deal-score')
os.environ['PATH'] += os.pathsep + '/Users/kellenblumberg/shared/'

from utils import clean_token_id, get_ctx, merge, clean_name

# howrare.is api
# https://api.howrare.is/v0.1/collections/smb/only_rarity
# https://api.howrare.is/v0.1/collections/smb

# browser = webdriver.Chrome()

# old = pd.read_csv('./data/tokens.csv')
# metadata[(metadata.collection == 'Galactic Punks') & (metadata.feature_name=='attribute_count')].drop_duplicates(subset=['feature_value']).merge(old)

# url = 'https://api.solscan.io/collection/nft?sortBy=nameDec&collectionId=f046bec0889c9d431ce124a626237e2236bc2527051d32ed31f6b5e6dc230669&offset=0&limit=500'
# r = requests.get(url)
# j = r.json()
# j.keys()
# len(j['data'])
# j['data'][0]


def add_to_df(t_data):
	old = pd.read_csv('./data/tokens_2.csv')
	sorted(old.collection.unique())
	l0 = len(old)
	do_merge = False
	tokens = pd.DataFrame(t_data, columns=['collection','token_id','nft_rank','mint_address','image_url'])
	len(tokens)
	tokens[tokens.nft_rank.isnull()]
	tokens['collection'] = tokens.collection.apply(lambda x: 'Catalina Whale Mixer' if x == 'Catalina Whales' else x )
	rem = [ 'Jikan Studios','Fine Fillies' ]
	print(tokens.groupby('collection').token_id.count())
	tokens['clean_token_id'] = tokens.token_id
	tokens['chain'] = 'Solana'
	tokens = tokens[-tokens.collection.isin(rem)]
	if do_merge:
		old['token_id'] = old.token_id.astype(str)
		tokens['token_id'] = tokens.token_id.astype(str)
		old = old.merge(tokens, how='left', on=['collection','token_id'])
		old[old.collection == 'Solana Monkey Business']
		for c in [ 'nft_rank','mint_address','image_url' ]:
			old[c] = old[c+'_x'].fillna(old[c+'_y'])
			del old[c+'_x']
			del old[c+'_y']
		old['clean_token_id'] = old.clean_token_id.fillna(old.token_id)
		old['chain'] = old.chain.fillna('Solana')
	else:
		# old = old.append(tokens)
		old = pd.concat( [old, tokens] )
		old['token_id'] = old.token_id.astype(str)
		old = old.drop_duplicates(subset=['collection','token_id'], keep='last')
	print('Adding {} rows'.format(len(old) - l0))
	old[old.collection.isin(tokens.collection.unique())]
	old[(old.collection.isin(tokens.collection.unique())) & (old.token_id == '6437')]
	old[old.nft_rank.isnull()].groupby('collection').token_id.count()
	old = old[-old.collection.isin(['Astrals','Dazedducks','Nyanheroes','Shadowysupercoder','Taiyorobotics'])]
	old.to_csv('./data/tokens_2.csv', index=False)
	# tokens.to_csv('./data/tokens_2.csv', index=False)

def compile():
	ctx = get_ctx()
	query = 'SELECT DISTINCT address FROM silver_CROSSCHAIN.ADDRESS_LABELS'
	seen = ctx.cursor().execute(query)
	seen = pd.DataFrame.from_records(iter(seen), columns=[x[0] for x in seen.description])
	seen = sorted(list(seen.ADDRESS.unique()))

	tokens = pd.read_csv('./data/tokens.csv')
	tokens = tokens[tokens.chain == 'Solana']
	single_update_auth_labels = pd.read_csv('./data/single_update_auth_labels.csv')
	mult_update_auth_labels = pd.read_csv('./data/mult_update_auth_labels.csv')
	df = tokens.append(single_update_auth_labels).append(mult_update_auth_labels)
	df = df[ (df.collection != 'Nan') & (df.collection != 'nan') & (df.collection.notnull()) ]
	df = df[-(df.mint_address.isin(seen))]
	df = df.drop_duplicates(subset=['mint_address'], keep='first')
	# len(df)
	# len(df.collection.unique())
	# df.head()
	# df.mint_address.tail(11000).head(5)
	# df[df.mint_address == '2GgPNKGyzAQL4mriuH4kBpntYCNVSM2pQfzdsu3p8du5']
	# df['seen'] = df.mint_address.isin(seen).astype(int)
	# tmp = df[df.seen == 0].groupby('collection').mint_address.count().reset_index().sort_values('mint_address', ascending=0)
	# tmp.head(40)
	# tmp.mint_address.sum()
	df[df.mint_address.isnull()]
	df[['mint_address','collection']].to_csv('~/Downloads/solana-nft-labels-06-29.csv', index=False)


def add_to_df(t_data, metadata, exclude_new = False):
	old = pd.read_csv('./data/tokens.csv')
	g0 = old.groupby('collection').token_id.count().reset_index()
	sorted(old.collection.unique())
	l0 = len(old)
	do_merge = False
	tokens = pd.DataFrame(t_data, columns=['collection','token_id','nft_rank','mint_address','image_url'])
	len(tokens)
	tokens[tokens.nft_rank.isnull()]
	tokens['collection'] = tokens.collection.apply(lambda x: 'Catalina Whale Mixer' if x == 'Catalina Whales' else x )
	# rem = [ 'Jikan Studios','Fine Fillies' ]
	# print(tokens.groupby('collection').token_id.count())
	metadata['collection'] = metadata.collection.apply(lambda x: 'Catalina Whale Mixer' if x == 'Catalina Whales' else x )
	tokens['clean_token_id'] = tokens.token_id
	tokens['chain'] = 'Solana'
	# tokens = tokens[-tokens.collection.isin(rem)]
	# metadata = metadata[-metadata.collection.isin(rem)]
	if do_merge:
		old['token_id'] = old.token_id.astype(str)
		tokens['token_id'] = tokens.token_id.astype(str)
		old = old.merge(tokens, how='left', on=['collection','token_id'])
		old[old.collection == 'Solana Monkey Business']
		for c in [ 'nft_rank','mint_address','image_url' ]:
			old[c] = old[c+'_x'].fillna(old[c+'_y'])
			del old[c+'_x']
			del old[c+'_y']
		old['clean_token_id'] = old.clean_token_id.fillna(old.token_id)
		old['chain'] = old.chain.fillna('Solana')
	else:
		# old = old.append(tokens)
		old['collection'] = old.collection.apply(lambda x: clean_name(x))
		tokens['collection'] = tokens.collection.apply(lambda x: clean_name(x))
		if exclude_new:
			rem = tokens.collection.unique()
			old = old[-(old.collection.isin(rem))]
		old = pd.concat( [old, tokens] )
		old['token_id'] = old.token_id.astype(str)
		old = old.drop_duplicates(subset=['collection','token_id'], keep='last')
	g1 = old.groupby('collection').token_id.count().reset_index()
	g = g0.merge(g1, how='outer', on=['collection']).fillna(0)
	g['dff'] = g.token_id_y - g.token_id_x
	print(g[g.dff != 0].sort_values('dff', ascending=0))
	g[g.dff != 0].sort_values('dff', ascending=0).to_csv('~/Downloads/tmp.csv', index=False)
	print('Adding {} rows'.format(len(old) - l0))
	old = old[old.collection != 'Solanamonkeybusiness (Smb)']
	# old[old.collection.isin(tokens.collection.unique())]
	# old[(old.collection.isin(tokens.collection.unique())) & (old.token_id == '6437')]
	old[old.nft_rank.isnull()].groupby('collection').token_id.count()
	# old = old[-old.collection.isin(['Astrals','Dazedducks','Nyanheroes','Shadowysupercoder','Taiyorobotics'])]
	old.to_csv('./data/tokens.csv', index=False)
	# tokens.to_csv('./data/tokens_2.csv', index=False)

	old = pd.read_csv('./data/metadata.csv')
	g0 = old.groupby('collection').token_id.count().reset_index()
	l0 = len(old)
	old['collection'] = old.collection.apply(lambda x: clean_name(x))
	metadata['collection'] = metadata.collection.apply(lambda x: clean_name(x))
	if exclude_new:
		rem = metadata.collection.unique()
		old = old[-(old.collection.isin(rem))]
	# old = old[-old.collection.isin(['Astrals','Dazedducks','Nyanheroes','Shadowysupercoder','Taiyorobotics'])]
	# a = old[['collection','token_id']].drop_duplicates()
	# a['exclude'] = 0
	# a['token_id'] = a.token_id.astype(str)
	# metadata['token_id'] = metadata.token_id.astype(str)
	# m = metadata.merge(a, how='left')
	# m = m[m.exclude.isnull()]
	# len(m[m.exclude.isnull()].token_id.unique())
	# del m['exclude']
	# old = old[-(old.collection == 'Meerkat Millionaires Cc')]
	# print(sorted(old.collection.unique()))
	# metadata.collection.unique()
	# metadata = pd.DataFrame(t_data, columns=['collection','token_id','nft_rank','mint_address','image_url'])
	# old = old.merge(tokens, how='left', on=['collection','token_id'])
	# old = old.append(m[['collection','token_id','name','value']].rename(columns={'name':'feature_name','value':'feature_value'}) )
	old = pd.concat( [old, metadata[['collection','token_id','name','value']].rename(columns={'name':'feature_name','value':'feature_value'})] )
	old['token_id'] = old.token_id.astype(str)
	old = old.drop_duplicates(subset=['collection','token_id','feature_name'], keep='last')
	# old['nft_rank'] = old.nft_rank_y.fillna(old.nft_rank_y)
	# del old['nft_rank_x']
	g1 = old.groupby('collection').token_id.count().reset_index()
	g = g0.merge(g1, how='outer', on=['collection']).fillna(0)
	g['dff'] = g.token_id_y - g.token_id_x
	print(g[g.dff != 0].sort_values('dff', ascending=0))
	# del old['nft_rank_y']
	print('Adding {} rows'.format(len(old) - l0))
	# print(old.groupby('collection').token_id.count())
	# old[old.collection.isin(metadata.collection.unique())]
	# old[(old.collection == 'Catalina Whale Mixer') & (old.token_id == '1206')]
	old.to_csv('./data/metadata.csv', index=False)
	# metadata.to_csv('./data/metadata_2.csv', index=False)

def how_rare_is_api():
	ctx = get_ctx()
	query = '''
		SELECT DISTINCT LOWER(project_name) AS lower_collection
		FROM solana.core.dim_nft_metadata
	'''
	df = ctx.cursor().execute(query)
	df = pd.DataFrame.from_records(iter(df), columns=[x[0] for x in df.description])

	url = 'https://api.howrare.is/v0.1/collections'
	r = requests.get(url)
	j = r.json()
	j['result'].keys()
	j['result']['data'][:10]
	c_df = pd.DataFrame(j['result']['data']).sort_values('floor_marketcap', ascending=0)
	c_df['lower_collection'] = c_df.url.apply(lambda x: x.lower().strip() )
	seen = sorted(df.LOWER_COLLECTION.apply(lambda x: re.sub(' |_|\'', '', x) ).values)
	# seen[:300]
	# x = 590
	# seen[x:x+50]
	c_df['seen_1'] = c_df.url.apply(lambda x: re.sub(' |_|\'', '', x[1:]).lower() in seen ).astype(int)
	c_df['seen_2'] = c_df.name.apply(lambda x: re.sub(' |_|\'', '', x).lower() in seen ).astype(int)
	c_df['seen'] = (c_df.seen_1 + c_df.seen_2 > 0).astype(int)
	c_df.head()
	c_df.seen.sum()
	c_df[c_df.seen == 0].head(10)
	# c_df.head(16)
	seen = [ 'smb','aurory','degenapes','thugbirdz','degods','okay_bears','catalinawhalemixer','cetsoncreck','stonedapecrew','solgods' ]
	c_df = c_df[-(c_df.url.isin([ '/'+x for x in seen]))]
	# rem = [ 'kaiju','jikanstudios' ]
	# c_df = c_df[-(c_df.url.isin([ '/'+x for x in rem]))]
	# seen = list(pd.read_csv('./data/tokens.csv').collection.unique())
	# c_df = c_df[-(c_df.name.isin(seen))]
	# len(j['result']['data'])
	# c_df = c_df[c_df.url.isin(['/blocksmithlabs'])]
	# c_df = c_df[c_df.url.isin(['/generousrobotsdao','/thestonedfrogs'])]
	c_df = c_df[c_df.seen == 0]
	sorted(c_df.url.unique())
	it = 0
	tot = len(c_df)
	# c_df.head()
	# c_df = c_df[c_df.url != '/midnightpanthers']
	t_data = []
	m_data = []
	# metadata = pd.DataFrame()
	for row in c_df.iterrows():
		it += 1
		row = row[1]
		collection = row['name']
		print('#{} / {}: {}'.format(it, tot, collection))
		url = row['url'][1:]
		if it > 1:
			assert(len(t_data))
			assert(len(m_data))
		print('Working on collection {}, {}, {}'.format(collection, len(t_data), len(m_data)))
		# if url in seen or (len(metadata) and collection in metadata.collection.unique()):
		# 	print('Seen!')
		# 	continue
		# collection = 'Cets on Creck'
		# collection = 'SOLGods'
		# collection = 'Meerkat Millionaires'
		# collection = d['url'][1:]
		# url = 'https://api.howrare.is/v0.1/collections'+d['url']
		# url = 'https://api.howrare.is/v0.1/collections/meerkatmillionaires'
		# url = 'https://api.howrare.is/v0.1/collections/'+url+'/only_rarity'
		url = 'https://api.howrare.is/v0.1/collections/'+url
		r = requests.get(url)
		j = r.json()
		for i in j['result']['data']['items']:
			try:
				token_id = int(i['id'])
				if True:
					nft_rank = int(i['rank'])
					mint = i['mint']
					image = i['image']
					t_data += [[ collection, token_id, nft_rank, mint, image ]]
				if False:
					for d in i['attributes']:
						d['token_id'] = token_id
						d['collection'] = collection
						m_data += [ d ]
				# metadata = metadata.append(m)
				# metadata = pd.concat([metadata, m])
			except:
				print('Error')
		# add_to_df(t_data)
	metadata = pd.DataFrame(m_data)
	metadata

	add_to_df(t_data, metadata, True)
	metadata.head()
	metadata.value.unique()

def convert_collection_names():
	for c in [ 'pred_price', 'attributes', 'feature_values', 'model_sales', 'listings', 'coefsdf', 'tokens' ]:
		try:
			df = pd.read_csv('./data/{}.csv'.format(c))
			df['collection'] = df.collection.apply(lambda x: clean_name(x) if x in clean_names.keys() else x )
			df.to_csv('./data/{}.csv'.format(c), index=False)
		except:
			print('error',c)
			pass

def scrape_magic_eden_sales():
	url = 'https://api-mainnet.magiceden.io/rpc/getGlobalActivitiesByQuery?q={%22$match%22:{%22collection_symbol%22:%22pesky_penguins%22},%22$sort%22:{%22blockTime%22:-1},%22$skip%22:0}'
	results = requests.get(url).json()['results']
	df = pd.DataFrame([ x['parsedTransaction'] for x in results if 'parsedTransaction' in x.keys()])
	df[[ 'blockTime','collection_symbol','total_amount' ]]

def metadata_from_uri():
	df = pd.read_csv('./data/mint_address_token_id_map.csv')
	tokens = pd.read_csv('./data/tokens.csv')
	tokens['collection'] = tokens.collection.apply(lambda x: clean_name(x))
	tokens[tokens.collection == 'Stoned Ape Crew']
	sorted(tokens.collection.unique())
	collections = [ 'Stoned Ape Crew' ]
	data = []
	t_data = []
	# seen = [ x[1] for x in t_data]
	for collection in collections:
		print(collection)
		it = 0
		cur = df[df.collection == collection]
		seen = []
		for row in cur.iterrows():
			it += 1
			if it % 250 == 2:
				print('{} {} {}'.format(it, len(data), len(t_data)))
			row = row[1]
			uri = row['uri']
			token_id = row['token_id']
			if token_id in seen:
				continue
			try:
				j = requests.get(uri, timeout=3).json()
				for a in j['attributes']:
					data += [[ collection, token_id, a['trait_type'], a['value'] ]]
				t_data += [[ collection, token_id, j['image'] ]]
				seen.append(token_id)
			except:
				print(row['uri'])
	old = pd.read_csv('./data/metadata.csv')
	l0 = len(old)
	metadata = pd.DataFrame(data, columns=['collection','token_id','feature_name','feature_value'])
	metadata['chain'] = 'Solana'
	old['token_id'] = old.token_id.astype(str)
	metadata['token_id'] = metadata.token_id.astype(str)
	old = old.append(metadata).drop_duplicates(subset=['collection','token_id','feature_name'])
	l1 = len(old)
	print('Adding {} rows to metadata'.format(l1 - l0))
	# old['chain'] = old.collection.apply(lambda x: 'Terra' if x in ['Galactic Punks','Levana Dragon Eggs','LunaBulls'] else 'Solana')
	print(old.groupby(['chain','collection']).token_id.count())
	old.to_csv('./data/metadata.csv', index=False)

	old = pd.read_csv('./data/tokens.csv')
	l0 = len(old)
	tokens = pd.DataFrame(t_data, columns=['collection','token_id','image_url'])
	old['collection'] = old.collection.apply(lambda x: clean_name(x))
	old['token_id'] = old.token_id.astype(str)
	tokens['token_id'] = tokens.token_id.astype(str)
	old = old.merge(tokens, how='left', on=['collection','token_id'])
	old['image_url'] = old.image_url_y.fillna(old.image_url_x)
	del old['image_url_x']
	del old['image_url_y']
	tmp = old[old.collection == 'Stoned Ape Crew']
	tmp['tmp'] = tmp.image_url.apply(lambda x: x[:20] )
	tmp.groupby('tmp').token_id.count()
	old = old.drop_duplicates(subset=['collection','token_id'], keep='last')
	l1 = len(old)
	print('Adding {} rows to tokens'.format(l1 - l0))
	old.to_csv('./data/tokens.csv', index=False)

def scrape_not_found(browser):
	url = 'https://notfoundterra.com/lunabulls'
	browser.get(url)
	data = []
	for i in range(1, 9999+1):
		t = browser.find_element_by_id('text-field')
		if i % 100 == 0:
			print(i, len(data))
		t.clear()
		t.send_keys(Keys.CONTROL, 'a')
		sleep(0.1)
		for _ in range(len(str(i))):
			t.send_keys(Keys.BACKSPACE)
			sleep(0.1)
		# t.send_keys(u'\ue009' + u'\ue003')
		t.send_keys('{}'.format(i))
		b = browser.find_elements_by_class_name('MuiButton-label')
		try:
			b[0].click()
		except:
			sleep(1)
			b = browser.find_elements_by_class_name('MuiButton-label')
			b[0].click()

		rk = browser.find_elements_by_class_name('Statistics-Rank2')
		score = browser.find_elements_by_class_name('MuiListItemText-secondary')
		data += [[ 'LunaBulls', i, rk[0].text, score[-1].text ]]
		sleep(0.1)
	df = pd.DataFrame(data, columns=['collection','token_id','nft_rank','nft_score']).drop_duplicates()
	# df['collection'] = 'Galactic Punks'
	df.to_csv('~/Downloads/lb_ranks.csv', index=False)
	df['nft_rank'] = df.nft_rank.apply(lambda x: re.split('/|\)|\(', x)[1] )
	df.to_csv('./data/lp_ranks.csv', index=False)

def calculate_deal_scores(listings, alerted):
	listings = listings.sort_values('price')
	t1 = listings.groupby('collection').head(1).rename(columns={'price':'t1'})
	t2 = listings.groupby('collection').head(2).groupby('collection').tail(1).rename(columns={'price':'t2'})
	t = t1.merge(t2, on=['collection'])
	t['pct'] = t.t2 / t.t1
	t['dff'] = t.t2 - t.t1
	t = t[ (t.pct >= 1.15) | (t.dff >= 10 ) ]

	pred_price = pd.read_csv('./data/pred_price.csv')[['collection','token_id','pred_price','pred_sd']]
	pred_price['collection'] = pred_price.collection.apply(lambda x: clean_name(x))
	pred_price['token_id'] = pred_price.token_id.astype(str)
	listings['token_id'] = listings.token_id.astype(str)
	pred_price.collection.unique()
	listings.collection.unique()
	pred_price.head()
	listings.head()
	pred_price = pred_price.merge(listings[['collection','token_id','price']], on=['collection','token_id'])

	coefsdf = pd.read_csv('./data/coefsdf.csv')
	coefsdf['collection'] = coefsdf.collection.apply(lambda x: clean_name(x))
	coefsdf['tot'] = coefsdf.lin_coef + coefsdf.log_coef
	coefsdf['lin_coef'] = coefsdf.lin_coef / coefsdf.tot
	coefsdf['log_coef'] = coefsdf.log_coef / coefsdf.tot
	pred_price = pred_price.merge(coefsdf)
	floor = listings.groupby('collection').price.min().reset_index().rename(columns={'price':'floor'})
	pred_price = pred_price.merge(floor)

	# metadata = pd.read_csv('./data/metadata.csv')
	# solana_blob = metadata[ (metadata.collection == 'aurory') & (metadata.feature_name == 'skin') & (metadata.feature_value == 'Solana Blob (9.72%)')].token_id.unique()
	# pred_price['pred_price'] = pred_price.apply(lambda x: (x['pred_price'] * 0.8) - 4 if x['token_id'] in solana_blob and x['collection'] == 'Aurory' else x['pred_price'], 1 )

	# solana_blob = metadata[ (metadata.collection == 'aurory') & (metadata.feature_name == 'hair') & (metadata.feature_value == 'Long Blob Hair (9.72%)')].token_id.unique()
	# pred_price['pred_price'] = pred_price.apply(lambda x: (x['pred_price'] * 0.8) - 2 if x['token_id'] in solana_blob and x['collection'] == 'Aurory' else x['pred_price'], 1 )

	pred_price['abs_chg'] = (pred_price.floor - pred_price.floor_price) * pred_price.lin_coef
	pred_price['pct_chg'] = (pred_price.floor - pred_price.floor_price) * pred_price.log_coef
	pred_price['pred_price_0'] = pred_price.pred_price
	pred_price['pred_price'] = pred_price.apply( lambda x: x['pred_price'] + x['abs_chg'] + ( x['pct_chg'] * x['pred_price'] / x['floor_price'] ), 1 )
	pred_price['pred_price'] = pred_price.apply( lambda x: max( x['pred_price'], x['floor']), 1 )
	# pred_price['deal_score'] = pred_price.apply( lambda x: (( x['pred_price'] - x['price'] ) * 50 / ( 3 * x['pred_sd'])) + 50  , 1 )
	# pred_price['deal_score'] = pred_price.deal_score.apply( lambda x: min(max(0, x), 100) )
	pred_price['deal_score'] = pred_price.apply( lambda x: 100 * (1 - norm.cdf( x['price'], x['pred_price'], 2 * x['pred_sd'] * x['pred_price'] / x['pred_price_0'] )) , 1 )

	pred_price = pred_price.sort_values(['deal_score'], ascending=[0])
	pred_price[pred_price.token_id=='1656']
	g = pred_price.groupby('collection').head(10)[['collection','token_id','deal_score','price']]
	n1 = g.groupby('collection').head(2).groupby('collection').head(1)[['collection','deal_score']].rename(columns={'deal_score':'ds_1'})
	n2 = g.groupby('collection').head(2).groupby('collection').tail(1)[['collection','deal_score']].rename(columns={'deal_score':'ds_2'})
	n3 = g.groupby('collection').head(3).groupby('collection').tail(1)[['collection','deal_score']].rename(columns={'deal_score':'ds_3'})
	g = g.merge(n1)
	g = g.merge(n2)
	g = g.merge(n3)
	g['m2'] = g.ds_1 - g.ds_2
	g['m3'] = g.ds_1 - g.ds_3
	m1 = g.deal_score.max()
	m2 = g.m2.max()
	m3 = g.m3.max()
	print(g)
	g.to_csv('./data/tmp.csv', index=False)
	g['id'] = g.collection+'.'+g.token_id.astype(str)
	t['id'] = t.collection+'.'+t.token_id_x.astype(str)
	a = list(g[ (g.m2 >= 8) | (g.m3 >= 15) ].groupby('collection').head(1).id.unique())
	b = list(g[ (g.deal_score >= 90) ].id.unique())
	c = list(t.id.unique())

	# collections = t[ (t.pct >= 1.15) | (t.dff >= 1) ].collection.unique()
	collections = t.id.unique()
	to_alert = list(set(a + b + c))
	to_alert = [ x for x in to_alert if not x in alerted ]
	alerted += to_alert

	s = '@here\n' if len(to_alert) else ''
	if len(collections):
		s += ', '.join(collections)
		s += 'are listings far below floor\n'
	s += '```'
	for c in pred_price.collection.unique():
		s += '{} floor: {}\n'.format(c, round(pred_price[pred_price.collection==c].floor.min(), 1))
	# s += '```'
	g = g.sort_values(['collection','deal_score'], ascending=[1,0])
	for row in g.iterrows():
		row = row[1]
		txt = '{} | {} | ${} | {}'.format( str(row['collection']).ljust(10), str(row['token_id']).ljust(5), str(round(row['price'])).ljust(3), round(row['deal_score']) )
		s += '{}\n'.format(txt)
	s += '```'
	print(s)

	mUrl = 'https://discord.com/api/webhooks/976332557996150826/8KZqD0ov5OSj1w4PjjLWJtmgnCM9bPWaCkZUUEDMeC27Z0iqiA-ZU5U__rYU9tQI_ijA'

	data = {"content": s}
	response = requests.post(mUrl, json=data)

	return alerted

def scrape_randomearth(browser):
	print('Querying randomearth.io sales...')
	d_address = {
		'Galactic Angels': 'terra13nccm82km0ttah37hkygnvz67hnvkdass24yzv',
		'Galactic Punks': 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k',
		'LunaBulls': 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2',
		'Levana Dragon Eggs': 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg',
	}
	data = []
	# for collection in [ 'Levana Dragon Eggs' ]:
	scraper = cloudscraper.create_scraper()
	for collection in d_address.keys():
		print(collection)
		page = 0
		has_more = True
		while has_more:
			page += 1
			print('Page #{} ({})'.format(page, len(data)))
			url = 'https://randomearth.io/api/items?collection_addr={}&sort=price.asc&page={}&on_sale=1'.format( d_address[collection], page)
			# r = requests.get(url)
			# browser.get(url)
			# soup = BeautifulSoup(browser.page_source)
			# j = json.loads(soup.text)
			t = scraper.get(url).text
			j = json.loads(t)
			has_more = 'items' in j.keys() and len(j['items'])
			if has_more:
				for i in j['items']:
					data += [[ 'Terra', collection, i['token_id'], i['price'] / (10 ** 6) ]]
		df = pd.DataFrame(data, columns=['chain','collection','token_id','price'])
		df = clean_token_id(df)
		# df.to_csv('~/Downloads/tmp.csv', index=False)
		old = pd.read_csv('./data/listings.csv')
		old = old[-old.collection.isin(df.collection.unique())]
		old = old.append(df)
		print(old.groupby('collection').token_id.count())
		old.to_csv('./data/listings.csv', index=False)

def awards():
	dailysales = pd.read_csv('~/Downloads/dailysales.csv')
	pred_price = pd.read_csv('./data/pred_price.csv')[['collection','token_id','pred_price','pred_sd']]
	pred_price['collection'] = pred_price.collection.apply(lambda x: clean_name(x))
	dailysales['collection'] = dailysales.collection.apply(lambda x: clean_name(x))

	coefsdf = pd.read_csv('./data/coefsdf.csv')
	coefsdf['collection'] = coefsdf.collection.apply(lambda x: clean_name(x))
	coefsdf['tot'] = coefsdf.lin_coef + coefsdf.log_coef
	coefsdf['lin_coef'] = coefsdf.lin_coef / coefsdf.tot
	coefsdf['log_coef'] = coefsdf.log_coef / coefsdf.tot
	pred_price = pred_price.merge(coefsdf)
	floors = {
		'Aurory': 17,
		'Thugbirdz': 27,
		'Solana Monkey Business': 98,
		'Degen Apes': 29,
		'Pesky Penguins': 3.65,
	}
	pred_price = pred_price[pred_price.collection.isin(floors.keys())]
	pred_price['floor'] = pred_price.collection.apply(lambda x: floors[x] )

	# metadata = pd.read_csv('./data/metadata.csv')
	# solana_blob = metadata[ (metadata.collection == 'aurory') & (metadata.feature_name == 'skin') & (metadata.feature_value == 'Solana Blob (9.72%)')].token_id.unique()
	# pred_price['pred_price'] = pred_price.apply(lambda x: (x['pred_price'] * 0.8) - 8 if x['token_id'] in solana_blob and x['collection'] == 'Aurory' else x['pred_price'], 1 )

	pred_price['abs_chg'] = (pred_price.floor - pred_price.floor_price) * pred_price.lin_coef
	pred_price['pct_chg'] = (pred_price.floor - pred_price.floor_price) * pred_price.log_coef
	pred_price['pred_price_0'] = pred_price.pred_price
	pred_price['pred_price'] = pred_price.apply( lambda x: x['pred_price'] + x['abs_chg'] + ( x['pct_chg'] * x['pred_price'] / x['floor_price'] ), 1 )
	pred_price['pred_price'] = pred_price.apply( lambda x: max( x['pred_price'], x['floor']), 1 )
	# pred_price['deal_score'] = pred_price.apply( lambda x: (( x['pred_price'] - x['price'] ) * 50 / ( 3 * x['pred_sd'])) + 50  , 1 )
	# pred_price['deal_score'] = pred_price.deal_score.apply( lambda x: min(max(0, x), 100) )
	dailysales = dailysales.merge(pred_price, how='left')
	dailysales['deal_score'] = dailysales.apply( lambda x: 100 * (1 - norm.cdf( x['price'], x['pred_price'], 2 * x['pred_sd'] * x['pred_price'] / x['pred_price_0'] )) , 1 )
	dailysales.sort_values('deal_score', ascending=0).head(100)[['collection','token_id','price','pred_price','deal_score']].to_csv('~/Downloads/tmp2.csv', index=False)

def scrape_recent_smb_sales(browser):
	print('Scraping recent SMB sales...')
	o_sales = pd.read_csv('./data/sales.csv').rename(columns={'block_timestamp':'sale_date'})
	o_sales.head()
	# o_sales['tmp'] = o_sales.sale_date.apply(lambda x: str(x)[:10] )
	# o_sales.groupby('tmp').token_id.count().tail(20)
	data = []
	url = 'https://market.solanamonkey.business/'
	browser.get(url)
	sleep(4)
	browser.find_elements_by_class_name('inline-flex')[-1].click()
	sleep(4)
	es = browser.find_elements_by_class_name('inline-flex')
	# for i in range(len(es)):
	# 	print(i, es[i].text)
	browser.find_elements_by_class_name('inline-flex')[4].click()
	sleep(4)
	soup = BeautifulSoup(browser.page_source)
	for tr in soup.find_all('table', class_='min-w-full')[0].find_all('tbody')[0].find_all('tr'):
		td = tr.find_all('td')
		token_id = int(re.split('#', td[0].text)[1])
		price = float(td[1].text)
		t = td[3].text
		num = 1 if 'an ' in t or 'a ' in t else int(re.split(' ', t)[0])
		hours_ago = 0 if 'minute' in t else num if 'hour' in t else 24 * num
		sale_date = datetime.today() - timedelta(hours=hours_ago)
		data += [[ 'Solana Monkey Business', token_id, price, sale_date  ]]
	new_sales = pd.DataFrame(data, columns=['collection','token_id','price','sale_date'])
	new_sales['chain'] = 'Solana'
	print('{} new sales'.format(len(new_sales)))
	smb = o_sales[o_sales.collection.isin(['smb','Solana Monkey Business'])]
	o_sales = o_sales[-(o_sales.collection.isin(['smb','Solana Monkey Business']))]
	l0 = len(smb)
	smb = smb.append(new_sales)
	smb['tmp'] = smb.sale_date.apply(lambda x: str(x)[:10] )
	smb = smb.drop_duplicates(subset=['token_id','price','tmp'], keep='last')
	l1 = len(smb)
	print('SMB sales {} -> {} (added {}, max date {})'.format(l0, l1, l1 - l0, new_sales.sale_date.max()))
	# smb[(smb.tmp >= '2021-12-13') & (-smb.token_id.isin(list(new_sales.token_id.unique())))]
	# smb['messed'] = smb.sale_date.apply(lambda x: str(x)[14:22] == '02:36.63' )
	# smb['sale_date'] = smb.sale_date.apply(lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S') if len(str(x)) == 19 else datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S.%f') )
	# mn = smb[smb.messed].sale_date.min()
	# def f(x):
	# 	if x['messed'] == True:
	# 		d = x['sale_date'] - mn
	# 		return(mn - d)
	# 	return(x['sale_date'])
	# smb['sale_date'] = smb.apply(lambda x: f(x), 1 )
	# smb['tmp'] = smb.sale_date.apply(lambda x: str(x)[:10] )
	# len(smb[smb.messed])
	# print(smb.groupby('tmp').token_id.count().reset_index())
	# smb.groupby('tmp').token_id.count().reset_index().to_csv('~/Downloads/tmp.csv', index=False)
	# print(smb[smb.messed==False].groupby('tmp').token_id.count().tail(20))
	# len(smb[(smb.tmp >= '2021-12-12') & (-smb.token_id.isin(list(new_sales.token_id.unique())))])
	del smb['tmp']
	# del smb['sales_date']
	# browser.find_elements_by_class_name('css-1wy0on6')[-1].click()
	# browser.find_element_by_id('react-select-4-option-1').click()
	o_sales = o_sales.append(smb)
	o_sales['chain'] = o_sales.collection.apply(lambda x: 'Terra' if x in [ 'Galactic Punks', 'Levana Dragon Eggs', 'LunaBulls' ] else 'Solana' )
	print(o_sales.groupby(['chain','collection']).token_id.count())
	o_sales.to_csv('./data/sales.csv', index=False)

def scrape_magic_eden():
	collection = 'boryokudragonz'
	listings = pd.DataFrame()
	url = 'https://magiceden.io/marketplace/boryoku_dragonz'
	for i in range(5):
		s = i * 20
		url = 'https://api-mainnet.magiceden.io/rpc/getListedNFTsByQuery?q={%22$match%22:{%22collectionSymbol%22:%22boryoku_dragonz%22},%22$sort%22:{%22takerAmount%22:1,%22createdAt%22:-1},%22$skip%22:'+str(s)+',%22$limit%22:20}'
		r = requests.get(url)
		j = r.json()
		if len(j['results']):
			cur = pd.DataFrame(j['results'])[[ 'price','title' ]]
			cur['token_id'] = cur.title.apply(lambda x: int(re.split('#', x)[1]) )
			del cur['title']
			cur['collection'] = collection
			listings = listings.append(cur)
	return(listings)

def scrape_recent_sales():
	print('Scraping recent solanart sales...')
	o_sales = pd.read_csv('./data/sales.csv')
	o_sales['collection'] = o_sales.collection.apply(lambda x: 'degenapes' if x == 'degenape' else x )
	o_sales.groupby('collection').sale_date.max()
	sales = pd.DataFrame()
	collections = [ 'thugbirdz','degenape']
	collections = [ 'aurory','thugbirdz','degenape']
	collection = 'peskypenguinclub'
	for collection in collections:
		url = 'https://qzlsklfacc.medianetwork.cloud/all_sold_per_collection_day?collection={}'.format(collection)
		try:
			j = requests.get(url).json()
		except:
			sleep(2)
			j = requests.get(url).json()
		cur = pd.DataFrame(j)
		cur['collection'] = 'degenapes' if collection == 'degenape' else collection
		cur['sale_date'] = cur.date.apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.000Z'))
		cur['token_id'] = cur.name.apply(lambda x: int(re.split('#', x)[1]) if '#' in x else -1 )
		cur = cur[cur.token_id >= 0]
		sales = sales.append( cur[['collection','token_id','price','sale_date']] )
	l0 = len(o_sales)
	print(sales.groupby('collection').token_id.count())
	sales['tmp'] = sales.sale_date.apply(lambda x: str(x)[:5] )
	sales['chain'] = 'Solana'
	o_sales['chain'] = o_sales['chain'].fillna('Solana')
	o_sales[['chain','collection']].drop_duplicates()
	o_sales['tmp'] = o_sales.sale_date.apply(lambda x: str(x)[:5] )
	o_sales = o_sales.append(sales)
	o_sales['token_id'] = o_sales.token_id.apply(lambda x: str(x) )
	o_sales['collection'] = o_sales.collection.apply(lambda x: clean_name(x) )
	o_sales = o_sales.drop_duplicates(subset=['collection','token_id','tmp','price'])
	o_sales[o_sales.token_id=='1606']
	o_sales = o_sales.drop_duplicates(subset=['collection','token_id','price'])
	o_sales = o_sales.drop_duplicates(subset=['collection','token_id','tmp','price'])
	l1 = len(o_sales)
	print('{} -> {} (added {})'.format(l0, l1, l1 - l0))
	o_sales.groupby('collection').tmp.max()
	print(o_sales.groupby('collection').token_id.count())
	del o_sales['tmp']
	o_sales.to_csv('./data/sales.csv', index=False)

def scrape_solanafloor():
	browser.get('https://solanafloor.com/')
	soup = BeautifulSoup(browser.page_source)
	d0 = soup.find_all('div', class_='ag-pinned-left-cols-container')
	d1 = soup.find_all('div', class_='ag-center-cols-clipper')
	len(d0)
	len(d1)
	d0 = d0[1]
	d1 = d1[1]
	rows0 = d0.find_all('div', class_='ag-row')
	rows1 = d1.find_all('div', class_='ag-row')
	data = []
	for r in rows1:
		cell1 = r.find_all('div', class_='ag-cell')
		a = cell1[0].find_all('a')[0]
		project = re.split('/', a.attrs['href'])[-1]
		data += [[ project, int('Lite' in cell1[0].text) ]]
	df = pd.DataFrame(data, columns=['project','is_lite'])
	df.to_csv('./data/sf_projects.csv', index=False)

def scrape_opensea_listings(browser, collections=['BAYC','MAYC']):
	data = []
	opensea_d = {
		'BAYC': 'boredapeyachtclub'
		, 'MAYC': 'mutant-ape-yacht-club'
	}
	collection = 'MAYC'
	for collection in collections:
		c = opensea_d[collection]
		url = 'https://opensea.io/collection/{}'.format(c)
		browser.get(url)
		browser.find_elements_by_class_name('Buttonreact__StyledButton-sc-glfma3-0')[-1].click()
		seen = []
		sleep(1)

		it = 0
		counter = 0
		while True:
			it += 1
			prv = len(seen)
			soup = BeautifulSoup(browser.page_source)
			ars = soup.find_all('article', class_='AssetSearchList--asset')
			print(it, len(ars), len(seen))
			for a in ars:
				if not 'Price' in a.text:
					continue
				try:
					token_id = int(a.find_all('div', class_='AssetCardFooter--name')[0].text)
					price = float(a.find_all('div', class_='Price--amount')[0].text)
				except:
					sleep(2)
				token_id = int(a.find_all('div', class_='AssetCardFooter--name')[0].text)
				price = float(a.find_all('div', class_='AssetCardFooter--price')[0].find_all('div', class_='AssetCardFooter--price-amount')[0].text)
				if not token_id in seen:
					data += [[ collection, token_id, price ]]
					seen.append(token_id)

			# browser.execute_script("window.scrollTo(0, document.body.scrollHeight - 600);")
			browser.execute_script("window.scrollTo(0, {});".format(300+(it * 300)))
			# scroll = browser.find_elements_by_class_name('AssetSearchList--asset')
			# scroll = browser.find_elements_by_class_name('Asset--loaded')
			# j = len(scroll) - 1
			# # j = 0
			# try:
			# 	browser.execute_script("arguments[0].scrollIntoView();", scroll[-1] )
			# except:
			# 	sleep(1)
			# 	try:
			# 		browser.execute_script("arguments[0].scrollIntoView();", scroll[j] )
			# 	except:
			# 		sleep(10)
			# 		browser.execute_script("arguments[0].scrollIntoView();", scroll[j] )
			if len(seen) == prv:
				counter += 1
				if counter >= 5:
					break
			else:
				counter = 0
			prv = len(seen)
			sleep(1)
	df = pd.DataFrame(data, columns=['collection','token_id','price']).drop_duplicates()
	df['chain'] = 'Ethereum'
	old = pd.read_csv('./data/listings.csv')
	old = old[-old.collection.isin(df.collection.unique())]
	old = old.append(df)
	old.collection.unique()
	old.groupby('collection').token_id.count()
	old.to_csv('./data/listings.csv', index=False)

def scrape_listings(browser, collections = [ 'okay-bears','catalina-whale-mixer','meerkat-millionaires-cc','solgods','cets-on-creck','stoned-ape-crew','degods','aurory','thugbirdz','smb','degenapes','peskypenguinclub' ], alerted = [], is_listings = True):
	print('Scraping solanafloor listings...')
	data = []
	m_data = []
	m_data_2 = []
	# collections = [ 'aurory','thugbirdz','meerkatmillionaires','aurory','degenapes' ]
	# collections = [ 'aurory','thugbirdz','smb','degenapes' ]
	# collections = [ 'smb' ]
	d = {
		'smb': 'solana-monkey-business'
		, 'degenapes': 'degen-ape-academy'
		, 'peskypenguinclub': 'pesky-penguins'
	}
	# collections = ['the-suites']
	# sf_projects = pd.read_csv('./data/sf_projects.csv')
	# old = pd.read_csv('./data/solana_rarities.csv')
	# collections = sf_projects[(sf_projects.to_scrape==1) & (sf_projects.is_lite==0) & (-sf_projects.collection.isin(old.collection.unique()))].collection.unique()
	collection = 'portals'
	collection = 'degods'
	collection = 'defi-pirates'
	collection = 'cets-on-creck'
	# collections = ['astrals']
	# collections = ['defi-pirates']
	# collections = ['cets-on-creck','astrals']
	# collections = ['cets-on-creck']
	# collections = ['meerkat-millionaires-cc']
	for collection in collections:
		if collection == 'boryokudragonz':
			continue
		c = d[collection] if collection in d.keys() else collection
		u = 'listed' if is_listings else 'all-tokens'
		url = 'https://solanafloor.com/nft/{}/{}'.format(c, u)
		browser.get(url)
		sleep(5)
		has_more = True
		page = 1
		seen = []
		while has_more and page < 120:
			# scroll = browser.find_element_by_class_name('ag-center-cols-viewport')
			print('{} page #{} ({})'.format(collection, page, len(data)))
			sleep(3)
			page += 1
			for j in [20, 30, 30, 30, 30, 30, 30, 30] * 1:
				for _ in range(1):
					pass
					soup = BeautifulSoup(browser.page_source)
					# for row in browser.find_elements_by_class_name('ag-row'):
					# 	cells = row.find_elements_by_class_name('ag-cell')
					# 	if len(cells) > 3:
					# 		token_id = int(cells[0].text)
					# 		price = float(cells[4].text)
					# 		data += [[ collection, token_id, price ]]
					d0 = soup.find_all('div', class_='ag-pinned-left-cols-container')
					d1 = soup.find_all('div', class_='ag-center-cols-clipper')
					h1 = soup.find_all('div', class_='ag-header-row')
					if not len(d0) or not len(d1):
						continue
					d0 = d0[0]
					d1 = d1[0]
					h1 = h1[1]
					rows0 = d0.find_all('div', class_='ag-row')
					rows1 = d1.find_all('div', class_='ag-row')
					hs1 = h1.find_all('div', class_='ag-header-cell')
					hs1 = [ x.text.strip() for x in hs1 ]
					for k in range(len(rows0)):
						# for row in soup.find_all('div', class_='ag-row'):
						# 	# print(row.text)
						cell0 = rows0[k].find_all('div', class_='ag-cell')
						cell1 = rows1[k].find_all('div', class_='ag-cell')
						if len(cell1) > 2:
							token_id = cell0[0].text
							mint_address = re.split('/', cell0[0].find_all('a')[0].attrs['href'])[-1] if len(cell0[0].find_all('a')) else None
							price = cell1[2 if is_listings else 0].text
							if len(token_id) and len(price):
								try:
									img = cell0[1].find_all('a')[0].find_all('img')[0].attrs['src']
								except:
									sleep(3)
									try:
										img = cell0[1].find_all('a')[0].find_all('img')[0].attrs['src']
									except:
										img = ''
								# token_id = int(token_id[0].text)
								# price = float(price[0].text)
								if not token_id.strip():
									continue
								token_id = 0 if token_id.strip() =='' else int(token_id)
								price = float(price) if price.strip() else 0
								if not price and is_listings:
									continue
								if not token_id in seen:
									if not is_listings:
										data += [[ collection, token_id, img, mint_address, price ]]
										# for i in range(len(cell1)):
										# 	print(i, hs1[i], cell1[i].text.strip())
										m = {
											'collection': collection
											, 'token_id': token_id
											, 'mint_address': mint_address
										}
										has_listing = len(cell1) > 11 and '$' in cell1[10].text and cell1[11].text.strip() == 'Buy'
										m_data_2 += [ [collection, token_id, mint_address] + [ x.text.strip() for x in cell1 ]]
										for l in range(len(hs1)):
											# print(hs1[l], cell1[l].text.strip())
											m = l if has_listing or l == 0 else l - 5 if l > 5 else l + 5
											# print(hs1[l], cell1[m].text.strip())
											v = cell1[l].text.strip() if has_listing else cell1[l].text.strip()
											m_data += [[ collection, token_id, mint_address, hs1[l], cell1[l].text.strip(), len(cell1) ]]
									else:
										data += [[ collection, token_id, price ]]
									seen.append(token_id)
							# else:
							# 	print(row.text)
					scroll = browser.find_elements_by_class_name('ag-row-even')
					j = min(j, len(scroll) - 1)
					try:
						browser.execute_script("arguments[0].scrollIntoView();", scroll[j] )
					except:
						sleep(1)
						try:
							browser.execute_script("arguments[0].scrollIntoView();", scroll[j] )
						except:
							sleep(10)
							browser.execute_script("arguments[0].scrollIntoView();", scroll[j] )

					sleep(.1)
			next = browser.find_elements_by_class_name('ag-icon-next')
			a = browser.find_element_by_id('ag-17-start-page-number').text
			b = browser.find_element_by_id('ag-17-of-page-number').text
			if len(next) and a != b:
				next[0].click()
			else:
				has_more = False
				break
		if not is_listings:
			rem = []
			# data = [['defi-pirates', 0, '2qnp1qNd7bLcFdCn82GdrcNy61jV45vFWu9yxk4psq8o', 9863.0]]
			old = pd.read_csv('./data/solana_rarities.csv')
			# tmp = list(old[old.collection == 'defi-pirates'].token_id.unique())
			# [x for x in range(0, 10001) if not x in tmp]
			rarities = pd.DataFrame(data, columns=['collection','token_id','image_url','mint_address','nft_rank'])
			rarities = rarities.append(old).drop_duplicates(subset=['collection','token_id'], keep = 'first')
			rarities[rarities.collection == 'cets-on-creck']
			rarities = rarities[-rarities.collection.isin(rem)]
			print(rarities.groupby('collection').token_id.count().reset_index().sort_values('collection'))
			print(rarities.groupby('collection').head(1))
			rarities.to_csv('./data/solana_rarities.csv', index=False)

			old = pd.read_csv('./data/sf_metadata.csv')
			# metadata = pd.DataFrame(m_data, columns=['collection','token_id','mint_address','feature_name','feature_value']).drop_duplicates()
			metadata = pd.DataFrame(m_data_2, columns=['collection','token_id','mint_address'] + [ 'col_{}'.format(i) for i in range(len(m_data_2[-1]) - 3) ] ).drop_duplicates()
			metadata = metadata.sort_values(['collection','token_id'])
			hs = [ 'nft_rank', 'Background', 'Skin', 'Clothing', 'Mouth', 'Eye', 'Headgear' ]
			hs = [ 'nft_rank', 'Background', 'Skin', 'Clothing', 'Mouth', 'Eye', 'Headgear', 'Hands' ]
			mdf = pd.DataFrame()
			for i in range(len(hs)):
				cur = metadata[[ 'collection','token_id','mint_address','col_{}'.format(i) ]].rename(columns={'col_{}'.format(i):'feature_value'})
				cur['feature_name'] = hs[i]
				mdf = mdf.append( cur )
			rem = mdf[mdf.feature_value == ''].token_id.unique()
			mdf = mdf[-mdf.token_id.isin(rem)]
			mdf[mdf.token_id == 6362].head(20)
			mdf['tmp'] = mdf.feature_value.apply(lambda x: len(x))
			mdf.sort_values('tmp', ascending=0).head()
			mdf = mdf[mdf.tmp < 40]
			del mdf['tmp']

			# g = mdf.groupby(['feature_name','feature_value']).token_id.count().reset_index()
			# g.to_csv('~/Downloads/tmp.csv', index=False)
			# metadata.head()
			# metadata.col_1.unique()
			# metadata[metadata.col_1 == '']
			# metadata = pd.DataFrame(m_data, columns=['collection','token_id','mint_address','feature_name','feature_value','n']).drop_duplicates()
			# metadata[metadata.feature_name == 'Eye'].feature_value.unique()
			# del metadata['n']
			# mdf[mdf.token_id == 2988]
			# metadata[metadata.token_id == 5641].head(10)
			# metadata[metadata.token_id == 1]
			# metadata['feature_name'] = metadata.feature_name.apply(lambda x: 'nft_rank' if x == 'Rank *' else x )
			# metadata = metadata[ -metadata.feature_name.isin(['Rank *','Owner','Listed On','Price','USD','Buy Link']) ]
			# metadata = metadata.drop_duplicates(subset=['collection','token_id','feature_name'], keep='first')
			# metadata = metadata.append(old).drop_duplicates(subset=['collection','token_id','feature_name'])
			metadata = mdf.append(old).drop_duplicates(subset=['collection','token_id','feature_name'], keep='first')
			metadata[metadata.token_id == 5641].head(10)

			g = metadata[[ 'collection','token_id' ]].drop_duplicates().groupby('collection').token_id.count().reset_index().sort_values('token_id')
			rem = g[g.token_id<99].collection.unique()
			metadata = metadata[-metadata.collection.isin(rem)]
			print(g)
			print('Adding {} rows'.format(len(metadata) - len(old)))
			# g.to_csv('~/Downloads')
			metadata.to_csv('./data/sf_metadata.csv', index=False)

	old = pd.read_csv('./data/listings.csv')
	listings = pd.DataFrame(data, columns=['collection','token_id','price']).drop_duplicates()
	listings.groupby('collection').price.min()
	# others = scrape_magic_eden()
	# listings = listings.append(others).drop_duplicates()
	# d = {
	# 	'aurory': 'Aurory'
	# 	,'thugbirdz': 'Thugbirdz'
	# 	,'smb': 'Solana Monkey Business'
	# 	,'degenapes': 'Degen Apes'
	# 	,'peskypenguinclub': 'Pesky Penguins'
	# 	,'meerkatmillionaires': 'Meerkat Millionaires'
	# 	,'boryokudragonz': 'Boryoku Dragonz'
	# 	,'degods': 'DeGods'
	# }
	listings['collection'] = listings.collection.apply(lambda x: clean_name(x))
	listings.groupby('collection').price.min()
	listings.groupby('collection').price.count()
	listings[listings.token_id=='1656']
	listings[listings.token_id==484]

	old = old[ -(old.collection.isin(listings.collection.unique())) ]
	old = old[old.collection != 'Meerkat Millionaires Cc']
	pred_price = pd.read_csv('./data/pred_price.csv')
	listings.token_id.values[:3]
	pred_price.token_id.values[:3]
	listings['token_id'] = listings.token_id.astype(str)
	pred_price['token_id'] = pred_price.token_id.astype(str)
	df = listings.merge(pred_price[['collection','token_id','pred_price']])
	df['ratio'] = df.pred_price / df.price
	print(df[df.collection == 'smb'].sort_values('ratio', ascending=0).head())
	listings[ listings.collection == 'smb' ].head()
	listings = listings.append(old)
	print(listings.groupby('collection').token_id.count())
	listings.to_csv('./data/listings.csv', index=False)
	return

	listings = listings.sort_values('price')
	t1 = listings.groupby('collection').head(1).rename(columns={'price':'t1'})
	t2 = listings.groupby('collection').head(2).groupby('collection').tail(1).rename(columns={'price':'t2'})
	t = t1.merge(t2, on=['collection'])
	t['pct'] = t.t2 / t.t1
	t['dff'] = t.t2 - t.t1
	t = t[ (t.pct >= 1.15) | (t.dff >= 10 ) ]

	pred_price = pd.read_csv('./data/pred_price.csv')[['collection','token_id','pred_price','pred_sd']]
	pred_price['collection'] = pred_price.collection.apply(lambda x: clean_name(x))
	pred_price['token_id'] = pred_price.token_id.astype(str)
	pred_price = pred_price.merge(listings)

	coefsdf = pd.read_csv('./data/coefsdf.csv')
	coefsdf['collection'] = coefsdf.collection.apply(lambda x: clean_name(x))
	coefsdf['tot'] = coefsdf.lin_coef + coefsdf.log_coef
	coefsdf['lin_coef'] = coefsdf.lin_coef / coefsdf.tot
	coefsdf['log_coef'] = coefsdf.log_coef / coefsdf.tot
	pred_price = pred_price.merge(coefsdf)
	floor = listings.groupby('collection').price.min().reset_index().rename(columns={'price':'floor'})
	pred_price = pred_price.merge(floor)

	# metadata = pd.read_csv('./data/metadata.csv')
	# solana_blob = metadata[ (metadata.collection == 'aurory') & (metadata.feature_name == 'skin') & (metadata.feature_value == 'Solana Blob (9.72%)')].token_id.unique()
	# pred_price['pred_price'] = pred_price.apply(lambda x: (x['pred_price'] * 0.8) - 4 if x['token_id'] in solana_blob and x['collection'] == 'Aurory' else x['pred_price'], 1 )

	# solana_blob = metadata[ (metadata.collection == 'aurory') & (metadata.feature_name == 'hair') & (metadata.feature_value == 'Long Blob Hair (9.72%)')].token_id.unique()
	# pred_price['pred_price'] = pred_price.apply(lambda x: (x['pred_price'] * 0.8) - 2 if x['token_id'] in solana_blob and x['collection'] == 'Aurory' else x['pred_price'], 1 )

	pred_price['abs_chg'] = (pred_price.floor - pred_price.floor_price) * pred_price.lin_coef
	pred_price['pct_chg'] = (pred_price.floor - pred_price.floor_price) * pred_price.log_coef
	pred_price['pred_price_0'] = pred_price.pred_price
	pred_price['pred_price'] = pred_price.apply( lambda x: x['pred_price'] + x['abs_chg'] + ( x['pct_chg'] * x['pred_price'] / x['floor_price'] ), 1 )
	pred_price['pred_price'] = pred_price.apply( lambda x: max( x['pred_price'], x['floor']), 1 )
	# pred_price['deal_score'] = pred_price.apply( lambda x: (( x['pred_price'] - x['price'] ) * 50 / ( 3 * x['pred_sd'])) + 50  , 1 )
	# pred_price['deal_score'] = pred_price.deal_score.apply( lambda x: min(max(0, x), 100) )
	pred_price['deal_score'] = pred_price.apply( lambda x: 100 * (1 - norm.cdf( x['price'], x['pred_price'], 2 * x['pred_sd'] * x['pred_price'] / x['pred_price_0'] )) , 1 )

	pred_price = pred_price.sort_values(['deal_score'], ascending=[0])
	pred_price[pred_price.token_id=='1656']
	g = pred_price.groupby('collection').head(4)[['collection','token_id','deal_score','price']]
	n1 = g.groupby('collection').head(2).groupby('collection').head(1)[['collection','deal_score']].rename(columns={'deal_score':'ds_1'})
	n2 = g.groupby('collection').head(2).groupby('collection').tail(1)[['collection','deal_score']].rename(columns={'deal_score':'ds_2'})
	n3 = g.groupby('collection').head(3).groupby('collection').tail(1)[['collection','deal_score']].rename(columns={'deal_score':'ds_3'})
	g = g.merge(n1)
	g = g.merge(n2)
	g = g.merge(n3)
	g['m2'] = g.ds_1 - g.ds_2
	g['m3'] = g.ds_1 - g.ds_3
	m1 = g.deal_score.max()
	m2 = g.m2.max()
	m3 = g.m3.max()
	print(g)
	g.to_csv('./data/tmp.csv', index=False)
	g['id'] = g.collection+'.'+g.token_id.astype(str)
	t['id'] = t.collection+'.'+t.token_id_x.astype(str)
	a = list(g[ (g.m2 >= 8) | (g.m3 >= 15) ].groupby('collection').head(1).id.unique())
	b = list(g[ (g.deal_score >= 90) ].id.unique())
	c = list(t.id.unique())

	# collections = t[ (t.pct >= 1.15) | (t.dff >= 1) ].collection.unique()
	collections = t.id.unique()
	to_alert = list(set(a + b + c))
	to_alert = [ x for x in to_alert if not x in alerted ]
	alerted += to_alert

	s = '@here\n' if len(to_alert) else ''
	if len(collections):
		s += ', '.join(collections)
		s += 'are listings far below floor\n'
	s += '```'
	g = g.sort_values(['collection','deal_score'], ascending=[1,0])
	for row in g.iterrows():
		row = row[1]
		txt = '{} | {} | ${} | {}'.format( str(row['collection']).ljust(10), str(row['token_id']).ljust(5), str(round(row['price'])).ljust(3), round(row['deal_score']) )
		s += '{}\n'.format(txt)
	s += '```'
	print(s)

	# mUrl = 'https://discord.com/api/webhooks/916027651397914634/_zrDNThkwu2ZYB4F503JNRtwhPh9EJ642rIdENawlJu1Di0dpfKT_ba045xXGCefAFvI'

	# data = {"content": s}
	# response = requests.post(mUrl, json=data)

	return alerted

def scrape_solanafloor(browser):
	data = []
	collections = [ 'aurory','thugbirdz','smb','degenapes','peskypenguinclub' ]
	collections = [ 'peskypenguinclub' ]
	d = {
		'smb': 'solana-monkey-business'
		, 'degenapes': 'degen-ape-academy'
		, 'peskypenguinclub': 'pesky-penguins'
	}
	for collection in collections:
		c = d[collection] if collection in d.keys() else collection
		url = 'https://solanafloor.com/nft/{}/all-tokens'.format(c)
		browser.get(url)
		sleep(5)
		has_more = True
		page = 1
		seen = []
		while has_more:
			# scroll = browser.find_element_by_class_name('ag-center-cols-viewport')
			print('{} page #{} ({})'.format(collection, page, len(data)))
			sleep(3)
			page += 1
			for j in [25, 30, 35, 30, 25] * 2:
				for _ in range(1):
					soup = BeautifulSoup(browser.page_source)
					# for row in browser.find_elements_by_class_name('ag-row'):
					# 	cells = row.find_elements_by_class_name('ag-cell')
					# 	if len(cells) > 3:
					# 		token_id = int(cells[0].text)
					# 		price = float(cells[4].text)
					# 		data += [[ collection, token_id, price ]]
					for row in soup.find_all('div', class_='ag-row'):
						# print(row.text)
						cell = row.find_all('div', class_='ag-cell')
						if len(cell) > 4:
							token_id = row.find_all('div', {'col-id':'tokenId'})
							image = row.find_all('div', {'col-id':'image'})
							if len(token_id) and len(image) and len(image[0].find_all('img')):
								image_url = image[0].find_all('img')[0].attrs['src']
								token_id = int(token_id[0].text)
								if not token_id in seen:
									data += [[ collection, token_id, image_url ]]
									seen.append(token_id)
						# else:
						# 	print(row.text)
					scroll = browser.find_elements_by_class_name('ag-row-even')
					j = min(j, len(scroll) - 1)
					browser.execute_script("arguments[0].scrollIntoView();", scroll[j] )
					sleep(.1)
			next = browser.find_elements_by_class_name('ag-icon-next')
			a = browser.find_element_by_id('ag-17-start-page-number').text
			b = browser.find_element_by_id('ag-17-of-page-number').text
			if len(next) and a != b:
				next[0].click()
			else:
				has_more = False
				break
	tokens = pd.DataFrame(data, columns=['collection','token_id','image_url']).drop_duplicates()
	len(tokens.token_id.unique())
	tokens[ tokens.collection == 'degenapes' ].sort_values('token_id')
	old = pd.read_csv('./data/tokens.csv')

	old[old.collection == 'degods']
	old['collection'] = old.collection.apply(lambda x: clean_name(x) )
	# old['image_url'] = old.apply(lambda x: 'https://metadata.degods.com/g/{}.png'.format(int(x['token_id']) - 1) if x['collection'] == 'DeGods' else x['image_url'], 1 )
	old.head()
	old[ old.collection == 'DeGods' ].head()
	old[ old.collection == 'LunaBulls' ].head()
	tokens = old.append(tokens).drop_duplicates()
	print(tokens.groupby('collection').token_id.count())
	tokens.to_csv('./data/tokens.csv', index=False)
	# old.to_csv('./data/tokens.csv', index=False)

def scrape_solana_explorer(browser):
	url = 'https://explorer.solana.com/address/9uBX3ASjxWvNBAD1xjbVaKA74mWGZys3RGSF7DdeDD3F/tokens'
	browser.get(url)
	# r = requests.get(url)
	# soup = BeautifulSoup(r.text)
	soup = BeautifulSoup(browser.page_source)
	soup.text
	len(soup.find_all('table', class_='card-table'))
	data = []
	for tr in soup.find_all('table', class_='card-table')[-1].find_all('tbody')[0].find_all('tr'):
		data += [[ tr.find_all('td')[1].find_all('a')[0].text ]]
	contract_address = pd.DataFrame(data, columns=['contract_address'])
	contract_address.to_csv('./data/contract_address.csv', index=False)

	txs = []
	ids = sorted(contract_address.contract_address.unique())
	for i in range(3476, len(ids)):
		if i % 50 == 0:
			print('#{}/{}'.format(i, len(ids)))
		id = ids[i]
		url = 'https://explorer.solana.com/address/{}'.format(id)
		browser.get(url)
		sleep(2)
		soup = BeautifulSoup(browser.page_source)
		try:
			id = soup.find_all('h2')[-1].text
			for tr in soup.find_all('table', class_='card-table')[-1].find_all('tbody')[0].find_all('tr'):
				try:
					tx = tr.find_all('td')[0].find_all('a')[0].attrs['href']
					txs += [[ id, tx ]]
				except:
					pass
		except:
			print('Error with #{}'.format(id))
			pass
	txdf = pd.DataFrame(txs, columns=['monke_id','tx_id'])
	txdf['monke_id'] = txdf.monke_id.apply(lambda x: re.split('#', x)[1] if '#' in x else None)
	txdf['tx_id'] = txdf.tx_id.apply(lambda x: re.split('/', x)[-1] if '/' in x else None)
	txdf = txdf[(txdf.monke_id.notnull()) & (txdf.tx_id.notnull())]
	txdf.to_csv('../data/tx.csv', index=False)

def scrape_tx(browser):
	txdf = pd.read_csv('../data/tx.csv')
	data = []
	t_data = []
	tx_ids = sorted(txdf.tx_id.unique())
	for i in range(0, len(tx_ids)):
		if i % 100 == 0:
			print('#{}/{}'.format(i, len(tx_ids)))
		tx_id = tx_ids[i]
		tx_id = '4eqXrk9ZvknDrtKJGH4JFgS94eG5nDqNcXHgPqFng27viiJeFzfD7S86RYkwZmpqYdY37GDLG6AAZZ6UdAEN9gf6'
		url = 'https://explorer.solana.com/tx/{}'.format(tx_id)
		browser.get(url)
		# r = requests.get(url)
		# soup = BeautifulSoup(r.text)
		sleep(2)
		soup = BeautifulSoup(browser.page_source)
		soup.text
		len(soup.find_all('table', class_='card-table'))
		datum = { 'tx_id': tx_id }
		for tr in soup.find_all('table', class_='card-table')[0].find_all('tbody')[0].find_all('tr'):
			td = tr.find_all('td')
			k = ' '.join(td[0].text.strip().split())
			v = ' '.join(td[1].text.strip().split())
			datum[k] = v
		data += [ datum ]
		for tr in soup.find_all('table', class_='card-table')[1].find_all('tbody')[0].find_all('tr'):
			td = tr.find_all('td')
			details = functools.reduce(lambda x, y: x+'.'+y, list(map(lambda x: x.text, list(td[4].find_all('span')))), '')
			t_data += [[ td[1].text, td[2].text, details ]]
		df = pd.DataFrame(data)
		t_df = pd.DataFrame(t_data, columns=['address','sol','details'])

def scrape_how_rare():
	o_metadata = pd.read_csv('./data/metadata.csv')
	o_metadata[ (o_metadata.collection == 'aurory') ].feature_name.unique()
	sorted(o_metadata[ (o_metadata.collection == 'aurory') & (o_metadata.feature_name == 'skin') ].feature_value.unique())
	ts = o_metadata[ (o_metadata.collection == 'aurory') & (o_metadata.feature_name == 'skin') & (o_metadata.feature_value == 'Solana Blob (9.72%)')].token_id.unique()
	ts = o_metadata[ (o_metadata.collection == 'smb') & (o_metadata.feature_name == 'type') & (o_metadata.feature_value == 'Skeleton (2.42%)')].token_id.unique()
	# ts = o_metadata[ (o_metadata.collection == 'smb') & (o_metadata.feature_name == 'type')].feature_value.unique()
	# ts = o_metadata[ (o_metadata.collection == 'smb') ].feature_name.unique()
	len(ts)

	token_data = []
	errors = []
	data = []
	s_data = []
	collections = {
		'smb': 5000,
		'aurory': 10000,
		'degenapes': 10000,
		'thugbirdz': 3333,
		'meerkatmillionaires': 10000,
		'peskypenguinclub': 8888,
		'boryokudragonz': 1111,
		'degods': 1111,
	}
	k = 'degenapes'
	v = collections[k]
	seen = o_metadata[ (o_metadata.collection == k) ].token_id.unique()
	opener = urllib.request.build_opener()
	opener.addheaders = [('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
	urllib.request.install_opener(opener)
	i = 1
	j = i
	# ts = [ x[1] for x in errors ]
	# errors = []
	# for i in range(11, 1112):
	# for i in ts:
	for i in range(j, v):
		if i in seen:
			continue
		t_0 = time.time()
		print('Token ID #{}: {}'.format(i, datetime.fromtimestamp(t_0)))
		print(i, len(data))
		# if i> 0 and i % 25 == 1:
		# 	print(i, len(data))
		# 	sleep(20)
		url = 'https://howrare.is/{}/{}/'.format(k, i)
		if k == 'thugbirdz':
			url = 'https://howrare.is/{}/{}/'.format(k, str(i).zfill(4))
		r = requests.get(url, cookies=cookies)
		r = s.get(url)
		soup = BeautifulSoup(r.text)
		atts = { 'token_id': i, 'collection': k }
		try:
			ul = soup.find_all('div', class_='attributes')[0]
		except:
			try:
				sleep(2)
				r = requests.get(url)
				soup = BeautifulSoup(r.text)
				ul = soup.find_all('div', class_='attributes')[0]
			except:
				try:
					sleep(10)
					r = requests.get(url)
					soup = BeautifulSoup(r.text)
					ul = soup.find_all('div', class_='attributes')[0]
				except:
					errors += [[ k, i ]]
					print('Error on ID #{}'.format(i))
					continue
		t_1 = time.time()
		print('Took {} seconds to load page'.format(round(t_1 - t_0, 1)))
		lis = ul.find_all('div', class_='attribute')
		if len(lis) == 0:
			print('No atts on ID #{}'.format(i))
		for li in lis:
			# try:
			if len(li.contents) >= 2:
				att = re.sub(' ', '_', li.contents[0].strip()).lower()
				# val = ' '.join(li.find_all('div')[0].text.strip().split())
				atts[att] = li.contents[1].text.strip()
			# except:
			# pass
		table = soup.find_all('div', class_='sales_history')
		img = soup.find_all('img')
		if len(img):
			src = img[0].attrs['src']
			token_data += [[ k, i, src ]]
			# urllib.request.urlretrieve(src, './viz/www/img/{}/{}.png'.format(k, i))
		# print('Took {} seconds to load image'.format(round(time.time() - t_1, 1)))
		data += [atts]
		if len(table):
			table = table[0]
			for tr in table.find_all('div', class_='all_coll_row')[1:]:
				td = tr.find_all('div', class_='all_coll_col')
				if len(td) > 4:
					tx_id = td[4].find_all('a')[0].attrs['href'] if len(td[4].find_all('a')) > 0 else None
					s_data += [[ k, i, td[0].text, re.sub('◎', '', td[1].text).strip(), td[2].find_all('a')[0].attrs['href'], td[3].find_all('a')[0].attrs['href'], tx_id ]]
		# except:
	df = pd.DataFrame(data)
	# seen = list(df.token_id.unique())
	s_df = pd.DataFrame(s_data, columns=['collection','token_id','sale_date','price','seller','buyer','tx_id'])
	s_df['price'] = s_df.price.apply(lambda x: re.sub('SOL', '', x).strip() ).astype(float)
	s_df['seller'] = s_df.seller.apply(lambda x: re.split('=', x)[-1] )
	s_df['buyer'] = s_df.buyer.apply(lambda x: re.split('=', x)[-1] )
	s_df['tx_id'] = s_df.tx_id.apply(lambda x: re.split('/', x)[-1] if x and x == x else None )
	s_df.sort_values('price').tail(30)
	# s_df[[ 'token_id','sale_date','price' ]].to_csv('~/Downloads/smb_skelly_sales.csv', index=False)

	def reshape_df(df):
		r_data = []
		for c in df.columns:
			if c in [ 'token_id','collection' ]:
				continue
			df['feature_name'] = c
			df['feature_value'] = df[c]
			r_data += list(df[[ 'token_id','collection', 'feature_name','feature_value' ]].values)
		r_df = pd.DataFrame(r_data, columns=['token_id','collection','feature_name','feature_value'])
		return(r_df)

	o_metadata = pd.read_csv('./data/metadata.csv')
	# o_metadata = reshape_df(o_metadata)
	df = reshape_df(df)
	m_df = o_metadata.append(df).drop_duplicates(subset=['token_id','collection','feature_name'])
	# print(m_df.groupby('collection').token_id.max())
	# print(m_df.groupby('collection').token_id.count())
	print(m_df[[ 'collection','token_id' ]].drop_duplicates().groupby('collection').token_id.count())
	m_df.to_csv('./data/metadata.csv', index=False)

	o_sales = pd.read_csv('./data/sales.csv').rename(columns={'block_timestamp':'sale_date'})[[ 'collection','token_id','sale_date','price' ]]
	print(o_sales.groupby('collection').sale_date.max())
	print(o_sales.groupby('collection').token_id.max())
	print(o_sales.groupby('collection').token_id.count())
	# o_sales = o_sales[o_sales.collection != 'aurory']
	s_df = o_sales.append(s_df).drop_duplicates()
	s_df = s_df.drop_duplicates()
	print(s_df.groupby('collection').token_id.count())
	print(o_sales.groupby('collection').token_id.count())
	s_df.to_csv('./data/sales.csv', index=False)
	# sales = pd.read_csv('./data/sales.csv')
	# sales[sales.collection == 'smb'][['token_id','sale_date','price']].to_csv('~/Downloads/historical_monke_sales.csv', index=False)

	tokens = pd.DataFrame(token_data, columns=['collection','token_id','image_url']).drop_duplicates()
	tokens['image_url'] = tokens.image_url.apply(lambda x: x[:-8] )
	tokens['image_url'].values[0]
	len(tokens.token_id.unique())
	tokens[ tokens.collection == 'degenapes' ].sort_values('token_id')
	old = pd.read_csv('./data/tokens.csv')
	old.image_url.unique()
	tokens = old.append(tokens).drop_duplicates(subset=['collection','token_id'], keep='last')
	print(tokens.groupby('collection').token_id.count())
	tokens.to_csv('./data/tokens.csv', index=False)

def save_img():
	i = 1
	for i in range(1, 5001):
		url = 'https://solanamonkey.business/.netlify/functions/fetchSMB?id={}'.format(i)
		src = requests.get(url).json()['nft']['image']
		src = 'https://arweave.net/{}'.format(src)
		urllib.request.urlretrieve(src, './viz/www/img/{}/{}.png'.format('smb', i))

def scrape_how_rare_is():
	d = {
		# 'degenapes': 40
		# ,'aurory': 40
		'shadowysupercoder': 40
		,'boryokudragonz': 5
		,'stonedapecrew': 17
		,'taiyorobotics': 9
		,'degods': 40
		,'nyanheroes': 45
		,'dazedducks': 40
	}
	data = []
	for collection, num_pages in d.items():
		print(collection)
		for page in range(num_pages):
			if len(data):
				print(data[-1])
			url = 'https://howrare.is/{}/?page={}&ids=&sort_by=rank'.format(collection, page)
			browser.get(url)
			sleep(0.1)
			soup = BeautifulSoup(browser.page_source)
			len(soup.find_all('div', class_='featured_item_img'))
			for div in soup.find_all('div', class_='featured_item'):
				image_url = div.find_all('img')[0].attrs['src']
				nft_rank = re.sub('rank', '', div.find_all('div', class_='item_stat')[0].text.strip())
				token_id = re.split('/', div.find_all('a')[0].attrs['href'])[-2]
				data += [[ collection, token_id, nft_rank, image_url ]]
	df = pd.DataFrame(data, columns=['collection','token_id','nft_rank','image_url'])
	df['collection'] = df.collection.apply(lambda x: clean_name(x) )
	df['clean_token_id'] = df.token_id
	df['chain'] = 'Solana'
	tokens = pd.read_csv('./data/tokens.csv')
	tokens.collection.unique()
	tokens = tokens[-tokens.collection.isin(df.collection.unique())]
	tokens = tokens.append(df)
	tokens.to_csv('./data/tokens.csv', index=False)

def scrape_howrare_token_id_mint_map():
	mints = pd.read_csv('./data/solana_mints.csv')
	collection = 'DeGods'
	s = requests.Session()
	s.get('https://magiceden.io/')
	r = s.get('https://httpbin.org/cookies')
	browser.manage().getCookies()
	cookie_list = browser.get_cookies()
	cookies = {}
	for c in cookie_list:
		cookies[c['name']] = c['value']


	for collection in [ 'DeGods' ]:
		it = 0
		for mint_address in sorted(mints[mints.collection == collection].mint_address.unique()):
			it += 1
			url = 'https://api-mainnet.magiceden.io/rpc/getNFTByMintAddress/{}'.format(mint_address)
			headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
			headers = {'authority': 'api-mainnet.magiceden.io',
			'cache-control': 'max-age=0',
			'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
			'sec-ch-ua-mobile': '?0',
			'sec-ch-ua-platform': '"macOS"',
			'upgrade-insecure-requests': '1',
			'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36',
			'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
			'sec-fetch-site': 'none',
			'sec-fetch-mode': 'navigate',
			'sec-fetch-user': '?1',
			'sec-fetch-dest': 'document',
			'accept-language': 'en-US,en;q=0.9',
			'cookie': 'ajs_anonymous_id=eba2f8f7-3519-4110-a1c2-4fbb1c350ae4; rl_page_init_referrer=RudderEncrypt%3AU2FsdGVkX19wSK5HkzEPJocowXJThl3gTmPeTtvkaH8%3D; rl_page_init_referring_domain=RudderEncrypt%3AU2FsdGVkX18tu9VUWzMjRFJLT3XFToyFsfZjjQBl4HM%3D; _clck=1xl6xwz|1|ez0|0; __cf_bm=BgwftpyDbLNKxpuLYvLE1ekRTn2MWF7KU5Suu3SRMlo-1644967874-0-ATSK7m2xYwOl+xF+SGhqbInjBZiL4ywBWBVroe1O22PBjZQtMzt8Pno6Q4panpaxXuaj7ys/wWkSRSAqGYih8PCDdzizesQyOA/U9HVidpqTXFRU/ckeufeFfxhesga4Sg==; rl_user_id=RudderEncrypt%3AU2FsdGVkX189KreVOKsVULLvR9eM%2FKE7IkW6qokXvIvedAi9vH71haADdBNKEtqdFIzVlBAeHTR411o5svVVeQ%3D%3D; rl_anonymous_id=RudderEncrypt%3AU2FsdGVkX184cKLxwy%2FrsyLEVzTyifhXO8CNRjMrODDx8Xe%2FukvGqi7V%2FMgmldWtnjIq1VLhmwusKYIZ%2FFAeSQ%3D%3D; rl_group_id=RudderEncrypt%3AU2FsdGVkX1%2FCcC9ITF5hwaucXM%2Bz4tvMxvI0YaIIGa0%3D; rl_trait=RudderEncrypt%3AU2FsdGVkX19PxxarpffnZFAUNe2tgmUmZC6txkvO2Z7ccbw4JrssvPZMZ9w2c1g3%2FUuHawkxfqu5d2FvhirmPn7f0dSP29A9UVJ3eHmXYBliJzuT8XW1B9prqWgRWWz3GpqGGJGXa%2BoqX1MQLCvltunTqQR%2Fxw2CKW%2BZDPsas89XXY0NlmixoT81TgLEPKddSjZDBihP09u9aM1YvCCxuw%3D%3D; rl_group_trait=RudderEncrypt%3AU2FsdGVkX1%2BUocb9WeinM5mlF6NiPC8ISK%2FYcQgmPYk%3D; _clsk=fewr53|1644967907370|3|0|e.clarity.ms/collect',
			'if-none-match': 'W/"564-Uw9zGORiLyOok/cpoNblXV6SsrI"'}
			r = s.get(url, headers=headers)
			browser.get(url)
			s = json.loads(BeautifulSoup(browser.page_source).text)
			token_id = re.split('#', s['results']['title'])[-1]
			data += [[ collection, mint_address, token_id ]]
			if it % 25 == 0:
				print(it, len(data))
				print(data[-2:])

			r = requests.get(url, cookies = dict(cookies), headers=headers)
			s.get(url, cookies = dict(cookies), headers=headers)
			r = s.get(url)
	sorted(mints.collection.unique())
	data = []
	s_data = []
	for i in range(1, 10001):
		if i % 25 == 0:
			print(i, len(data), len(s_data))
			if len(data) > 1:
				print(data[-2:])
			if len(s_data) > 1:
				print(s_data[-2:])
		url = 'https://howrare.is/{}/{}/'.format(collection, i)
		browser.get(url)
		sleep(0.01)
		soup = BeautifulSoup(browser.page_source)
		mint_address = re.split('/', soup.find_all('div', class_="nft_title")[0].find_all('div', class_='overflow')[0].find_all('a')[0].attrs['href'])[-1]
		data += [[ collection, i, mint_address ]]
		table = soup.find_all('div', class_='sales_history')
		if len(table) == 0:
			continue
		for row in table[0].find_all('div', class_='all_coll_row')[1:]:
			cells = row.find_all('div', class_='all_coll_col')
			if len(cells) > 1:
				s_data += [[ collection, i, cells[0].text.strip(), cells[1].text.strip() ]]
	df = pd.DataFrame(data, columns=['collection','token_id','mint_address'])
	df.to_csv('./data/solana_token_id_mint_map.csv', index=False)

def metadata_from_solscan():
	collections = [
		[ 'shadowy-super-coder', 'https://sld-gengo.s3.amazonaws.com/{}.json', 0, 10000 ]
		, [ 'degods', 'https://sld-gengo.s3.amazonaws.com/{}.json', 1, 10000 ]
		, [ 'balloonsville', 'https://bafybeih5i7lktx6o7rjceuqvlxmpqzwfh4nhr322wq5hjncxbicf4fbq2e.ipfs.dweb.link/{}.json', 0, 5000 ]
		, [ 'Stoned Ape Crew', 'https://bafybeih5i7lktx6o7rjceuqvlxmpqzwfh4nhr322wq5hjncxbicf4fbq2e.ipfs.dweb.link/{}.json', 0, 5000 ]
	]
	data = []
	token_data = []
	collection = 'DeGods'
	for i in range(0, 5000):
		if i % 25 == 2:
			print(i, len(data))
			print(data[-1])
			print(token_data[-1])
		url = 'https://sld-gengo.s3.amazonaws.com/{}.json'.format(i)
		url = 'https://bafybeih5i7lktx6o7rjceuqvlxmpqzwfh4nhr322wq5hjncxbicf4fbq2e.ipfs.dweb.link/{}.json'.format(i)
		url = 'https://metadata.degods.com/g/{}.json'.format(i)
		r = requests.get(url).json()
		token_data += [[ collection, i, r['image'] ]]
		for a in r['attributes']:
			data += [[ collection, i, a['trait_type'], a['value'] ]]
	df = pd.DataFrame(data, columns=['collection','token_id','feature_name','feature_value']).drop_duplicates()
	if False:
		df['token_id'] = df.token_id + 1
	old = pd.read_csv('./data/solscan_metadata.csv')
	old = old.append(df)
	old = old.drop_duplicates(keep='last')
	print(old[['collection','token_id']].drop_duplicates().groupby('collection').token_id.count())
	old.to_csv('./data/solscan_metadata.csv', index=False)

def add_solscan_metadata():
	solscan_metadata = pd.read_csv('./data/solscan_metadata.csv')
	metadata = pd.read_csv('./data/metadata.csv')
	solscan_metadata['collection'] = solscan_metadata.collection.apply(lambda x: clean_name(x) )
	metadata['collection'] = metadata.collection.apply(lambda x: clean_name(x) )
	rem = solscan_metadata[['collection','feature_name']].drop_duplicates()
	rem['rem'] = 1
	metadata = merge(metadata, rem, how='left', ensure=True)
	metadata = metadata[metadata.rem.isnull()]
	del metadata['rem']
	metadata[metadata.collection == 'DeGods'][['collection','feature_name']].drop_duplicates()
	metadata = metadata.append(solscan_metadata)
	metadata[metadata.collection == 'DeGods'][['collection','feature_name']].drop_duplicates()
	metadata.to_csv('./data/metadata.csv', index=False)

def get_metadata_from_metaboss():
	# mints = pd.read_csv('./data/solana_mints_3.csv')
	# os.system('metaboss -r {} -t 300 decode mint -a FYsPWQx1mr6X8McfuZ4oV3ZJaAf1NKWKU7NnYofx2MxQ  --output ~/git/nft-deal-score/data/mints/LightningOG/metadata/'.format(rpc))

	d = {
		# 'Astrals': '9ZtfJY5YggxyxNKncYFLPPFuS3fy8bx49n7Ly3M3QRJY'
		# , 'DeFi Pirates': '8oUB51HmCjwnquwQyptz2Ys9hSnqATeMQnv2jqeLPJhZ'
		# , 
		'Cets on Creck': 'FCUKDLYtjFs257BTNVjTJwrwrR14PHEKHu1Ff7HU6U4e'
	}
	collection = 'Cets on Creck'
	update_authority = 'FCUKDLYtjFs257BTNVjTJwrwrR14PHEKHu1Ff7HU6U4e'

	rpc = 'https://red-cool-wildflower.solana-mainnet.quiknode.pro/a1674d4ab875dd3f89b34863a86c0f1931f57090/'
	# update_authority = 'FCUKDLYtjFs257BTNVjTJwrwrR14PHEKHu1Ff7HU6U4e'
	# collection = 'Cets on Creck'
	for collection, update_authority in d.items():
		print('Working on {}...'.format(collection))
		collection_dir = re.sub(' ', '_', collection)

		dir = './data/mints/{}/'.format(collection_dir)
		if not os.path.exists(dir):
			os.makedirs(dir)

		os.system('metaboss -r {} -t 300 snapshot mints --update-authority {} --output {}'.format(rpc, update_authority, dir))
		# os.system('metaboss -r {} -t 300 derive metadata mints --update-authority {} --output {}'.format(rpc, update_authority, dir))

		fname = os.listdir(dir)
		if len(fname) == 1:
			fname = dir+fname[0]

			dir_mints = '{}mints/'.format(dir)
			if not os.path.exists(dir_mints):
				os.makedirs(dir_mints)
			os.system('metaboss -r {} -t 300 decode mint --list-file {} --output {}'.format(rpc, fname, dir_mints))
			# os.system('metaboss -r {} -t 300 derive metadata --list-file {} --output {}'.format(rpc, fname, dir_mints))
			# os.system('metaboss -r {} -t 300 derive metadata 5svxneN9H6xyFB1TueaEqBPDjDvPHYgV6yqyFXVR2KVK'.format(rpc))

	dir = './data/mints/{}/mints/'.format(collection_dir)
	errs = []
	m_data = []
	t_data = []
	seen = [ x[1] for x in m_data ]
	it = 0
	prv = it
	start = time.time()
	include = [
		'4eenV26jZCtRbbRRRLJ3ZnUzPQkGiJb7sncLVMQnFx3o'
		, 'J2UhB1F7YyGNXdMDvNPyVwNYMzEw9xLznBTdrwbx52Z9'
		, 'HRSJYvtv5YUBTjFe7WX87sN3cqVow2jjoPvpMr95YQQj'
		, 'AccFHHRZXmqzWVvYrQXVS1scg17mAAKuC2ZTGg9zdvrz'
		, '4t7DofyqHYdjfwqLKkHQ798MQWfce26RSnuLccuyTHvw'
		, '7VctQnWtMAEPSgfrkYDFhzPdw7ATKCuBnhKryHAi32wk'
		, '77GR2QhBAADxjjeSngqMxziAuxvSD3N1uMPhbBHht9Ar'
	]
	include = [
		'8Xo9BUSxqG4Gi2oLzeNYEJLKDV176e6Zy8HXUb9mDsFQ'
	]
	include = []
	# for fname in os.listdir(dir):
	for fname in os.listdir(dir)[it-1:]:
		if it % 10 == 0 and it:
			now = time.time()
			avg = round((now - start) / (it - prv), 1)
			print('Averaging {} seconds per NFT'.format(avg))
		# sleep(1)
		it += 1
		print(it, len(t_data), len(m_data))
		mint_address = fname[:-5]
		if len(include) and not mint_address in include:
			continue
		path = dir+fname
		with open(path) as f:
			s = f.read()
			d = json.loads(s)
			# try:
			if '#' in d['name'] and not 'Incubator' in d['name'] and not 'Infant' in d['name']:
				token_id = int(re.split('#', d['name'])[1])
				if token_id in seen:
					continue
				r = requests.get(d['uri'])
				# r = requests.get('https://api-mainnet.magiceden.dev/v2/tokens/{}'.format(mint_address))
				j = r.json()
				try:
					for a in j['attributes']:
						m_data += [[ collection, token_id, a['trait_type'], a['value'] ]]
					t_data += [[ collection, token_id, mint_address, j['image'] ]]
				except:
					print('Error with {}'.format(token_id))
					errs.append(token_id)
				# except:
				# 	pass
	m = pd.DataFrame(m_data, columns=['collection','token_id','feature_name','feature_value']).drop_duplicates()
	t = pd.DataFrame(t_data, columns=['collection','token_id','mint_address','image_url']).drop_duplicates()
	print('{} m. {} t.'.format(len(m), len(t)))
	m['chain'] = 'Solana'
	tokens = pd.read_csv('./data/tokens.csv')
	metadata = pd.read_csv('./data/metadata.csv')
	metadata[metadata.collection == collection]
	tokens[tokens.collection == collection]
	l0 = len(metadata)
	metadata = metadata.append(m)
	# metadata = metadata.merge(m, how=['outer'], on=['collection','token_id','feature_name'])
	metadata = metadata.drop_duplicates(subset=['collection','token_id','feature_name'], keep='last')
	l1 = len(metadata)
	print('Adding {} rows to metadata'.format(l1 - l0))

	l0 = len(tokens)
	# tokens = tokens.append(t)
	tokens = tokens.merge(t, how='outer', on=['collection','token_id'])
	for c in [ 'mint_address', 'image_url' ]:
		tokens[c] = tokens[c+'_y'].fillna(tokens[c+'_x'])
		del tokens[c+'_x']
		del tokens[c+'_y']
	tokens = tokens.drop_duplicates(subset=['collection','token_id'], keep='last')
	tokens = tokens[-((tokens.token_id == 0) & (tokens.collection == 'Cets on Creck'))]
	tokens['chain'] = tokens.chain.fillna('Solana')
	l1 = len(tokens)
	print('Adding {} rows to tokens'.format(l1 - l0))
	metadata.to_csv('./data/metadata.csv', index=False)
	tokens.to_csv('./data/tokens.csv', index=False)

def scrape_mints():

	nft_mint_addresses = pd.read_csv('./data/nft_mint_addresses.csv')
	sorted(nft_mint_addresses.collection.unique())
	nft_mint_addresses.head()
	nft_mint_addresses[nft_mint_addresses.collection == 'kaiju-cards']
	nft_mint_addresses['collection'] = nft_mint_addresses.collection.apply(lambda x: clean_name(x) )
	nft_mint_addresses.head()

	solana_nfts = pd.read_csv('./data/solana_nfts.csv')
	solana_nfts = solana_nfts[solana_nfts.update_authority.notnull()]
	solana_nfts = solana_nfts[solana_nfts.collection != 'Boryoku Baby Dragonz']
	print(solana_nfts.groupby('update_authority').collection.count().reset_index().sort_values('collection', ascending=0).head(10))
	
	nft_mint_addresses.collection.unique()
	nft_mint_addresses = nft_mint_addresses.merge( solana_nfts )
	nft_mint_addresses.collection.unique()
	mints = pd.read_csv('./data/solana_mints.csv')
	mints[mints.collection == 'DeGods'].to_csv('~/Downloads/tmp.csv', index=False)
	mints[mints.collection == 'DeGods'].drop_duplicates().to_csv('~/Downloads/tmp.csv', index=False)
	sorted(mints.collection.unique())
	mints[mints.collection == 'Kaiju Cards']
	# mints['tmp'] = mints.mint_address.apply(lambda x: x.lower() )
	# mints[(mints.collection == 'Kaiju Cards') & (mints.tmp == '4omp7eincl8pyuuesjromqdmet2v88wqhrvcfzaapcng')]
	mints = mints[-mints.collection.isin(nft_mint_addresses.collection.unique())]
	mints = mints.append(nft_mint_addresses[list(mints.columns)])
	mints.head()
	seen = list(mints.update_authority.unique())
	rpc = 'https://red-cool-wildflower.solana-mainnet.quiknode.pro/a1674d4ab875dd3f89b34863a86c0f1931f57090/'
	d = {}
	for row in solana_nfts.iterrows():
		row = row[1]
		d[row['collection']] = row['update_authority']

	remaining = sorted(solana_nfts[-solana_nfts.collection.isin(mints.collection.unique())].collection.unique())
	print('{}'.format(len(remaining)))
	collection = 'Lightning OG'
	d = {
		'Lightning OG':'2dtWbt8X3uQNRNLdwwybDuFLzPrZribVXDdSVaRE5JzR'
	}
	for collection in remaining:
		update_authority = d[collection]
		if update_authority in seen or collection in [ 'Solana Monkey Business','Thugbirdz','Degenerate Ape Academy','Pesky Penguins','Aurory' ]:
			print('Seen '+collection)
			continue
		else:
			print('Working on '+collection)
			sleep(.10 * 60)
			os.system('metaboss -r {} -t 300 snapshot mints --update-authority {} --output ~/git/nft-deal-score/data/mints/LightningOG/'.format(rpc, update_authority, collection))

	mints = pd.DataFrame()
	auth_to_mint = {}
	# metaboss -r https://red-cool-wildflower.solana-mainnet.quiknode.pro/a1674d4ab875dd3f89b34863a86c0f1931f57090/ decode mint --list-file ./data/mints/etc/degods.json -o ~/Downloads/degods
	# metaboss -r https://red-cool-wildflower.solana-mainnet.quiknode.pro/a1674d4ab875dd3f89b34863a86c0f1931f57090/ derive metadata ChANfqf7AP9x1rFRjZkE6n19u3tQckv65Z6r6xPqkRKR  --output ~/Downloads
	# metaboss decode mint --list-file <LIST_FILE> -o <OUPUT_DIRECTORY>


	fname = './data/mints/LightningOG/2dtWbt8X3uQNRNLdwwybDuFLzPrZribVXDdSVaRE5JzR_mint_accounts.json'
	for collection, update_authority in d.items():
		auth_to_mint[update_authority] = collection
	for fname in [ './data/mints/'+f for f in os.listdir('./data/mints') ]:
		if not '.json' in fname:
			continue
		with open(fname, 'r') as f:
			j = json.load(f)
			cur = pd.DataFrame(j)
			if len(cur):
				cur.columns = ['mint_address']
				cur['update_authority'] = re.split('/|_', fname)[4]
				cur['collection'] = cur.update_authority.apply(lambda x: auth_to_mint[x] )
				mints = mints.append(cur)
	g = mints.groupby('collection').update_authority.count().reset_index()
	mints[mints.update_authority == 'DRGNjvBvnXNiQz9dTppGk1tAsVxtJsvhEmojEfBU3ezf']
	g.to_csv('~/Downloads/tmp.csv', index=False)
	mints.to_csv('./data/solana_mints.csv', index=False)
	mints[mints.collection == 'Balloonsville'].to_csv('./data/solana_mints_2.csv', index=False)
	cur.to_csv('./data/solana_mints_3.csv', index=False)

	url = 'https://solscan.io/token/GxERCTcBDmB6pfEoYYNWvioAhACifEGdn3dXNqVh5rXz'
	url = 'https://explorer.solana.com/address/6CCprsgJT4nxBMSitGathXcLshDTL3BE4LcJXvSFwoe2'
	r = requests.get(url)

# scrape_listings(['smb'])
# alerted = []
# for i in range(1):
# 	alerted = scrape_listings(alerted = alerted)
# 	sleep_to = (datetime.today() + timedelta(minutes=15)).strftime("%H:%M %p")
# 	print('Sleeping until {}'.format(sleep_to))
# 	sleep(60 * 15)
# alerted = []
# scrape_randomearth()
# alerted = scrape_listings(alerted = alerted)
# convert_collection_names()