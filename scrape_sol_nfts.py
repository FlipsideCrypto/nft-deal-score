import os
import re
import json
import time
import requests
import functools
import collections
import pandas as pd
import urllib.request
from time import sleep
from scipy.stats import norm
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
from selenium.webdriver.common.keys import Keys

os.chdir('/Users/kellenblumberg/git/nft-deal-score')
os.environ['PATH'] += os.pathsep + '/Users/kellenblumberg/shared/'

# browser = webdriver.Chrome()

# old = pd.read_csv('./data/tokens.csv')
# metadata[(metadata.collection == 'Galactic Punks') & (metadata.feature_name=='attribute_count')].drop_duplicates(subset=['feature_value']).merge(old)

clean_names = {
	'aurory': 'Aurory'
	,'thugbirdz': 'Thugbirdz'
	,'smb': 'Solana Monkey Business'
	,'degenapes': 'Degen Apes'
	,'peskypenguinclub': 'Pesky Penguins'
	,'meerkatmillionaires': 'Meerkat Millionaires'
	,'boryokudragonz': 'Boryoku Dragonz'
}

def clean_name(name):
	if name in clean_names.keys():
		return(clean_names[name])
	return(name)

def convert_collection_names():
	d = {
		'aurory': 'Aurory'
		,'thugbirdz': 'Thugbirdz'
		,'smb': 'Solana Monkey Business'
		,'degenapes': 'Degen Apes'
		,'peskypenguinclub': 'Pesky Penguins'
		,'meerkatmillionaires': 'Meerkat Millionaires'
		,'boryokudragonz': 'Boryoku Dragonz'
	}
	for c in [ 'pred_price', 'attributes', 'feature_values', 'model_sales', 'listings', 'coefsdf', 'tokens' ]:
		try:
			df = pd.read_csv('./data/{}.csv'.format(c))
			df['collection'] = df.collection.apply(lambda x: clean_name(x) if x in d.keys() else x )
			df.to_csv('./data/{}.csv'.format(c), index=False)
		except:
			print('error',c)
			pass

def scrape_magic_eden_sales():
	url = 'https://api-mainnet.magiceden.io/rpc/getGlobalActivitiesByQuery?q={%22$match%22:{%22collection_symbol%22:%22pesky_penguins%22},%22$sort%22:{%22blockTime%22:-1},%22$skip%22:0}'
	results = requests.get(url).json()['results']
	df = pd.DataFrame([ x['parsedTransaction'] for x in results if 'parsedTransaction' in x.keys()])
	df[[ 'blockTime','collection_symbol','total_amount' ]]

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

	mUrl = 'https://discord.com/api/webhooks/931615663565443082/L_cQCI_fs1D4vOC5lCoP0qJuzvZKVX-3wx-jNSBw32v0HwWxz6OcKK0iGOz9T1-1xElf'

	data = {"content": s}
	response = requests.post(mUrl, json=data)

	return alerted

def scrape_randomearth(browser):
	print('Querying randomearth.io sales...')
	d_address = {
		'Galactic Punks': 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k',
		'LunaBulls': 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2',
		'Levana Dragon Eggs': 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg',
	}
	data = []
	# for collection in [ 'Levana Dragon Eggs' ]:
	for collection in d_address.keys():
		print(collection)
		page = 0
		has_more = True
		while has_more:
			page += 1
			print('Page #{} ({})'.format(page, len(data)))
			url = 'https://randomearth.io/api/items?collection_addr={}&sort=price.asc&page={}&on_sale=1'.format( d_address[collection], page)
			browser.get(url)
			soup = BeautifulSoup(browser.page_source)
			# txt = browser.page_source
			j = json.loads(soup.text)
			has_more = 'items' in j.keys() and len(j['items'])
			if has_more:
				for i in j['items']:
					data += [[ 'Terra', collection, i['token_id'], i['price'] / (10 ** 6) ]]
		df = pd.DataFrame(data, columns=['chain','collection','token_id','price'])
		df.to_csv('~/Downloads/tmp.csv', index=False)
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
	collections = [ 'aurory','thugbirdz','degenape']
	collection = 'peskypenguinclub'
	for collection in collections:
		url = 'https://qzlsklfacc.medianetwork.cloud/all_sold_per_collection_day?collection={}'.format(collection)
		# url = 'https://qzlsklfacc.medianetwork.cloud/all_sold_per_collection_day?collection={}'.format('degenape')
		# url = 'https://hkgwtdvfyh.medianetwork.cloud/all_sold_per_collection_day?collection={}'.format(collection)
		r = requests.get(url)
		j = r.json()
		cur = pd.DataFrame(j)
		cur['collection'] = 'degenapes' if collection == 'degenape' else collection
		cur['sale_date'] = cur.date.apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.000Z'))
		cur['token_id'] = cur.name.apply(lambda x: int(re.split('#', x)[1]) )
		sales = sales.append( cur[['collection','token_id','price','sale_date']] )
	l0 = len(o_sales)
	print(sales.groupby('collection').token_id.count())
	sales['tmp'] = sales.sale_date.apply(lambda x: str(x)[:10] )
	sales['chain'] = 'Solana'
	o_sales['chain'] = o_sales['chain'].fillna('Solana')
	o_sales[['chain','collection']].drop_duplicates()
	o_sales['tmp'] = o_sales.sale_date.apply(lambda x: str(x)[:10] )
	o_sales = o_sales.append(sales)
	o_sales['token_id'] = o_sales.token_id.apply(lambda x: str(x) )
	o_sales['collection'] = o_sales.collection.apply(lambda x: clean_name(x) )
	o_sales = o_sales.drop_duplicates(subset=['collection','token_id','tmp','price'])
	o_sales[o_sales.token_id=='1606']
	o_sales = o_sales.drop_duplicates(subset=['collection','token_id','tmp','price'])
	l1 = len(o_sales)
	print('{} -> {} (added {})'.format(l0, l1, l1 - l0))
	o_sales.groupby('collection').tmp.max()
	print(o_sales.groupby('collection').token_id.count())
	del o_sales['tmp']
	o_sales.to_csv('./data/sales.csv', index=False)

def scrape_listings(browser, collections = [ 'aurory','thugbirdz','smb','degenapes','peskypenguinclub' ], alerted = [], is_listings = True):
	print('Scraping solanafloor listings...')
	data = []
	# collections = [ 'aurory','thugbirdz','meerkatmillionaires','aurory','degenapes' ]
	# collections = [ 'aurory','thugbirdz','smb','degenapes' ]
	# collections = [ 'smb' ]
	d = {
		'smb': 'solana-monkey-business'
		, 'degenapes': 'degen-ape-academy'
		, 'peskypenguinclub': 'pesky-penguins'
	}
	collection = 'smb'
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
					soup = BeautifulSoup(browser.page_source)
					# for row in browser.find_elements_by_class_name('ag-row'):
					# 	cells = row.find_elements_by_class_name('ag-cell')
					# 	if len(cells) > 3:
					# 		token_id = int(cells[0].text)
					# 		price = float(cells[4].text)
					# 		data += [[ collection, token_id, price ]]
					d0 = soup.find_all('div', class_='ag-pinned-left-cols-container')
					d1 = soup.find_all('div', class_='ag-center-cols-clipper')
					if not len(d0) or not len(d1):
						continue
					d0 = d0[0]
					d1 = d1[0]
					rows0 = d0.find_all('div', class_='ag-row')
					rows1 = d1.find_all('div', class_='ag-row')
					for k in range(len(rows0)):
						# for row in soup.find_all('div', class_='ag-row'):
						# 	# print(row.text)
						cell0 = rows0[k].find_all('div', class_='ag-cell')
						cell1 = rows1[k].find_all('div', class_='ag-cell')
						if len(cell1) > 2:
							token_id = cell0[0].text
							price = cell1[2 if is_listings else 0].text
							if len(token_id) and len(price):
								# token_id = int(token_id[0].text)
								# price = float(price[0].text)
								if not token_id.strip():
									continue
								token_id = int(token_id)
								price = float(price) if price.strip() else 0
								if not price and is_listings:
									continue
								if not token_id in seen:
									data += [[ collection, token_id, price ]]
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
	if not is_listings:
		old = pd.read_csv('./data/solana_rarities.csv')
		rarities = pd.DataFrame(data, columns=['collection','token_id','nft_rank']).drop_duplicates()
		rarities = rarities.append(old).drop_duplicates()
		print(rarities.groupby('collection').token_id.count())
		rarities.to_csv('./data/solana_rarities.csv', index=False)

	old = pd.read_csv('./data/listings.csv')
	listings = pd.DataFrame(data, columns=['collection','token_id','price']).drop_duplicates()
	# others = scrape_magic_eden()
	# listings = listings.append(others).drop_duplicates()
	d = {
		'aurory': 'Aurory'
		,'thugbirdz': 'Thugbirdz'
		,'smb': 'Solana Monkey Business'
		,'degenapes': 'Degen Apes'
		,'peskypenguinclub': 'Pesky Penguins'
		,'meerkatmillionaires': 'Meerkat Millionaires'
		,'boryokudragonz': 'Boryoku Dragonz'
	}
	listings['collection'] = listings.collection.apply(lambda x: clean_name(x))
	listings[listings.token_id=='1656']
	listings[listings.token_id==1656]

	old = old[ -(old.collection.isin(listings.collection.unique())) ]
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
	old[ old.collection == 'LunaBulls' ].head()
	tokens = old.append(tokens).drop_duplicates()
	print(tokens.groupby('collection').token_id.count())
	tokens.to_csv('./data/tokens.csv', index=False)

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

def scratch():
	o_metadata = pd.read_csv('./data/metadata.csv')
	o_sales = pd.read_csv('./data/sales.csv')
	o_sales[ o_sales.collection == 'Solana Monkey Business' ].head()
	o_sales[ o_sales.collection == 'Solana Monkey Business' ].sort_values('sale_date', ascending=0).head()
	o_sales['tmp'] = o_sales.sale_date.apply(lambda x: str(x)[:10] )
	o_sales['n_sales'] = 1
	o_sales.groupby(['collection','tmp'])[['price','n_sales']].sum().reset_index().to_csv('./data/tableau_sales_data.csv', index=False)
	o_metadata.head()
	o_sales.head()
	o_sales.to_csv('./data/md_sales.csv', index=False)

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