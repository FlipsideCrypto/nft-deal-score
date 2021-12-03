import os
import re
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

os.chdir('/Users/kellenblumberg/git/nft-deal-score')
# os.chdir('/Users/kellenblumberg/git/nft-deal-score/viz/www/img/aurory/')
os.environ['PATH'] += os.pathsep + '/Users/kellenblumberg/shared/'

browser = webdriver.Chrome()


def scrape_recent_sales():
	o_sales = pd.read_csv('./data/sales.csv')
	sales = pd.DataFrame()
	collections = [ 'smb','thugbirdz']
	for collection in collections:
		url = 'https://qzlsklfacc.medianetwork.cloud/all_sold_per_collection_day?collection={}'.format(collection)
		r = requests.get(url)
		j = r.json()
		cur = pd.DataFrame(j)
		cur['collection'] = collection
		cur['sale_date'] = cur.date.apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.000Z'))
		sales = sales.append( cur[['collection','token_id','price','sale_date']] )

def scrape_listings(collections = [ 'aurory','thugbirdz','smb','degenapes' ], alerted = []):
	data = []
	# collections = [ 'aurory','thugbirdz','meerkatmillionaires','aurory','degenapes' ]
	# collections = [ 'aurory','thugbirdz','smb','degenapes' ]
	# collections = [ 'smb' ]
	d = {
		'smb': 'solana-monkey-business'
		, 'degenapes': 'degen-ape-academy'
	}
	for collection in collections:
		c = d[collection] if collection in d.keys() else collection
		url = 'https://solanafloor.com/nft/{}/listed'.format(c)
		browser.get(url)
		sleep(5)
		has_more = True
		page = 1
		seen = []
		while has_more and page < 20:
			# scroll = browser.find_element_by_class_name('ag-center-cols-viewport')
			print('{} page #{} ({})'.format(collection, page, len(data)))
			sleep(3)
			page += 1
			for j in [25, 30, 35, 30, 25] * 1:
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
							price = row.find_all('div', {'col-id':'price'})
							if len(token_id) and len(price):
								token_id = int(token_id[0].text)
								price = float(price[0].text)
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
	old = pd.read_csv('./data/listings.csv')
	listings = pd.DataFrame(data, columns=['collection','token_id','price']).drop_duplicates()
	old = old[ -(old.collection.isin(listings.collection.unique())) ]
	pred_price = pd.read_csv('./data/pred_price.csv')
	df = listings.merge(pred_price[['collection','token_id','pred_price']])
	df['ratio'] = df.pred_price / df.price
	print(df[df.collection == 'smb'].sort_values('ratio', ascending=0).head())
	listings[ listings.collection == 'smb' ].head()
	print(listings.groupby('collection').token_id.count())
	listings = listings.append(old)
	listings.to_csv('./data/listings.csv', index=False)

	listings = listings.sort_values('price')
	t1 = listings.groupby('collection').head(1).rename(columns={'price':'t1'})
	t2 = listings.groupby('collection').head(2).groupby('collection').tail(1).rename(columns={'price':'t2'})
	t = t1.merge(t2, on=['collection'])
	t['pct'] = t.t2 / t.t1
	t['dff'] = t.t2 - t.t1
	t = t[ (t.pct >= 1.15) | (t.dff >= 10 ) ]

	pred_price = pd.read_csv('./data/pred_price.csv')[['collection','token_id','pred_price','pred_sd']]
	pred_price = pred_price.merge(listings)

	coefsdf = pd.read_csv('./data/coefsdf.csv')
	coefsdf['tot'] = coefsdf.lin_coef + coefsdf.log_coef
	coefsdf['lin_coef'] = coefsdf.lin_coef / coefsdf.tot
	coefsdf['log_coef'] = coefsdf.log_coef / coefsdf.tot
	pred_price = pred_price.merge(coefsdf)
	floor = listings.groupby('collection').price.min().reset_index().rename(columns={'price':'floor'})
	pred_price = pred_price.merge(floor)

	pred_price['abs_chg'] = (pred_price.floor - pred_price.floor_price) * pred_price.lin_coef
	pred_price['pct_chg'] = (pred_price.floor - pred_price.floor_price) * pred_price.log_coef
	pred_price['pred_price_0'] = pred_price.pred_price
	pred_price['pred_price'] = pred_price.apply( lambda x: x['pred_price'] + x['abs_chg'] + ( x['pct_chg'] * x['pred_price'] / x['floor_price'] ), 1 )
	pred_price['pred_price'] = pred_price.apply( lambda x: max( x['pred_price'], x['floor']), 1 )
	# pred_price['deal_score'] = pred_price.apply( lambda x: (( x['pred_price'] - x['price'] ) * 50 / ( 3 * x['pred_sd'])) + 50  , 1 )
	# pred_price['deal_score'] = pred_price.deal_score.apply( lambda x: min(max(0, x), 100) )
	pred_price['deal_score'] = pred_price.apply( lambda x: 100 * (1 - norm.cdf( x['price'], x['pred_price'], 2 * x['pred_sd'] * x['pred_price'] / x['pred_price_0'] )) , 1 )

	pred_price = pred_price.sort_values(['deal_score'], ascending=[0])
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
	g = g.sort_values('deal_score', ascending=0)
	for row in g.iterrows():
		row = row[1]
		txt = '{} | {} | ${} | {}'.format( str(row['collection']).ljust(10), str(row['token_id']).ljust(5), str(round(row['price'])).ljust(3), round(row['deal_score']) )
		s += '{}\n'.format(txt)
	s += '```'
	print(s)

	mUrl = 'https://discord.com/api/webhooks/916027651397914634/_zrDNThkwu2ZYB4F503JNRtwhPh9EJ642rIdENawlJu1Di0dpfKT_ba045xXGCefAFvI'

	data = {"content": s}
	response = requests.post(mUrl, json=data)

	return alerted

def scrape_solanafloor():
	data = []
	collections = [ 'aurory','thugbirdz','smb','degenapes' ]
	collections = [ 'degenapes' ]
	d = {
		'smb': 'solana-monkey-business'
		, 'degenapes': 'degen-ape-academy'
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
			for j in [20, 25, 30, 25] * 3:
				for _ in range(3):
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
	tokens[ tokens.collection == 'degenapes' ].sort_values('token_id')
	print(tokens.groupby('collection').token_id.count())
	tokens.to_csv('./data/tokens.csv', index=False)

def scrape_solana_explorer():
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

def scrape_tx():
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

	o_metadata[ (o_metadata.collection == 'smb') & (o_metadata.feature_name == 'type') ]
	ts = o_metadata[ (o_metadata.collection == 'smb') & (o_metadata.feature_name == 'type') & (o_metadata.feature_value == 'Zombie (7.28%)')].token_id.unique()
	len(ts)

	errors = []
	data = []
	s_data = []
	collections = {
		'smb': 5000,
		'aurory': 10000,
		'degenapes': 10000,
		'thugbirdz': 3333,
		'meerkatmillionaires': 10000
	}
	k = 'smb'
	v = 10000
	seen = o_metadata[ (o_metadata.collection == k) ].token_id.unique()
	opener = urllib.request.build_opener()
	opener.addheaders = [('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
	urllib.request.install_opener(opener)
	i = 1
	j = i
	# for i in range(j, v + 1):
	# ts = [ x[1] for x in errors ]
	for i in ts:
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
		r = requests.get(url)
		soup = BeautifulSoup(r.text)
		atts = { 'token_id': i, 'collection': k }
		try:
			ul = soup.find_all('ul', class_='attributes')[0]
		except:
			try:
				sleep(2)
				r = requests.get(url)
				soup = BeautifulSoup(r.text)
				ul = soup.find_all('ul', class_='attributes')[0]
			except:
				try:
					sleep(10)
					r = requests.get(url)
					soup = BeautifulSoup(r.text)
					ul = soup.find_all('ul', class_='attributes')[0]
				except:
					errors += [[ k, i ]]
					print('Error on ID #{}'.format(i))
					continue
		t_1 = time.time()
		print('Took {} seconds to load page'.format(round(t_1 - t_0, 1)))
		lis = ul.find_all('li')
		if len(lis) == 0:
			print('No atts on ID #{}'.format(i))
		for li in lis:
			# try:
			if len(li.find_all('span')) + len(li.find_all('div')) == 2:
				att = re.sub(' ', '_', re.sub( ':', '', li.find_all('span')[0].text)).strip().lower()
				val = re.sub(' ', '', li.find_all('div')[0].text.strip())
				val = ' '.join(li.find_all('div')[0].text.strip().split())
				atts[att] = val
			# except:
			# pass
		data += [atts]
		table = soup.find_all('table', class_='table')
		# img = soup.find_all('img')
		# if len(img):
		# 	src = img[0].attrs['src']
		# 	urllib.request.urlretrieve(src, './viz/www/img/{}/{}.png'.format(k, i))
		# print('Took {} seconds to load image'.format(round(time.time() - t_1, 1)))
		if len(table):
			table = table[0]
			for tr in table.find_all('tbody')[0].find_all('tr'):
				td = tr.find_all('td')
				tx_id = td[5].find_all('a')[0].attrs['href'] if len(td[5].find_all('a')) > 0 else None
				s_data += [[ k, i, td[0].text, td[2].text, td[3].find_all('a')[0].attrs['href'], td[4].find_all('a')[0].attrs['href'], tx_id ]]
		# except:
	df = pd.DataFrame(data)
	# seen = list(df.token_id.unique())
	s_df = pd.DataFrame(s_data, columns=['collection','token_id','sale_date','price','seller','buyer','tx_id'])
	s_df['price'] = s_df.price.apply(lambda x: re.sub('SOL', '', x).strip() ).astype(float)
	s_df['seller'] = s_df.seller.apply(lambda x: re.split('=', x)[-1] )
	s_df['buyer'] = s_df.buyer.apply(lambda x: re.split('=', x)[-1] )
	s_df['tx_id'] = s_df.tx_id.apply(lambda x: re.split('/', x)[-1] if x and x == x else None )
	s_df.sort_values('price').tail(30)
	# s_df[[ 'token_id','sale_date','price' ]].to_csv('~/Downloads/smb_zombie_sales.csv', index=False)

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
	s_df = o_sales.append(s_df).drop_duplicates()
	print(s_df.groupby('collection').token_id.count())
	len(s_df)
	s_df = s_df.drop_duplicates()
	s_df.to_csv('./data/sales.csv', index=False)
	sales = pd.read_csv('./data/sales.csv')
	sales[sales.collection == 'smb'][['token_id','sale_date','price']].to_csv('~/Downloads/historical_monke_sales.csv', index=False)

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
	o_metadata.head()
	o_sales.head()
	o_sales.to_csv('./data/md_sales.csv', index=False)

# scrape_listings(['smb'])
alerted = []
for i in range(10):
	alerted = scrape_listings(alerted = alerted)
	sleep_to = (datetime.today() + timedelta(minutes=25)).strftime("%H:%M %p")
	print('Sleeping until {}'.format(sleep_to))
	sleep(60 * 25)