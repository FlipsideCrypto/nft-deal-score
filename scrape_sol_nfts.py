import os
import re
import time
import requests
import functools
import collections
import pandas as pd
import urllib.request
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver

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

def scrape_listings():
	data = []
	# collections = [ 'aurory','thugbirdz','meerkatmillionaires','aurory','degenapes' ]
	collections = [ 'aurory','thugbirdz','smb','degenapes' ]
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
	listings = pd.DataFrame(data, columns=['collection','token_id','price']).drop_duplicates()
	pred_price = pd.read_csv('./data/pred_price.csv')
	df = listings.merge(pred_price[['collection','token_id','pred_price']])
	df['ratio'] = df.pred_price / df.price
	print(df[df.collection == 'smb'].sort_values('ratio', ascending=0).head())
	listings[ listings.collection == 'smb' ].head()
	print(listings.groupby('collection').token_id.count())
	listings.to_csv('./data/listings.csv', index=False)

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
	k = 'degenapes'
	v = 10000
	seen = o_metadata[ (o_metadata.collection == k) ].token_id.unique()
	opener = urllib.request.build_opener()
	opener.addheaders = [('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
	urllib.request.install_opener(opener)
	i = 1
	j = i
	for i in range(j, v + 1):
		if i in seen:
			continue
		t_0 = time.time()
		print('Token ID #{}: {}'.format(i, datetime.fromtimestamp(t_0)))
		if i> 0 and i % 25 == 1:
			print(i, len(data))
			sleep(20)
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
		img = soup.find_all('img')
		if len(img):
			src = img[0].attrs['src']
			urllib.request.urlretrieve(src, './viz/www/img/{}/{}.png'.format(k, i))
		print('Took {} seconds to load image'.format(round(time.time() - t_1, 1)))
		if len(table):
			table = table[0]
			for tr in table.find_all('tbody')[0].find_all('tr'):
				td = tr.find_all('td')
				tx_id = td[5].find_all('a')[0].attrs['href'] if len(td[5].find_all('a')) > 0 else None
				s_data += [[ k, i, td[0].text, td[2].text, td[3].find_all('a')[0].attrs['href'], td[4].find_all('a')[0].attrs['href'], tx_id ]]
		# except:
	df = pd.DataFrame(data)
	s_df = pd.DataFrame(s_data, columns=['collection','token_id','sale_date','price','seller','buyer','tx_id'])
	s_df['price'] = s_df.price.apply(lambda x: re.sub('SOL', '', x).strip() ).astype(float)
	s_df['seller'] = s_df.seller.apply(lambda x: re.split('=', x)[-1] )
	s_df['buyer'] = s_df.buyer.apply(lambda x: re.split('=', x)[-1] )
	s_df['tx_id'] = s_df.tx_id.apply(lambda x: re.split('/', x)[-1] if x and x == x else None )
	s_df.sort_values('price').tail(30)

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

scrape_listings()