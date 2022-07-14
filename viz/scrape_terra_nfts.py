import re
import json
import cloudscraper
import pandas as pd

from time import sleep

clean_names = {
	'aurory': 'Aurory'
	,'thugbirdz': 'Thugbirdz'
	,'smb': 'Solana Monkey Business'
	,'degenapes': 'Degen Apes'
	,'peskypenguinclub': 'Pesky Penguins'
	,'meerkatmillionaires': 'Meerkat Millionaires'
	,'boryokudragonz': 'Boryoku Dragonz'
	,'degods': 'DeGods'
	,'lunabulls': 'LunaBulls'
	,'boredapekennelclub': 'BAKC'
	,'boredapeyachtclub': 'BAYC'
	,'mutantapeyachtclub': 'MAYC'
	,'bayc': 'BAYC'
	,'mayc': 'MAYC'
	,'solgods': 'SOLGods'
	,'meerkatmillionairescc': 'Meerkat Millionaires'
	# ,'stonedapecrew': 'Stoned Ape Crew'
}

def clean_token_id(df, data_folder):
	tokens = pd.read_csv(data_folder+'nft_deal_score_tokens.csv')
	df['collection'] = df.collection.apply(lambda x: clean_name(x))
	df['token_id'] = df.token_id.apply(lambda x: re.sub('"', '', x) if type(x)==str else x )
	df['tmp'] = df.token_id.apply(lambda x: x[:10] )
	tokens['tmp'] = tokens.token_id.apply(lambda x: str(x)[:10] )
	df = df.merge(tokens[['collection','tmp','clean_token_id']], how='left', on=['collection','tmp'])
	df['token_id'] = df.clean_token_id.fillna(df.token_id)
	df['token_id'] = df.token_id.astype(int)
	del df['tmp']
	del df['clean_token_id']
	return(df)

def clean_name(name):
	x = re.sub('-', '', name).lower()
	x = re.sub(' ', '', x).lower()
	if x in clean_names.keys():
		return(clean_names[x])
	name = name.title()
	name = re.sub('-', ' ', name)
	name = re.sub(' On ', ' on ', name)
	name = re.sub('Defi ', 'DeFi ', name)
	# name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
	return(name)

def scrape_randomearth(data_folder = '/rstudio-data/'):
	print('Querying randomearth.io sales...')
	d_address = {
		'Galactic Angels': 'terra13nccm82km0ttah37hkygnvz67hnvkdass24yzv',
		'Galactic Punks': 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k',
		'LunaBulls': 'lunabulls',
		'Levana Dragon Eggs': 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg',
	}
	data = []
	scraper = cloudscraper.create_scraper()
	# for collection in [ 'Levana Dragon Eggs' ]:
	for collection in d_address.keys():
		print(collection)
		page = 0
		has_more = True
		while has_more:
			sleep(0.1)
			page += 1
			# print('Page #{} ({})'.format(page, len(data)))
			tmp = d_address[collection]
			t = 'collection_addr' if tmp[:5] == 'terra' else 'collection_slug'
			url = 'https://randomearth.io/api/items?{}={}&sort=price.asc&page={}&on_sale=1'.format( t, tmp, page)
			# url = 'https://randomearth.io/api/items?page=1&collection_addr=terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2'
			# url = 'https://randomearth.io/api/items?page=1&collection_addr=terra18d5cqlsqgxp8w7ysn48l4r8a5328592wfwjtyz'
			t = scraper.get(url).text
			# r = requests.get(url)
			# browser.get(url)
			# soup = BeautifulSoup(browser.page_source)
			# txt = browser.page_source
			j = json.loads(t)
			has_more = 'items' in j.keys() and len(j['items'])
			if has_more:
				for i in j['items']:
					data += [[ 'Terra', collection, str(i['token_id']), float(i['price']) / (10 ** 6) ]]
			else:
				print(t[:30])
	df = pd.DataFrame(data, columns=['chain','collection','token_id','price'])
	df = clean_token_id(df, data_folder)
	# df.token_id.unique()
	# df.to_csv('~/Downloads/tmp.csv', index=False)
	# old = pd.read_csv('./data/listings.csv')
	# old = old[-old.collection.isin(df.collection.unique())]
	# old = old.append(df)
	# print(old.groupby('collection').token_id.count())
	# old.to_csv('./data/listings.csv', index=False)
	return(df[[ 'collection','token_id','price' ]])