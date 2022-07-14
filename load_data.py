import re
import os
import json
import time
import math
import requests
import pandas as pd
import urllib.request
import snowflake.connector
from bs4 import BeautifulSoup
from time import sleep

import cloudscraper

from theblockchainapi import SolanaAPIResource, SolanaNetwork, SearchMethod

# Get an API key pair for free here: https://dashboard.blockchainapi.com/api-keys
MY_API_KEY_ID = 'sLbjx8YFYdTtUuH'
MY_API_SECRET_KEY = 'p24pFaM9lLbWscN'
BLOCKCHAIN_API_RESOURCE = SolanaAPIResource(
    api_key_id=MY_API_KEY_ID,
    api_secret_key=MY_API_SECRET_KEY
)

os.chdir('/Users/kellenblumberg/git/nft-deal-score')

from solana_model import just_float
from utils import clean_name, clean_token_id, format_num, merge



#########################
#     Connect to DB     #
#########################
with open('snowflake.pwd', 'r') as f:
	pwd = f.readlines()[0].strip()
with open('snowflake.usr', 'r') as f:
	usr = f.readlines()[0].strip()

ctx = snowflake.connector.connect(
	user=usr,
	password=pwd,
	account='vna27887.us-east-1'
)



# query = '''
# 	SHOW TABLES
# '''
# sales = ctx.cursor().execute(query)
# sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
# sales = clean_colnames(sales)

# sorted(sales.name.unique())
# sorted(sales.schema_name.unique())
# sorted(sales.database_name.unique())

# sales[sales.name == 'ACTIVE_VAULT_EVENTS'][['name','schema_name']]

# tables = pd.DataFrame()
# df = sales[sales.schema_name.isin(['BRONZE_MIDGARD_2_6_9','BRONZE_MIDGARD_20211108_MIDGARD']) ]
# for row in df.iterrows():
# 	row = row[1]
# 	query = 'DESCRIBE TABLE {}.{}'.format(row['schema_name'], row['name'])
# 	table = ctx.cursor().execute(query)
# 	table = pd.DataFrame.from_records(iter(table), columns=[x[0] for x in table.description])
# 	table = clean_colnames(table)
# 	table['schema_name'] = row['schema_name']
# 	table['table_name'] = row['name']
# 	table.head()
# 	tables = tables.append(table)
# tables['clean_table_name'] = tables.table_name.apply(lambda x: re.sub('MIDGARD_', '', x) )
# a = tables[tables.schema_name == 'BRONZE_MIDGARD_20211108_MIDGARD'][['name','clean_table_name','type']]
# b = tables[tables.schema_name == 'BRONZE_MIDGARD_2_6_9'][['name','clean_table_name','type']]

# c = a.merge(b, on=['clean_table_name','name'], how='outer')
# c['is_hevo_fivetran'] = c.name.apply(lambda x: int(x[:7] == '__HEVO_' or x[:10] == '_FIVETRAN_') )
# c['in_old'] = (c.type_x.notnull()).astype(int)
# c['in_new'] = (c.type_y.notnull()).astype(int)
# c['in_both'] = ((c.in_old + c.in_new) == 2).astype(int)
# c.to_csv('~/Downloads/tmp.csv', index=False)

d_market = {
	'Galactic Punks': 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k',
	'LunaBulls': 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2',
	'Levana Dragon Eggs': 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg',
	'Levana Dust': 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7',
	'Levana Meteors': 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v',
	'Galactic Angels': 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v',
}

###################################
#     Define Helper Functions     #
###################################
def clean_colnames(df):
	names = [ x.lower() for x in df.columns ]
	df.columns = names
	return(df)

def add_collection_steps():
	# 1. mint_address_token_id_map
	# 2. scrape metadata
	metadata = pd.read_csv('./data/metadata.csv')
	metadata['collection'] = metadata.collection.apply(lambda x: clean_name(x) )
	sorted(metadata.collection.unique())
	metadata.to_csv('./data/metadata.csv', index=False)
	metadata[metadata.collection == 'Stoned Ape Crew']
	metadata[metadata.collection == 'Stoned Ape Crew'].feature_name.unique()
	# 3. scrape howrareis
	# 4. add sales
	# 5. run model
	pass

def create_upload_file():
	cols = [ 'collection','mint_address' ]
	a = pd.read_csv('./data/mints-2022-06-13-2pm.csv')[cols]
	b = pd.read_csv('~/Downloads/manual_labels.csv')
	b.columns = cols
	c = pd.read_csv('~/Downloads/solscan_collections.csv')[cols]
	d = pd.read_csv('./data/tokens.csv')[cols]
	df = pd.concat([a, b, c, d]).drop_duplicates(subset=['mint_address'], keep='last')
	df.to_csv('~/Downloads/mints-2022-06-13-5pm.csv', index=False)
	tmp = pd.read_csv('~/Downloads/mints-2022-06-13-5pm.csv')
	tmp[tmp.mint_address == 'EhuVN896QVypRreAt6mcJr6eKkKunVzsgSRz7qt4oeBr']
	

def manual_clean():
	for c in [ 'pred_price', 'attributes', 'feature_values', 'model_sales', 'listings', 'coefsdf', 'tokens' ]:
		df = pd.read_csv('./data/{}.csv'.format(c))
		df['chain'] = 'Solana'
		if c == 'tokens':
			df['clean_token_id'] = df.token_id
		df.to_csv('./data/{}.csv'.format(c), index=False)


def pull_from_solscan():

	todo = [
		['50a75e6d3d0b6d4a72b2f745fdba4b1c28bc774ca9629fe8e36053ae2fb396f8','Degen Egg']
		, ['45e3f45d695e9e8775eed480cb0f5a6a957d47dcb3ed3800e454846dca9ab7fc','Genopets']
		, ['a437071c6f9679e8431a072ae39421262bf289cc6ead21e38190d5b7b409e7f7','Shin Sengoku']
		, ['d38349f2704e8cd1c538cc48fbea4b3e2596ac8da14b62c0eb3c07aeda7ae75e','SolStein']
		, ['9e0593a4842ceb9ccdc510e6ffdf0d84f736bff2b58d5803c5002ace17df9fe0','Zillaz NFT']
		, ['895d8f01108fbb6b28c5e32027c9c98e3054241927c8e59c304fa4763c5c88ea','enviroPass Tier 02']
		, ['59c2a35d902f85feec4c774df503a0df2be263f763dcbcb73bce50c999fc2c78','The Fracture']
		, ['e8dfb059b1dfc71cf97342a1c46793bc5e154909416a93a155929da5bba44a57','Suteki']
		, ['271e0d68d069d80afbcb916e877831b060933b97e7b02e1cfb77e74b228b4745','Chillchat']
	]
	start = time.time()
	data = []
	meta = []
	it = 0
	tot = len(todo)
	for collectionId, collection in todo:
		it += 1
		print('#{} / {}'.format(it, tot))
		# collectionId = j['data']['collectionId']
		# collection = j['data']['collection']
		offset = 0
		limit = 500
		while True:
			print(offset)
			url = 'https://api.solscan.io/collection/nft?sortBy=nameDec&collectionId={}&offset={}&limit={}'.format(collectionId, offset, limit)
			r = requests.get(url)
			js = r.json()['data']
			offset += limit
			if len(js) == 0:
				break
			for j in js:
				data += [[ collectionId, collection, j['info']['mint'] ]]
				m = j['info']['meta']
				m['mint_address'] = j['info']['mint']
				# m['name'] = row['name']
				# m['update_authority'] = update_authority
				meta += [ m ]
		it += 1
		end = time.time()
		print('Finished {} / {} in {} minutes'.format(it, tot, round((end - start) / 60.0, 1)))
	df = pd.DataFrame(data, columns=['collection_id','collection','mint_address'])
	df.to_csv('~/Downloads/solscan_collections.csv', index=False)
	df[['collection','mint_address']].to_csv('~/Downloads/mints-2022-06-14-8am.csv', index=False)
	df.groupby('collection').mint_address.count()

def collecitons_from_missing_tokens():
	query = '''
		WITH base AS (
			SELECT block_timestamp::date AS date
			, s.*
			, ROW_NUMBER() OVER (ORDER BY sales_amount DESC) AS rn
			FROM solana.fact_nft_sales s
			LEFT JOIN solana.dim_labels l on s.mint = l.address
			WHERE marketplace in ('magic eden v1', 'magic eden v2') 
			AND block_timestamp >= '2022-01-01' 
			AND l.address IS NULL
			AND sales_amount >= 10
		)
		SELECT *
		FROM base
		WHERE rn % 20 = 0
		ORDER BY sales_amount DESC
		LIMIT 500
	'''
	missing = ctx.cursor().execute(query)
	missing = pd.DataFrame.from_records(iter(missing), columns=[x[0] for x in missing.description])
	missing = clean_colnames(missing)
	missing.head()

	headers = {
		'Authorization': 'Bearer 9c39e05c-db3c-4f3f-ac48-84099111b813'
	}
	it = 0
	tot = len(missing)
	data = []
	for m in missing.mint.unique():
		it += 1
		if it % 10 == 0:
			print('#{} / {} ({})'.format(it, tot, len(data)))
		url = 'https://api-mainnet.magiceden.dev/v2/tokens/{}'.format(m)
		r = requests.get(url, headers=headers)
		j = r.json()
		data.append(j)
		pass
	df = pd.DataFrame(data)
	df.head()[['collection','mintAddress']]
	df.to_csv('~/Downloads/tmp.csv', index=False)
	need = df.groupby(['collection','updateAuthority']).mintAddress.count().reset_index().sort_values('mintAddress', ascending=0)
	need = need[need.mintAddress > 1].rename(columns={'updateAuthority':'update_authority'})
	need.to_csv('~/Downloads/missing.csv', index=False)
	need.head()
	sorted(need.collection.unique())
	need['collection'] = need.collection.apply(lambda x: re.sub('_', ' ', x.title()).strip() )
	need['collection'] = need.collection.apply(lambda x: re.sub('\|', '-', x).strip() )
	need['collection'] = need.collection.apply(lambda x: re.sub('\)', '', x).strip() )
	need['collection'] = need.collection.apply(lambda x: re.sub('\(', '', x).strip() )
	need['collection'] = need.collection.apply(lambda x: re.sub('\'', '', x).strip() )

	us = sorted(g[g.mintAddress > 1].updateAuthority.unique())
	tot = len(us)
	it = 0
	for u in us:
		it += 1
		print('#{} / {} ({})'.format(it, tot, len(data)))

		nfts = BLOCKCHAIN_API_RESOURCE.search_nfts(
			update_authority = u
			, update_authority_search_method = SearchMethod.EXACT_MATCH
		)
		print(u, len(nfts))
		for n in nfts:
			m = n['nft_metadata']
			data += [[ m['update_authority'], m['mint'], m['data']['symbol'], m['data']['name'] ]]

def manual_tags():
	d = {
		'daaLrDfvcT4joui5axwR2gCkGAroruJFzyVsacU926g': 'Degenerate Ape Kindergarten'
		, 'FbfGrZ3LKuGSsayK57DetzzyN7qKeNnDuLMu5bBSocwF': 'Botheads'
	}
	a = 'FbfGrZ3LKuGSsayK57DetzzyN7qKeNnDuLMu5bBSocwF'
	c = 'Botheads'
	labels = pd.DataFrame()
	for a, c in d.items():
		query = '''
			SELECT DISTINCT instructions[1]:parsed:info:mint::string AS mint_address
			FROM solana.fact_transactions
			WHERE instructions[1]:parsed:info:mintAuthority = '{}'
		'''.format(a)
		df = ctx.cursor().execute(query)
		df = pd.DataFrame.from_records(iter(df), columns=[x[0] for x in df.description])
		df = clean_colnames(df)
		df['collection'] = c
		labels = labels.append(df)
	labels.to_csv('~/Downloads/manual_labels.csv', index=False)

def mints_from_me():
	##################################
	#     Get All ME Collections     #
	##################################
	headers = {
		'Authorization': 'Bearer 9c39e05c-db3c-4f3f-ac48-84099111b813'
	}
	data = []
	has_more = 1
	offset = 0
	while has_more:
		sleep(1)
		print(offset)
		url = 'https://api-mainnet.magiceden.dev/v2/collections?offset={}&limit=500'.format(offset)
		r = requests.get(url)
		j = r.json()
		data = data + j
		has_more = len(j)
		offset += 500
	df = pd.DataFrame(data)
	df.to_csv('./data/me_collections.csv', index=False)
	df = pd.read_csv('./data/me_collections.csv')

	# lp_data = []
	# has_more = 1
	# offset = 0
	# while has_more:
	# 	sleep(1)
	# 	print(offset)
	# 	url = 'https://api-mainnet.magiceden.dev/v2/launchpad/collections?offset={}&limit=500'.format(offset)
	# 	r = requests.get(url)
	# 	j = r.json()
	# 	lp_data = lp_data + j
	# 	has_more = len(j)
	# 	offset += 500
	# lp_df = pd.DataFrame(lp_data)
	# lp_df.to_csv('./data/me_lp_collections.csv', index=False)
	# lp_df = pd.read_csv('./data/me_lp_collections.csv')

	###########################################
	#     Get 1 Mint From Each Collection     #
	###########################################
	it = 0
	l_data = []
	old_l_df = pd.read_csv('./data/me_mints.csv')
	seen = list(old_l_df.symbol.unique())
	df = df[ -df.symbol.isin(seen) ]
	df = df.sort_values('symbol')
	for row in df.iterrows():
		it += 1
		row = row[1]
		print('Listings on {}...'.format(row['symbol']))
		url = 'https://api-mainnet.magiceden.dev/v2/collections/{}/activities?offset=0&limit=1'.format(row['symbol'])
		if row['symbol'] in seen:
			print('Seen')
			continue
		try:
			r = requests.get(url, headers=headers)
			j = r.json()
		except:
			print('Re-trying in 10s')
			sleep(10)
			try:
				r = requests.get(url, headers=headers)
				j = r.json()
			except:
				print('Re-trying in 60s')
				sleep(60)
				r = requests.get(url, headers=headers)
				j = r.json()
		if len(j):
			l_data += [[ row['symbol'], row['name'], j[0]['tokenMint'] ]]
		if it % 10 == 0:
			print('it#{}: {}'.format(it, len(l_data)))
			l_df = pd.DataFrame(l_data, columns=['symbol','name','mint_address'])
			l_df.to_csv('./data/me_mints.csv', index=False)
	l_df = pd.DataFrame(l_data, columns=['symbol','name','mint_address'])
	l_df = l_df.append(old_l_df).drop_duplicates(subset=['symbol'])
	print('Adding {} rows to me_mints'.format(len(l_df) - len(old_l_df)))
	l_df.to_csv('./data/me_mints.csv', index=False)

	# it = 0
	# l_data = []
	# seen = [ x[0] for x in l_data ]
	# print(len(seen))
	# for row in df.iterrows():
	# 	it += 1
	# 	row = row[1]
	# 	print('Listings on {}...'.format(row['symbol']))
	# 	url = 'https://api-mainnet.magiceden.dev/v2/collections/{}/listings?offset=0&limit=1'.format(row['symbol'])
	# 	if row['symbol'] in seen:
	# 		print('Seen')
	# 		continue
	# 	try:
	# 		r = requests.get(url)
	# 		j = r.json()
	# 	except:
	# 		print('Re-trying in 10s')
	# 		sleep(10)
	# 		try:
	# 			r = requests.get(url)
	# 			j = r.json()
	# 		except:
	# 			print('Re-trying in 60s')
	# 			sleep(60)
	# 			r = requests.get(url)
	# 			j = r.json()
	# 	if len(j):
	# 		l_data += [[ row['symbol'], row['name'], j[0]['tokenMint'] ]]
	# 	if it % 10 == 0:
	# 		print('it#{}: {}'.format(it, len(l_data)))
	# 		l_df = pd.DataFrame(l_data, columns=['symbol','name','mint_address'])
	# 		l_df.to_csv('./data/me_mints.csv', index=False)
	# l_df = pd.DataFrame(l_data, columns=['symbol','name','mint_address'])
	# l_df.to_csv('./data/me_mints.csv', index=False)

	# get missing collections
	query = '''
		WITH base AS (
			SELECT block_timestamp::date AS date
			, s.*
			, ROW_NUMBER() OVER (ORDER BY sales_amount DESC) AS rn
			FROM solana.fact_nft_sales s
			LEFT JOIN solana.dim_labels l on s.mint = l.address
			WHERE marketplace in ('magic eden v1', 'magic eden v2') 
			AND block_timestamp >= '2022-01-01' 
			AND block_timestamp <= '2022-05-20' 
			AND l.address IS NULL
			AND sales_amount > 20
		)
		SELECT *
		FROM base
		WHERE rn % 50 = 1
		LIMIT 100
	'''
	missing = ctx.cursor().execute(query)
	missing = pd.DataFrame.from_records(iter(missing), columns=[x[0] for x in missing.description])
	missing = clean_colnames(missing)

	######################################################
	#     Get Update Authorities For All Collections     #
	######################################################
	l_df = pd.read_csv('./data/me_mints.csv')
	len(l_df)
	l_df.head()
	m_old = pd.read_csv('./data/me_update_authorities.csv')
	m_old['seen'] = 1
	m_data = list(m_old[['symbol','name','update_authority']].values)
	seen = [ x[0] for x in m_data ]
	print('Seen {} m_data'.format(len(seen)))
	l_df = l_df[-l_df.symbol.isin(seen)]
	l_df = l_df.sort_values('symbol')
	it = 0
	for row in l_df.iterrows():
		sleep(.5)
		it += 1
		row = row[1]
		symbol = row['symbol']
		print('Working on {}...'.format(symbol))
		if symbol in seen:
			print('Seen')
			continue
		url = 'https://api-mainnet.magiceden.dev/v2/tokens/{}'.format(row['mint_address'])
		try:
			r = requests.get(url, headers=headers)
			j = r.json()
		except:
			print('Re-trying in 10s')
			sleep(10)
			try:
				r = requests.get(url, headers=headers)
				j = r.json()
			except:
				print('Re-trying in 60s')
				sleep(60)
				r = requests.get(url, headers=headers)
				j = r.json()
		if 'updateAuthority' in j.keys():
			m_data += [[ row['symbol'], row['name'], j['updateAuthority'] ]]
		if it % 10 == 0:
			print('it#{}: {}'.format(it, len(m_data)))
			m_df = pd.DataFrame(m_data, columns=['symbol','name','update_authority'])
			m_df.to_csv('./data/me_update_authorities.csv', index=False)
	m_df = pd.DataFrame(m_data, columns=['symbol','name','update_authority'])
	m_df = m_df.drop_duplicates()
	print('Adding {} rows to me_update_authorities'.format(len(m_df) - len(m_old)))
	m_df.to_csv('./data/me_update_authorities.csv', index=False)
	m_df.tail(134).head(20)
	m_df = m_df.tail(134)


	query = '''
		SELECT DISTINCT project_name, LOWER(project_name) AS lower_name
		FROM crosschain.address_labels
		WHERE blockchain = 'solana'
		AND label_subtype = 'nf_token_contract'
		AND project_name IS NOT NULL
	'''

	labels = ctx.cursor().execute(query)
	labels = pd.DataFrame.from_records(iter(labels), columns=[x[0] for x in labels.description])
	labels = clean_colnames(labels)
	labels.to_csv('~/Downloads/tmp-la.csv', index=False)

	######################################################
	#     Get Update Authorities For All Collections     #
	######################################################
	m_df = pd.read_csv('./data/me_update_authorities.csv')
	m_df['seen'] = (-m_df.name.isin(m_df.name.tail(134).values)).astype(int)
	m_df['lower_name'] = m_df.name.apply(lambda x: x.lower() )
	seen = list(labels.lower_name.unique())
	m_df['seen'] = m_df.lower_name.isin(seen).astype(int)
	n_auth = m_df.groupby('update_authority').name.count().reset_index().rename(columns={'name':'n_auth'})
	m_df = m_df.merge(n_auth)
	len(m_df[m_df.seen == 0])
	len(m_df[ (m_df.seen == 0) & (m_df.n_auth == 1)])
	len(m_df[ (m_df.seen == 0) & (m_df.n_auth > 1)])

	m_df.to_csv('~/Downloads/tmp-m_df.csv', index=False)
	len(m_df.name.unique())

	need = list(m_df[m_df.seen == 0].update_authority.unique())
	need = list(m_df[ (m_df.seen == 0) & (m_df.n_auth == 1) ].update_authority.unique())
	len(need)
	# need = need + [
	# need = [
	# 	'CDgbhX61QFADQAeeYKP5BQ7nnzDyMkkR3NEhYF2ETn1k' # taiyo
	# 	, 'DC2mkgwhy56w3viNtHDjJQmc7SGu2QX785bS4aexojwX' # DAA
	# 	, 'daaLrDfvcT4joui5axwR2gCkGAroruJFzyVsacU926g' # Degen Egg
	# 	, 'BL5U8CoFPewr9jFcKf3kE1BhdFS1J59cwGpeZrm7ZTeP' # Skullbot
	# 	, 'DRGNjvBvnXNiQz9dTppGk1tAsVxtJsvhEmojEfBU3ezf' # Boryoku
	# 	, '7hYkx2CNGRB8JE7X7GefX1ak1dqe7GxgYKbpfj9moE9D' # mindfolk
	# 	, 'CjwNEVQFKk8YzZLCvvw6sNrjxiQW8dYDSzhTph18T7g5' # jelly rascals
	# 	, 'EcxEqUj4RNgdGJwPE3ktsM99Ea9ThPmXHUV5g37Qm4ju' # women monkey
	# 	, 'EQSoRhbN9fEEYXKEE5Lg63Mqf17P3JydcWTvDhdMJW1N' # hydrascripts
	# 	, '75CPiM9ywLgxhii9SQsNoA1SH3h66o5EhrYsazHR5Tqk' # hydrascripts
	# 	, 'aury7LJUae7a92PBo35vVbP61GX8VbyxFKausvUtBrt' # aurory
	# 	, 'ET3LWbEL6q4aUSjsX5xLyWktCwqKh6qsQE5j6TDZtZBY' # enviropass
	# 	, '8ERR2gYrvXcJFuoNAbPRvHXtrJnAXXHgXKkVviwz9R6C' # enviroPass
	# 	, 'GRDCbZBP1x2JxYf3rQQoPFGzF57LDPy7XtB1gEMaCqGV' # Space Robots
	# 	, 'GenoS3ck8xbDvYEZ8RxMG3Ln2qcyoAN8CTeZuaWgAoEA' # Genopet
	# 	, 'STEPNq2UGeGSzCyGVr2nMQAzf8xuejwqebd84wcksCK' # stepn
	# 	, 'HcS8iaEHwUino8wKzcgC16hxHodnPCyacVYUdBaSZULP' # BASC
	# 	, 'AvkbtawpmMSy571f71WsWEn41ATHg5iHw27LoYJdk8QA' # THUG
	# 	, 'GH4QhJznKEHHv44AqEH5SUohkUauWyAFtu5u8zUWUKL4' # StepN Shoebox
	# 	, 'FTQmhcD7SNBWrVxTgQMFr7xL2aA6adfAJJPBxGKU4VsZ' # Solstien
	# ]
	need = m_df[m_df.update_authority.isin(need)]

	# m_df[m_df.lower_name.isin(seen)]
	# m_df[-m_df.lower_name.isin(seen)]
	# tmp = m_df[['update_authority','collection']].drop_duplicates().groupby(['update_authority']).collection.count().reset_index().rename(columns={'collection':'n_collection'})
	# tmp = tmp.sort_values('n_collection', ascending=0)
	# m_df = m_df.merge(tmp)
	# m_df = m_df.sort_values(by=['n_collection','update_authority','collection'], ascending=[0,0,0])
	l_df = pd.read_csv('./data/me_mints.csv')
	fix = need.merge(l_df[[ 'name','mint_address' ]])
	# len(need.name.unique())
	# len(fix.name.unique())
	# fix = fix.sort_values(by=['update_authority','collection'], ascending=[0,0])
	# fix.head()


	# seen = []
	# data = []
	# meta = []

	# fix = fix[-(fix.name.isin(seen))]
	# start = time.time()
	# it = 0
	# tot = len(fix)
	# scraper = cloudscraper.create_scraper()
	# # for each collection
	# for row in fix.iterrows():
	# 	row = row[1]
	# 	print(row['name'])
	# 	if row['name'] in seen:
	# 		print('Seen')
	# 		continue
	# 	url = 'https://api.solscan.io/nft/detail?mint={}'.format(row['mint_address'])
	# 	t = scraper.get(url).text
	# 	j = json.loads(t)
	# 	# r = requests.get(url)
	# 	# j = r.json()
	# 	j['data']
	# 	if not j['success']:
	# 		print('Error')
	# 		print(r)
	# 		print(j)
	# 		sleep(1)
	# 		continue
	# 	update_authority = j['data']['updateAuthority']
	# 	collectionId = j['data']['collectionId']
	# 	collection = j['data']['collection']
	# 	offset = 0
	# 	limit = 500
	# 	while True:
	# 		print(offset)
	# 		url = 'https://api.solscan.io/collection/nft?sortBy=nameDec&collectionId={}&offset={}&limit={}'.format(collectionId, offset, limit)
	# 		r = requests.get(url)
	# 		js = r.json()['data']
	# 		offset += limit
	# 		if len(js) == 0:
	# 			break
	# 		for j in js:
	# 			data += [[ update_authority, collectionId, collection, row['symbol'], row['name'], row['collection'], j['info']['mint'] ]]
	# 			m = j['info']['meta']
	# 			m['mint_address'] = j['info']['mint']
	# 			m['name'] = row['name']
	# 			m['update_authority'] = update_authority
	# 			meta += [ m ]
	# 	it += 1
	# 	end = time.time()
	# 	print('Finished {} / {} in {} minutes'.format(it, tot, round((end - start) / 60.0, 1)))
	
	# old = pd.read_csv('./data/nft_label_tokens.csv')
	# token_df = pd.DataFrame(data, columns=['update_authority','collectionId','solscan_collection','symbol','name','collection','mint'])
	# token_df = token_df.append(old).drop_duplicates()
	# token_df.to_csv('./data/nft_label_tokens.csv', index=False)

	# old = pd.read_csv('./data/nft_label_metadata.csv')
	# meta_df = pd.DataFrame(meta)
	# meta_df = meta_df.append(old).drop_duplicates()
	# meta_df.to_csv('./data/nft_label_metadata.csv', index=False)
	# seen = list(token_df.name.unique())

	# m_df.to_csv('~/Downloads/tmp.csv', index=False)
	# tmp[tmp.collection > 1]
	# m_df.head()
	# def f(x):
	# 	x = re.sub('\(|\)', '', x)
	# 	x = re.sub(' ', '_', x)
	# 	x = re.sub('\'', '', x)
	# 	return(x)
	# m_df['collection'] = m_df.name.apply(lambda x: f(x) )

	# x = 'asf (asf)'
	# f(x)

	# query = '''
	# 	WITH base AS (
	# 	SELECT *
	# 	, ROW_NUMBER() OVER (PARTITION BY project_name ORDER BY insert_date DESC) AS rn
	# 	FROM crosschain.address_labels
	# 	WHERE blockchain = 'solana'
	# 	AND label_subtype = 'nf_token_contract'
	# 	)
	# 	SELECT *
	# 	FROM base
	# '''

	# examples = ctx.cursor().execute(query)
	# examples = pd.DataFrame.from_records(iter(examples), columns=[x[0] for x in examples.description])
	# examples = clean_colnames(examples)
	# examples.head()
	# examples[examples.address_name == 'paradisedao'].head()
	# examples[examples.address == 'GUXSatf5AAFKmuQgSgn4GoGzBEhwJ9WAQRxeVt1vZvkb'].head()
	# # m_df = pd.read_csv('./data/me_update_authorities.csv')
	# # fix = m_df[m_df.n_collection > 1].merge(examples[[ 'address','address_name' ]].rename(columns={'address_name':'name'}) )
	# fix = m_df[m_df.n_collection > 1].merge(examples[[ 'address','address_name' ]].rename(columns={'address_name':'name'}) )
	# len(m_df[m_df.n_collection > 1].name.unique())
	# len(fix.name.unique())

	# j = list(fix.address.unique())
	# with open('./data/fix_mints.json', 'w') as f:
	# 	json.dump(j, f)
	
	# seen = list(examples.address.unique())
	# seen = []
	# need = df[-df.mint_address.isin(seen)].sort_values(['collection','mint_address'])
	# CDgbhX61QFADQAeeYKP5BQ7nnzDyMkkR3NEhYF2ETn1k - taiyo
	# DC2mkgwhy56w3viNtHDjJQmc7SGu2QX785bS4aexojwX - DAA
	# DRGNjvBvnXNiQz9dTppGk1tAsVxtJsvhEmojEfBU3ezf - Boryoku
	# 7hYkx2CNGRB8JE7X7GefX1ak1dqe7GxgYKbpfj9moE9D - mindfolk
	# CjwNEVQFKk8YzZLCvvw6sNrjxiQW8dYDSzhTph18T7g5 - mindfolk
	need = fix.copy().rename(columns={'name':'collection'})
	# need = need.drop_duplicates(subset=['update_authority']).sort_values('collection').head(7).tail(1)
	need = need.drop_duplicates(subset=['update_authority']).sort_values('collection')
	need['collection'] = need.collection.apply(lambda x: re.sub('\|', '-', x).strip() )
	need['collection'] = need.collection.apply(lambda x: re.sub('\)', '', x).strip() )
	need['collection'] = need.collection.apply(lambda x: re.sub('\(', '', x).strip() )
	need['collection'] = need.collection.apply(lambda x: re.sub('\'', '', x).strip() )
	need.collection.unique()
	# need = need.drop_duplicates(subset=['collection']).sort_values('collection')
	n = 0
	# 1310 - 310
	# need = need.tail(n).head(300).tail(25)
	# need = need.tail(1009).head(17)
	# need = need.tail(1009 - 17).head(17)
	# 1-285, 1310-975
	len(need)
	# print(n)

	mfiles = ['/data/mints/{}/{}_mint_accounts.json'.format(re.sub(' |-', '_', collection), update_authority) for collection, update_authority in zip(need.collection.values, need.update_authority.values) ]
	seen = [ x for x in mfiles if os.path.exists(x) ]
	seen = []

	# for update authorities that have only 1 collection, we can just check metaboss once
	rpc = 'https://red-cool-wildflower.solana-mainnet.quiknode.pro/a1674d4ab875dd3f89b34863a86c0f1931f57090/'
	# need = need.tail(400)
	it = 0
	tot = len(need)
	for row in need.iterrows():
		it += 1
		row = row[1]
		collection = row['collection']
		print('#{} / {}: {}'.format(it, tot, collection))
		# if collection in seen:
		# 	continue
		update_authority = row['update_authority']
		# print('Working on {}...'.format(collection))
		collection_dir = re.sub(' |-', '_', collection)

		dir = './data/mints/{}/'.format(collection_dir)
		mfile = '{}{}_mint_accounts.json'.format(dir, update_authority)
		if not os.path.exists(dir):
			print(collection)
			os.makedirs(dir)
		# elif len(os.listdir(dir)) and os.path.exists(mfile):
		# 	print('Already have {}.'.format(collection))
		# 	print('Seen')
		# 	continue
		seen.append(update_authority)
		os.system('metaboss -r {} -t 300 snapshot mints --update-authority {} --output {}'.format(rpc, update_authority, dir))

	# write the mints to csv
	data = []
	for path in os.listdir('./data/mints/'):
		if os.path.isdir('./data/mints/'+path):
			collection = re.sub('_', ' ', path).strip()
			for fname in os.listdir('./data/mints/'+path):
				f = './data/mints/'+path+'/'+fname
				if os.path.isfile(f) and '.json' in f:
					with open(f) as file:
						j = json.load(file)
						for m in j:
							data += [[ collection, m ]]
	df = pd.DataFrame(data, columns=['collection','mint_address'])
	df.collection.unique()
	df.to_csv('./data/single_update_auth_labels.csv', index=False)

	################################
	#     Multiple Authorities     #
	################################
	rpc = 'https://red-cool-wildflower.solana-mainnet.quiknode.pro/a1674d4ab875dd3f89b34863a86c0f1931f57090/'
	need = list(m_df[ (m_df.seen == 0) & (m_df.n_auth > 1) ].update_authority.unique())
	need = m_df[m_df.update_authority.isin(need)]
	fix = need.merge(l_df[[ 'name','mint_address' ]])
	need = fix.copy().rename(columns={'name':'collection'})
	need = need.sort_values('collection').drop_duplicates(subset=['update_authority'], keep='first')
	i = 5
	sz = 112
	t = len(need) - (sz * (i - 1)) if sz * i > len(need) else sz
	print(t)
	need = need.head(sz * i).tail(t)
	# need = need.head(150 * 2).tail(150)
	# need = need.head(150 * 3).tail(150)
	# need = need.head(150 * 4).tail(150)
	need['collection'] = need.collection.apply(lambda x: re.sub('\|', '-', x).strip() )
	need['collection'] = need.collection.apply(lambda x: re.sub('\)', '', x).strip() )
	need['collection'] = need.collection.apply(lambda x: re.sub('\(', '', x).strip() )
	need['collection'] = need.collection.apply(lambda x: re.sub('\'', '', x).strip() )
	need.collection.unique()
	it = 0
	a = []
	print(i)
	for row in need.iterrows():
		it += 1
		# if it < 20:
		# 	continue
		# if it % 100 == 0:
		# 	print('#{}/{}'.format(it, len(m_df)))
		print('#{}/{}'.format(it, len(need)))
		row = row[1]
		collection = row['collection']
		if collection in seen:
			continue
		update_authority = row['update_authority']
		print('Working on {}...'.format(collection))
		collection_dir = re.sub(' |-', '_', collection)

		dir = './data/mints/{}/'.format(collection_dir)
		mfile = '{}{}_mint_accounts.json'.format(dir, update_authority)
		if not os.path.exists(dir):
			print(collection)
			os.makedirs(dir)
		# elif len(os.listdir(dir)) and os.path.exists(mfile):
		# 	print('Already have {}.'.format(collection))
		# 	print('Seen')
		# 	continue
		print('LETS GOOO')
		a.append(update_authority)
		os.system('metaboss -r {} -t 300 snapshot mints --update-authority {} --output {}'.format(rpc, update_authority, dir))

		# len(need)
		# len(need.drop_duplicates(subset=['mint_address']))
		# len(need.collection.unique())
		# tot = len(need.collection.unique())
		# it = 0
		# # for each collection, get all the mints from metaboss
		# for c in need.collection.unique():
		# it += 1
		# print('#{} / {}: {}'.format(it, tot, c))
		# dir = './data/fix_labels_1/{}/'.format(re.sub(' ', '_', c))
		odir = dir+'output/'
		# if not os.path.exists(dir):
		# 	print('Making dir {}'.format(dir))
		# 	os.makedirs(dir)
		if not os.path.exists(odir):
			print('Making dir {}'.format(odir))
			os.makedirs(odir)
		# elif os.path.exists(dir+'mints.json'):
		# 	print('Already Seen')
		# 	continue
		# ms = list(need[need.collection == c].mint_address.unique())
		# with open(dir+'mints.json', 'w') as f:
		# 	json.dump(ms, f)
		os.system('metaboss -r {} -t 300 decode mint --list-file {} --output {}'.format(rpc, mfile, odir ))

	##################################################
	#     Load All The Mints for Each Collection     #
	##################################################
	# now that we have the mints, create a data frame with the info for each mint in each collection
	data = []
	seen = [ x[1] for x in data ]
	it = 0
	dirs = os.listdir('./data/mints/')
	for path in dirs:
		print(it)
		it += 1
		if os.path.isdir('./data/mints/'+path):
			collection = re.sub('_', ' ', path).strip()
			if not os.path.exists('./data/mints/'+path+'/output/'):
				continue
			fnames = os.listdir('./data/mints/'+path+'/output/')
			print(collection, len(fnames))
			for fname in fnames:
				f = './data/mints/'+path+'/output/'+fname
				if fname[:-5] in seen:
					continue
				if os.path.isfile(f) and '.json' in f:
					try:
						with open(f) as file:
							j = json.load(file)
							data += [[ collection, fname, j['name'], j['symbol'], j['uri'] ]]
					except:
						print('Error {}'.format(fname[:-5]))
	
	##################################################
	#     Load All The Mints for Each Collection     #
	##################################################
	new_mints = pd.DataFrame(data, columns=['collection','mint_address','name','symbol','uri'])
	# tmp = tmp[-(tmp.collection.isin(['Dskullys','Decimusdynamics']))]
	n = len(new_mints[(new_mints.uri.isnull()) | (new_mints.uri == '')])
	tot = len(new_mints)
	pct = round(n * 100 / tot, 1)
	print('{} ({}%) rows have no uri'.format(n, pct))
	new_mints = new_mints[new_mints.uri != '']

	# function to clean the name of each NFT (remove the number)
	def f_cn(x):
		if not x or x != x:
			return(x)
		if '#' in x[-6:]:
			x = ''.join(re.split('#', x)[:-1]).strip()
		elif bool(re.match('.+\s+[0-9]+', x)):
			x = ' '.join(re.split(' ', x)[:-1]).strip()
		return(x)
	new_mints['clean_name'] = new_mints.name.apply(lambda x: f_cn(x) )

	# determine for each collection if we should look at collection-name-symbol, collection-symbol, or just collection to determine what collection it actuallly belongs to
	# this logic is because e.g. some only have a few names in the collection so we can iterate, but some have a different name for each NFT, so we assume its the same collection for all
	a = new_mints.drop_duplicates(subset=['collection','clean_name','symbol']).groupby(['collection']).uri.count().reset_index().sort_values('uri', ascending=0)
	symbol_only = a[a.uri > 10].collection.unique()
	b = new_mints[new_mints.collection.isin(symbol_only)].drop_duplicates(subset=['collection','symbol']).groupby(['collection']).uri.count().reset_index().sort_values('uri', ascending=0)
	collection_only = b[b.uri > 10].collection.unique()

	# now get the info for each collection-name-symbol combo
	g1 = new_mints[ (-(new_mints.collection.isin(symbol_only))) & (-(new_mints.collection.isin(collection_only))) ].groupby(['collection','clean_name','symbol']).head(1).reset_index()
	g2 = new_mints[ ((new_mints.collection.isin(symbol_only))) & (-(new_mints.collection.isin(collection_only))) ].groupby(['collection','symbol']).head(1).reset_index()
	g3 = new_mints[ (-(new_mints.collection.isin(symbol_only))) & ((new_mints.collection.isin(collection_only))) ].groupby(['collection']).head(1).reset_index()
	g = g1.append(g2).append(g3).drop_duplicates(subset=['mint_address'])
	print('{} Total: {} all, {} collection-symbol {} collection'.format(len(g), len(g1), len(g2), len(g3)))
	g.to_csv('~/Downloads/tmp-g.csv', index=False)

	# iterate over each row to get what collection they are actually in
	# by pulling data from the uri
	uri_data = []
	it = 0
	tot = len(g)
	print(tot)
	errs = []
	seen = [ x['uri'] for x in uri_data ]
	# for row in g.iterrows():
	for row in g[ -(g.uri.isin(seen)) ].iterrows():
		row = row[1]
		it += 1
		if it % 100 == 0:
			uri_df = pd.DataFrame(uri_data)[[ 'collection','name','symbol','row_symbol','row_collection','uri','row_clean_name','mint_address' ]]
			uri_df.to_csv('~/Downloads/uri_df.csv', index=False)
		print('#{} / {}: {}'.format(it, tot, row['collection']))
		try:
			r = requests.get(row['uri'])
			j = r.json()
			j['uri'] = row['uri']
			j['row_collection'] = row['collection']
			j['row_clean_name'] = row['clean_name']
			j['row_symbol'] = row['symbol']
			j['mint_address'] = row['mint_address']
			uri_data += [j]
		except:
			print('Error')
			errs.append(row)
	uri_df = pd.DataFrame(uri_data)[[ 'collection','name','symbol','row_symbol','row_collection','uri','row_clean_name','mint_address' ]]
	uri_df.to_csv('~/Downloads/uri_df.csv', index=False)

	# for each row, parse the json from the uri
	uri_df = pd.read_csv('~/Downloads/uri_df.csv')
	def f(x, c):
		x = str(x)
		try:
			n = json.loads(re.sub("'", "\"", x))[c]
			if type(n) == list:
				return(n[0])
			return(n)
		except:
			try:
				return(json.loads(re.sub("'", "\"", x))[c])
			except:
				try:
					return(json.loads(re.sub("'", "\"", x))[0][c])
				except:
					try:
						return(json.loads(re.sub("'", "\"", x))[0])
					except:
						return(x)
	# parse the json more
	uri_df['parsed_collection'] = uri_df.collection.apply(lambda x: f(x, 'name') )
	uri_df['parsed_family'] = uri_df.collection.apply(lambda x: f(x, 'family') )
	uri_df['clean_name'] = uri_df.name.apply( lambda x: f_cn(x) )
	# calculate what the collection name is
	uri_df['use_collection'] = uri_df.parsed_collection.replace('', None).fillna( uri_df.clean_name )#.fillna( uri_df.row_symbol )
	uri_df[uri_df.use_collection == 'nan'][['use_collection','parsed_collection','parsed_family','clean_name','name','collection','symbol','row_symbol','row_collection']].head()
	uri_df[uri_df.use_collection == 'nan'][['use_collection','parsed_collection','parsed_family','clean_name','name','collection','symbol','row_symbol','row_collection']].to_csv('~/Downloads/tmp.csv', index=False)
	len(uri_df)

	# clean the collection name
	def f1(x):
		try:
			if len(x['use_collection']) == 1:
				return(x['clean_name'])
			if bool(re.match('.+\s+#[0-9]+', x['use_collection'])):
				return(''.join(re.split('#', x['use_collection'])[:-1]).strip())
			if '{' in x['use_collection']:
				return(x['clean_name'])
			return(x['use_collection'].strip().title())
		except:
			return(x['use_collection'].strip().title())
	uri_df['tmp'] = uri_df.apply(lambda x: f1(x), 1 )
	uri_df[uri_df.tmp == 'Nan']['use_collection','tmp']
	uri_df['use_collection'] = uri_df.apply(lambda x: f1(x), 1 )
	sorted(uri_df.use_collection.unique())[:20]
	sorted(uri_df.use_collection.unique())[-20:]

	# clean the mint_address
	uri_df['mint_address'] = uri_df.mint_address.apply(lambda x: re.sub('.json','', x))
	uri_df.head()
	uri_df = uri_df.fillna('None')

	for i in range(2):
		# for each collection-name-symbol combo, see how many have multiple mappings
		a = uri_df.copy().fillna('None')
		a = a[['row_collection','row_clean_name','row_symbol','use_collection']].drop_duplicates().groupby(['row_collection','row_clean_name','row_symbol']).use_collection.count().reset_index().rename(columns={'use_collection':'n_1'})
		uri_df = merge(uri_df, a, ensure=True)

		# for each collection-symbol combo, see how many have multiple mappings
		a = uri_df.copy().fillna('None')
		a = a[['row_collection','row_symbol','use_collection']].drop_duplicates().groupby(['row_collection','row_symbol']).use_collection.count().reset_index().rename(columns={'use_collection':'n_2'})
		uri_df = merge(uri_df, a, ensure=True)

		# for each collection combo, see how many have multiple mappings
		a = uri_df.copy().fillna('None')
		a = a[['row_collection','use_collection']].drop_duplicates().groupby(['row_collection']).use_collection.count().reset_index().rename(columns={'use_collection':'n_3'})
		uri_df = merge(uri_df, a, ensure=True)

		uri_df['n'] = uri_df.apply(lambda x: x['n_3'] if x['row_collection'] in collection_only else x['n_2'] if x['row_collection'] in symbol_only else x['n_1'], 1 )
		print('{} / {} ({}%) have multiple collection-name-symbol mappings'.format(len(uri_df[uri_df.n > 1]), len(uri_df), round( 100.0 * len(uri_df[uri_df.n > 1]) / len(uri_df))))

		# if there is multiple, use the parsed_family instead of the use_collection
		uri_df['use_collection'] = uri_df.apply(lambda x: x['use_collection'] if x['n'] == 1 else x['parsed_family'], 1 )
		del uri_df['n_1']
		del uri_df['n_2']
		del uri_df['n_3']

	# only take rows where there is a single mapping
	m = uri_df[uri_df.n==1][[ 'use_collection','row_collection','row_clean_name','row_symbol' ]].dropna().drop_duplicates()
	m.columns = [ 'use_collection','collection','clean_name','symbol' ]

	m_1 = new_mints[ (-(new_mints.collection.isin(symbol_only))) & (-(new_mints.collection.isin(collection_only)))  ].fillna('').merge(m.fillna(''), how='left')
	m_2 = new_mints[ ((new_mints.collection.isin(symbol_only))) & (-(new_mints.collection.isin(collection_only)))  ][[ 'collection','mint_address','symbol' ]].fillna('').merge(m.fillna(''), how='left')
	m_3 = new_mints[ (-(new_mints.collection.isin(symbol_only))) & ((new_mints.collection.isin(collection_only)))  ][[ 'collection','mint_address' ]].fillna('').merge(m.fillna(''), how='left')
	len(m_1) + len(m_2) + len(m_3)
	len(new_mints)
	# m = new_mints.fillna('').merge(m.fillna(''), how='left')
	m = m_1.append(m_2).append(m_3)
	print('After all this, we have {}% of the mints'.format( round(len(m) * 100 / len(new_mints)) ))
	len(new_mints)
	len(m)
	m['mint_address'] = m.mint_address.apply(lambda x: re.sub('.json', '', x) )
	m = m[['mint_address','use_collection']].dropna().drop_duplicates()
	m.columns = ['mint_address','collection']

	m[m.collection.isnull()].head()
	m[m.collection=='Nan'].head()

	m = m[m.collection != 'Nan']

	tmp = m.groupby('collection').mint_address.count().reset_index().sort_values('mint_address', ascending=0)
	tmp.head()

	m.to_csv('./data/mult_update_auth_labels.csv', index=False)
	################
	#     DONE     #
	################



	tokens = m.append(pd.read_csv('./data/tokens.csv')[['collection','mint_address']]).drop_duplicates(subset=['mint_address'], keep='last')
	tokens.to_csv('./data/mints-2022-06-13-2pm.csv', index=False)

	tokens.head()
		
	m.to_csv('./data/mints-2022-06-09.csv', index=False)
	m = pd.read_csv('./data/mints-2022-06-09.csv')
	m.groupby('collection').head(1).to_csv('~/Downloads/tmp.csv', index=False)
	len(m)
	len(m.mint_address.unique())
	m.head()
	m.head()
	# m = m.merge(symbol_map, how='left', on='symbol')
	# m['use_collection'] = m.use_collection_x.fillna(m.use_collection_y)
	len(new_mints)
	len(m)
	len(m[m.use_collection.isnull()])
	len(m[m.use_collection.isnull()]) / len(m)
	len(m[m.use_collection_x.isnull()]) / len(m)
	m[m.use_collection.isnull()].fillna('').drop_duplicates(subset=['collection','clean_name','symbol']).to_csv('~/Downloads/tmp-3.csv', index=False)
	m[m.use_collection.isnull()].drop_duplicates(subset=['collection']).to_csv('~/Downloads/tmp-3.csv', index=False)

	a = uri_df[(uri_df.parsed_collection.isnull()) | (uri_df.parsed_collection == '')].groupby('row_clean_name').uri.count().reset_index()
	a = uri_df[(uri_df.parsed_collection.isnull()) | (uri_df.parsed_collection == '')]
	uri_df.head()
	uri_df['row_clean_name'] = uri_df.row_clean_name.apply(lambda x: f_cn(x) )
	id_map = uri_df
	a.to_csv('~/Downloads/tmp-1.csv', index=False)
	len(uri_df)
	n = uri_df.groupby()
	uri_df
	uri_df
	uri_df.head()
	uri_df[['symbol','collection','']]
	uri_df.head()

	query = '''
		SELECT DISTINCT project_name
		FROM crosschain.address_labels
		WHERE blockchain = 'solana'
		AND label_subtype = 'nf_token_contract'
		AND project_name IS NOT NULL
	'''

	labels = ctx.cursor().execute(query)
	labels = pd.DataFrame.from_records(iter(labels), columns=[x[0] for x in labels.description])
	labels = clean_colnames(labels)

	seen = [ x for x in m_df.collection.unique() if os.path.exists('./data/mints/{}/'.format(x)) and len(os.listdir('./data/mints/{}/'.format(x))) ]
	seen = seen + [ re.sub('_', '', f(x.lower())) for x in labels.project_name.unique() ]
	m_df = m_df[m_df.symbol.notnull()]
	m_df['tmp'] = m_df.name.apply(lambda x: re.sub('_', '', f(x.lower())))
	m_df[m_df.symbol == 'the_last_apes']
	# m_df.to_csv('~/Downloads/tmp.csv', index=False)
	len(m_df[m_df.tmp.isin(seen)])
	[x for x in seen if not x in m_df.tmp.unique()][:11]
	m_df[m_df.symbol == 'apesquad']
	m_df[m_df.symbol == 'chimp_frens']
	url = 'https://api.solscan.io/nft/detail?mint=D5pT5HYPeQkHD6ryoHxnc2jdcUMYmjs6sS6LswbSDsuy'
	us = sorted(m_df[m_df.n_collection > 1].update_authority.unique())
	u = us[1]
	m_df[m_df.update_authority == u]
	m_df[m_df.mint == 'G3xiAFZEp49BJc8nNrDJxwTXZ34teKH7CRf5KTGakxte']
	data = []
	for u in us[:10]:
		nfts = BLOCKCHAIN_API_RESOURCE.search_nfts(
			update_authority = u
			, update_authority_search_method = SearchMethod.EXACT_MATCH
		)
		print(u, len(nfts))
		for n in nfts:
			m = n['nft_metadata']
			data += [[ m['update_authority'], m['mint'], m['data']['symbol'], m['data']['name'] ]]
	nft_df = pd.DataFrame(data, columns=['update_authority','mint','symbol','name'])
	len(nft_df.update_authority.unique())
	nft_df['collection'] = nft_df.name.apply(lambda x: re.split('#', x)[0].strip() )
	nft_df.groupby(['symbol','collection']).mint.count()
	nft_df.groupby(['symbol','name']).mint.count()
	print(len(seen))
	# m_df = m_df.merge(lp_df)
	len(m_df)
	it = 0
	m_df = m_df[(-m_df.tmp.isin(seen)) & (-m_df.collection.isin(seen)) & (-m_df.name.isin(seen))]
	rpc = 'https://red-cool-wildflower.solana-mainnet.quiknode.pro/a1674d4ab875dd3f89b34863a86c0f1931f57090/'
	len(seen)
	for row in m_df.sort_values('collection').iterrows():
		it += 1
		# if it < 20:
		# 	continue
		if it % 100 == 0:
			print('#{}/{}'.format(it, len(m_df)))
		row = row[1]
		collection = row['collection']
		if collection in seen:
			continue
		update_authority = row['update_authority']
		print('Working on {}...'.format(collection))
		collection_dir = re.sub(' ', '_', collection)

		dir = './data/mints/{}/'.format(collection_dir)
		if not os.path.exists(dir):
			os.makedirs(dir)
		elif len(os.listdir(dir)):
			# print('Already have {}.'.format(collection))
			print('Seen')
			continue

		os.system('metaboss -r {} -t 300 snapshot mints --update-authority {} --output {}'.format(rpc, update_authority, dir))
		# os.system('metaboss -r {} -t 300 derive metadata mints --update-authority {} --output {}'.format(rpc, update_authority, dir))

		# fname = os.listdir(dir)
		# if len(fname) == 1:
		# 	fname = dir+fname[0]

		# 	dir_mints = '{}mints/'.format(dir)
		# 	if not os.path.exists(dir_mints):
		# 		os.makedirs(dir_mints)
		# 	os.system('metaboss -r {} -t 300 decode mint --list-file {} --output {}'.format(rpc, fname, dir_mints))

	data = []
	for path in os.listdir('./data/mints/'):
		if os.path.isdir('./data/mints/'+path):
			collection = re.sub('_', ' ', path).strip()
			for fname in os.listdir('./data/mints/'+path):
				f = './data/mints/'+path+'/'+fname
				if os.path.isfile(f) and '.json' in f:
					with open(f) as file:
						j = json.load(file)
						for m in j:
							data += [[ collection, m ]]
	df = pd.DataFrame(data, columns=['collection','mint_address'])
	df = df[df.collection != 'etc']
	# df = df.drop_duplicates(subset='mint_address')
	df = df.drop_duplicates()
	df['n'] = 1
	g = df.groupby(['mint_address']).n.sum().reset_index()
	g = g[g.n > 1]
	len(g)
	tmp_0 = g[['mint_address']].merge(df).groupby('collection').n.count().reset_index().sort_values('n', ascending=0)
	tmp_0.head(20)
	tmp_0.to_csv('~/Downloads/tmp.csv', index=False)
	tmp = g.merge(df[[ 'collection','mint_address' ]])
	tmp = tmp.sort_values(['mint_address','collection'])
	tmp.collection.unique()
	len(tmp.collection.unique())
	len(df.collection.unique())
	rem = tmp.collection.unique()
	df = df[-df.collection.isin(rem)]
	df.to_csv('~/Downloads/solana_nft_tags.csv', index=False)


def mint_address_token_id_map_2():
	old = pd.read_csv('./data/mint_address_token_id_map.csv')
	old = pd.DataFrame()
	mints = pd.read_csv('./data/solana_mints.csv')
	data = []
	for collection in [ 'Stoned Ape Crew','DeGods' ]:
		for m in mints[mints.collection == collection].mint_address.unique():
			pass
			f = open('./data/mints/{}/{}.json'.format(collection, m))
			j = json.load(f)
			try:
				token_id = int(re.split('#', j['name'])[1])
				data += [[ collection, m, token_id, j['uri'] ]]
			except:
				print(m)
		df = pd.DataFrame(data, columns=['collection','mint','token_id','uri'])
		old = old.append(df).drop_duplicates()
		print(old[old.token_id.notnull()].groupby('collection').token_id.count())
	old.to_csv('./data/mint_address_token_id_map.csv', index=False)

def mint_address_token_id_map():
	mints = pd.read_csv('./data/solana_mints.csv')
	mints[mints.collection == 'Stoned Ape Crew'][['mint_address']].drop_duplicates().to_csv('~/Downloads/tmp.csv', index=False)
	mints[mints.collection == 'Degods'][['mint_address']].drop_duplicates().to_csv('~/Downloads/tmp.csv', index=False)
	mints[mints.collection == 'DeGods'][['mint_address']].drop_duplicates().to_csv('~/Downloads/tmp.csv', index=False)
	old = pd.read_csv('./data/mint_address_token_id_map.csv')
	my_file = open('./scripts/solana-rpc-app/output.txt', 'r')
	content = my_file.read()
	my_file.close()
	content_list = content.split('[')
	data = []
	for c in content_list:
		s = re.split(',', c)
		if len(s) > 1 and '#' in s[1]:
			data += [[ re.split('"', s[0])[1], int(re.split('#', re.split('"', s[1])[1])[1]) ]]
	df = pd.DataFrame(data, columns=['mint','token_id']).drop_duplicates()
	df['collection'] = 'DeGods'
	df.to_csv('./data/mint_address_token_id_map.csv', index=False)

def mint_address_token_id_map():
	old = pd.read_csv('./data/mint_address_token_id_map.csv')
	l0 = len(old)
	tokens = pd.read_csv('./data/tokens.csv')[['collection','token_id','mint_address']].rename(columns={'mint_address':'mint'}).dropna()
	tokens['uri'] = None
	tokens = tokens[-tokens.collection.isin(old.collection.unique())]
	old = old.append(tokens)
	print('Adding {} rows'.format(len(old) - l0))
	old.to_csv('./data/mint_address_token_id_map.csv', index=False)

def add_solana_sales():
	print('Adding Solana sales...')

	query = '''
		WITH mints AS (
			SELECT DISTINCT LOWER(mint) AS mint
			, token_id
			, project_name AS collection
			FROM solana.dim_nft_metadata
			WHERE mint IS NOT NULL 
			AND token_id IS NOT NULL
			AND project_name IS NOT NULL
			AND project_name IN (
				'Astrals',
				'Aurory',
				'Cets on Creck',
				'Catalina Whale Mixer',
				'DeFi Pirates',
				'DeGods',
				'Degen Apes',
				'Meerkat Millionaires',
				'Okay Bears',
				'Pesky Penguins',
				'SOLGods',
				'Solana Monkey Business',
				'Stoned Ape Crew',
				'Thugbirdz'
			)
		)
		SELECT tx_id
		, s.mint
		, m.collection
		, s.block_timestamp AS sale_date
		, m.token_id
		, sales_amount AS price
		FROM solana.fact_nft_sales s
		JOIN mints m ON LOWER(m.mint) = LOWER(s.mint)
		WHERE block_timestamp >= CURRENT_DATE - 20
	'''
	sales = ctx.cursor().execute(query)
	sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
	sales = clean_colnames(sales)
	len(sales)
	len(sales.tx_id.unique())

	m = sales[[ 'tx_id','collection','token_id','sale_date','price' ]]
	m['sale_date'] = m.sale_date.apply(lambda x: str(x)[:19] )
	old = pd.read_csv('./data/sales.csv')
	go = old.groupby('collection').token_id.count().reset_index().rename(columns={'token_id':'n_old'})
	l0 = len(old)
	app = old[old.collection.isin(m.collection.unique())].append(m)
	app['tmp'] = app.apply(lambda x: x['collection']+str(int(float(x['token_id'])))+x['sale_date'][:10], 1 )
	if len(app[app.tx_id.isnull()]):
		app['null_tx'] = app.tx_id.isnull().astype(int)
		app = app.sort_values('null_tx', ascending=1)
		app = app.drop_duplicates(subset=['tmp'], keep='first')
		app['tx_id'] = app.tx_id.fillna(app.tmp)
	old = old[-old.collection.isin(m.collection.unique())]
	app = app.drop_duplicates(subset=['tx_id'])
	old = old.append(app)

	old = old[[ 'collection','token_id','sale_date','price','tx_id' ]]
	old['token_id'] = old.token_id.astype(str)
	# old = old.drop_duplicates(subset=['tx_id'])
	# old[old.tx_id.isnull()]

	# check changes
	l1 = len(old)
	gn = old.groupby('collection').token_id.count().reset_index().rename(columns={'token_id':'n_new'})
	g = gn.merge(go, how='outer', on=['collection']).fillna(0)
	g['dff'] = g.n_new - g.n_old
	g = g[g.dff != 0].sort_values('dff', ascending=0)
	print(g)
	print('Added {} sales'.format(l1 - l0))
	old.to_csv('./data/sales.csv', index=False)
	return(old)

def add_eth_sales():
	print('Adding ETH sales...')

	query = '''
		SELECT project_name
		, token_id
		, block_timestamp AS sale_date
		, price
		, tx_id
		FROM ethereum.nft_events
		WHERE project_name IN (
			'BoredApeYachtClub'
			, 'MutantApeYachtClub'
			, 'BoredApeKennelClub'
		)
	'''
	sales = ctx.cursor().execute(query)
	sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
	sales = clean_colnames(sales)
	# print('Queried {} sales'.format(len(sales)))
	# sales['chain'] = 'Ethereum'
	sorted(sales.project_name.unique())
	sales['collection'] = sales.project_name.apply(lambda x: clean_name(x) )
	# print(sales.groupby('collection').sale_date.max())
	sorted(sales.collection.unique())
	# m = sales.merge(id_map, how='left', on=['mint','collection'])
	# m = sales.merge(id_map, how='inner', on=['mint','collection'])
	# m.sort_values('collection')
	m = sales[[ 'collection','token_id','sale_date','price','tx_id' ]]
	old = pd.read_csv('./data/sales.csv')
	l0 = len(old)
	old = old[old.collection != 'Bakc']
	old = old[-old.collection.isin(sales.collection.unique())]
	old = old.append(m)
	# print(old.groupby('collection').token_id.count())
	l1 = len(old)
	print('Added {} sales'.format(l1 - l0))
	old.to_csv('./data/sales.csv', index=False)
	pass

def solana_metadata():
	metadata = pd.read_csv('./data/metadata.csv')
	metadata[metadata.collection == 'Solana Monkey Business'].feature_name.unique()
	
	metadata = metadata[ metadata.collection.isin(['Aurory', 'Degen Apes', 'Galactic Punks', 'Pesky Penguins', 'Solana Monkey Business', 'Thugbirdz']) ]
	collection = 'Solana Monkey Business'
	for collection in metadata.collection.unique():
		cur = metadata[metadata.collection == collection].fillna('None')
		cur['token_id'] = cur.token_id.astype(int)
		pct = cur[['token_id']].drop_duplicates()
		pct['pct'] = 1
		num_tokens = len(cur.token_id.unique())
		print('Working on {} with {} tokens'.format(collection, num_tokens))
		min(cur.token_id)
		max(cur.token_id)
		ps = pd.DataFrame()
		for c in cur.feature_name.unique():
			# if c in [ 'Attribute Count' ]:
			#     continue
			g = cur[cur.feature_name == c].groupby('feature_value').token_id.count().reset_index()
			g['cur_pct'] = (g.token_id / num_tokens)
			g = cur[cur.feature_name == c].merge(g[[ 'feature_value', 'cur_pct' ]] )
			ps = ps.append(g[['token_id','cur_pct']])
			pct = pct.merge(g[['token_id', 'cur_pct']])
			pct['pct'] = pct.pct * pct.cur_pct * pct.cur_pct
			del pct['cur_pct']
		ps['rk'] = ps.groupby('token_id').cur_pct.rank(ascending=0)
		ps[ps.token_id == 1355]
		mn = ps.rk.min()
		mx = ps.rk.max()
		ps['mult'] = ps.apply(lambda x: x['cur_pct'] ** (1 + (x['rk'] / (mx - mn)) ) )

def run_queries():
	for c in [ 'Levana Dragon Eggs','Levana Meteors','Levana Dust' ][1:]:
		print(c)
		with open('./metadata/sql/{}.txt'.format(c)) as f:
			query = f.readlines()
		metadata = ctx.cursor().execute(' '.join(query))
		metadata = pd.DataFrame.from_records(iter(metadata), columns=[x[0] for x in metadata.description])
		metadata = clean_colnames(metadata)
		metadata['image'] = metadata.image.apply(lambda x: 'https://cloudflare-ipfs.com/ipfs/'+re.split('/', x)[-1] )
		metadata['collection'] = c
		metadata['chain'] = 'Terra'
		list(metadata.image.values[:2]) + list(metadata.image.values[-2:])
		metadata.to_csv('./data/metadata/{}.csv'.format(c), index=False)

def add_terra_tokens():
	# galactic punks
	query = '''
		SELECT msg_value:execute_msg:mint_nft:token_id AS token_id
		, msg_value:execute_msg:mint_nft:extension:name AS name
		, msg_value:execute_msg:mint_nft:extension:image AS image
		FROM terra.msgs 
		WHERE msg_value:contract::string = 'terra16wuzgsx3tz4hkqu73q5s7unxenefkkvefvewsh'
		AND tx_status = 'SUCCEEDED'
		AND msg_value:execute_msg:mint_nft is not null
	'''
	tokens = ctx.cursor().execute(query)
	tokens = pd.DataFrame.from_records(iter(tokens), columns=[x[0] for x in tokens.description])
	tokens = clean_colnames(tokens)
	len(tokens)
	for c in tokens.columns:
		tokens[c] = tokens[c].apply(lambda x: re.sub('"', '', x) )
	collection = 'Levana Dragon Eggs'
	collection = 'Galactic Angels'
	for collection in [ 'Galactic Punks', 'LunaBulls', 'Levana Dragon Eggs' ]:
		if collection == 'Galactic Punks':
			df = tokens
			df['image_url'] = df.image.apply(lambda x: 'https://ipfs.io/ipfs/'+re.split('/', x)[-1] )
		else:
			df = pd.read_csv('./data/metadata/{}.csv'.format(collection))
			df = clean_colnames(df).rename(columns={'tokenid':'token_id'})
			df['collection'] = collection
			if collection == 'LunaBulls':
				df['image_url'] = df.ipfs_image
			elif 'image' in df.columns:
				df['image_url'] = df.image
		df['clean_token_id'] = df.name.apply(lambda x: re.split('#', x)[1] ).astype(int)
		df['collection'] = collection
		df['chain'] = 'Terra'
		old = pd.read_csv('./data/tokens.csv')
		old = old[ -(old.collection == collection) ]
		old = old.drop_duplicates(subset=['collection','token_id'], keep='first')
		df['market_url'] = df.apply(lambda x: '' if x['chain'] == 'Solana' else 'https://randomearth.io/items/{}_{}'.format( d_market[x['collection']], x['token_id'] ), 1)
		df = df[list(old.columns)]
		old = old.append(df)
		print(old.groupby('collection').clean_token_id.count())
		old.to_csv('./data/tokens.csv', index=False)

def add_terra_metadata():
	query = '''
	SELECT CASE 
		WHEN contract_address = 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' THEN 'Levana Dragons'
		WHEN contract_address = 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2' THEN 'LunaBulls'
		WHEN contract_address = 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k' THEN 'Galactic Punks'
		ELSE 'Other' 
	END AS collection
	, token_id
	, token_metadata:traits AS traits
	FROM terra.nft_metadata
	WHERE contract_address in (
		'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v'
		, 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2'
		, 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k'
	)
	AND token_metadata:traits IS NOT NULL
	'''
	db_metadata = ctx.cursor().execute(query)
	db_metadata = pd.DataFrame.from_records(iter(db_metadata), columns=[x[0] for x in db_metadata.description])
	db_metadata = clean_colnames(db_metadata)
	collection = 'Levana Dragon Eggs'
	collection = 'Galactic Angels'
	for collection in [ 'Galactic Punks', 'LunaBulls', 'Levana Dragon Eggs' ]:
		if collection == 'Galactic Punks':
			cur = db_metadata[ db_metadata.collection == collection ]
			data = []
			for row in cur.iterrows():
				row = row[1]
				trait_names = [ re.split('"', re.split(':', x)[0])[1] for x in re.split(',', row['traits'])]
				trait_values = [ re.split('"', re.split(':', x)[1])[1] for x in re.split(',', row['traits'])]
				d = {'collection':row['collection'], 'token_id':row['token_id']}
				for n, v in zip(trait_names, trait_values):
					d[n] = v
				data += [d]
			metadata = pd.DataFrame(data)
		else:
			metadata = pd.read_csv('./data/metadata/{}.csv'.format(collection))
			metadata.columns = [ x.lower() for x in metadata.columns ]
			if 'Levana' in collection:
				metadata = metadata.rename(columns={'rank':'collection_rank'})
			metadata = clean_colnames(metadata).rename(columns={'tokenid':'token_id'})
			cols = [ c for c in metadata.columns if not c in [ 'block_timestamp','block_id','tx_id','collection','chain','name','image','token_name' ] ]
			metadata = metadata[cols]
			metadata['collection'] = collection

		none_col = 'None'
		metadata = metadata.fillna(none_col)
		for c in [ x for x in metadata.columns if type(metadata[x].values[0])==str]:
			metadata[c] = metadata[c].apply(lambda x: re.sub('"', '', x) )
		if collection == 'Galactic Punks':
			glitches = [ 'messy pink','messy blue','ponytail red','messy brown','neat brown','ponytail black','neat red','messy blonde','neat black','neat blonde','ponytail blonde' ]
			metadata['glitch_trait'] = metadata.hair.apply(lambda x: 'Yes' if x in glitches else 'No' )
		metadata['pct'] = 1
		metadata['attribute_count'] = 0
		l = len(metadata)
		incl_att_count = not collection in [ 'Levana Dragon Eggs' ]
		for c in list(metadata.columns) + ['attribute_count']:
			if c in ['token_id','collection','pct','levana_rank','meteor_id']:
				continue
			if c == 'attribute_count' and not incl_att_count:
				continue
			metadata[c] = metadata[c].apply(lambda x: re.sub('_', ' ', x).title() if x==x and type(x) == str else x )
			g = metadata.groupby(c).token_id.count().reset_index()
			g['cur_pct'] = g.token_id / l
			metadata = metadata.merge(g[[c, 'cur_pct']])
			metadata['pct'] = metadata.pct * metadata.cur_pct
			if incl_att_count and not c in ['attribute_count','glitch_trait']:
				metadata['attribute_count'] = metadata.attribute_count + metadata[c].apply(lambda x: int(x != none_col) )
			del metadata['cur_pct']
			# cur = metadata[[ 'collection','token_id', c ]].rename(columns={c: 'feature_value'})
			# cur['feature_name'] = c
			# m = m.append(cur)
		if incl_att_count:
			metadata.groupby('attribute_count').token_id.count().reset_index()
			# metadata.groupby(['rarity','attribute_count']).token_id.count().reset_index()
		# metadata.groupby('backgrounds').token_id.count().reset_index().token_id.sum()
		# metadata.sort_values('pct_rank')
		metadata.sort_values('pct')
		metadata['nft_rank'] = metadata.pct.rank()
		# metadata['rarity_score'] = metadata.pct.apply(lambda x: 1.0 / (x**0.07) )
		# mn = metadata.rarity_score.min()
		# mx = metadata.rarity_score.max()
		# metadata = metadata.sort_values('token_id')
		# metadata['rarity_score'] = metadata.rarity_score.apply(lambda x: ((x - mn) * 99 / (mx - mn)) + 1)
		# metadata['rarity_score_rank'] = metadata.rarity_score.rank(ascending=0, method='first').astype(int)
		# metadata.sort_values('rarity_score', ascending=0).head(20)[['token_id','collection_rank','rarity_score','rarity_score_rank']]
		# metadata.sort_values('rarity_score', ascending=0).tail(20)[['token_id','collection_rank','rarity_score']]
		# len(metadata[metadata.rarity_score<=2.4]) / len(metadata)
		# metadata[metadata.token_id==6157].sort_values('rarity_score', ascending=0).tail(20)[['token_id','collection_rank','rarity_score','rank']]
		# metadata[metadata['rank']>=3000].groupby('weight').token_id.count()

		# metadata.rarity_score.max()
		# metadata.rarity_score.min()
		# metadata.sort_values('rank')[['rank','pct','rarity_score']]

		m = pd.DataFrame()
		for c in metadata.columns:
			if c in [ 'token_id','collection' ]:
				continue
			cur = metadata[[ 'token_id','collection', c ]].rename(columns={c: 'feature_value'})
			cur['feature_name'] = c
			m = m.append(cur)
		m['chain'] = 'Terra'
		m.groupby('feature_name').feature_value.count()
		if collection == 'Levana Dragon Eggs':
			add = m[m.feature_name=='collection_rank']
			add['feature_name'] = 'transformed_collection_rank'
			mx = add.feature_value.max()
			mn = add.feature_value.min()
			add['feature_value'] = add.feature_value.apply(lambda x: 1.42**(1.42**(8*(x-mn)/(mx-mn))) + 0.13)
			# add['tmp'] = add.feature_value.rank() * 10 / len(add)
			# add['tmp'] = add.tmp.astype(int)
			# add.groupby('tmp').feature_value.mean()
			m = m.append(add)

			add = m[m.feature_name=='collection_rank']
			add['feature_name'] = 'collection_rank_group'
			add['feature_value'] = add.feature_value.apply(lambda x: int(x/1000))
			m = m.append(add)

		g = m.groupby('feature_value').feature_name.count().reset_index().sort_values('feature_name').tail(50)
		old = pd.read_csv('./data/metadata.csv')
		m['feature_name'] = m.feature_name.apply(lambda x: re.sub('_', ' ', x).title() )
		m['feature_value'] = m.feature_value.apply(lambda x: re.sub('_', ' ', x).title() if type(x) == str else x )
		l0 = len(old)
		if not 'chain' in old.columns:
			old['chain'] = old.collection.apply(lambda x: 'Terra' if x in [ 'Galactic Punks', 'LunaBulls' ] else 'Solana' )
		old = old[-old.collection.isin(m.collection.unique())]
		old = old.append(m)
		old = old.drop_duplicates(subset=['collection','token_id','feature_name'])
		old = old[-(old.feature_name.isin(['last_sale']))]
		# print(old.groupby(['chain','collection']).token_id.count())
		print(old[['chain','collection','token_id']].drop_duplicates().groupby(['chain','collection']).token_id.count())
		l1 = len(old)
		print('Adding {} rows'.format(l1 - l0))
		old.to_csv('./data/metadata.csv', index=False)

def add_terra_sales():
	print('Adding Terra sales')
	query = '''
		WITH 
		RE_events AS (
			SELECT
				block_timestamp,
				tx_id,
				event_attributes
			FROM
				terra.msg_events
			WHERE event_attributes:action = 'execute_orders'
				AND event_type = 'from_contract'
				AND tx_status = 'SUCCEEDED'
				AND block_timestamp >= CURRENT_DATE - 3
		),

		RE_takers AS (
			SELECT DISTINCT
				tx_id,
				msg_value:sender as taker
			FROM
				terra.msgs
			WHERE
				tx_id IN (SELECT DISTINCT tx_id FROM RE_events)
				AND block_timestamp >= CURRENT_DATE - 3
		),
		allSales AS 
		(
			SELECT 
			block_timestamp,
			tx_id,
			platform,
			nft_from,
			nft_to,
			nft_address,
			CASE nft_address
			WHEN 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2' THEN 'LunaBulls'
			WHEN 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k' THEN 'Galactic Punks'
			WHEN 'terra1vhuyuwwr4rkdpez5f5lmuqavut28h5dt29rpn6' THEN 'Levana Dragons'
			WHEN 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' THEN 'Levana Dust'
			WHEN 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' THEN 'Levana Eggs'
			WHEN 'terra14gfnxnwl0yz6njzet4n33erq5n70wt79nm24el' THEN 'Levana Loot'
			WHEN 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' THEN 'Levana Meteors'
			WHEN 'terra13nccm82km0ttah37hkygnvz67hnvkdass24yzv' THEN 'Galactic Angels'
			ELSE nft_address END
			as nft,
			amount,
			denom,
			tokenid
			FROM (
				SELECT
				block_timestamp,
				tx_id,
				'Random Earth' as platform,
				action,
				IFF(action = 'SELL', maker, taker) as nft_from,
				IFF(action = 'ACCEPT BID', maker, taker) as nft_to,
				nft_address,
				amount,
				denom,
				tokenid
				FROM (
					SELECT
						block_timestamp,
						e.tx_id,
						action,
						maker,
						taker,
						nft_address,
						amount,
						denom,
						tokenid
					FROM (
						SELECT 
							block_timestamp,
							tx_id,
							IFF(event_attributes:order:order:maker_asset:info:nft is not null, 'SELL', 'ACCEPT BID') as action,
							LISTAGG(CHR(F.VALUE)) WITHIN GROUP (ORDER BY F.INDEX) as maker,
							IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:maker_asset:info:nft:contract_addr, event_attributes:order:order:taker_asset:info:nft:contract_addr)::string as nft_address,
							IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:taker_asset:amount, event_attributes:order:order:maker_asset:amount) / 1e6 as amount,
							IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:taker_asset:info:native_token:denom, event_attributes:order:order:maker_asset:info:native_token:denom)::string as denom,
									IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:maker_asset:info:nft:token_id, event_attributes:order:order:taker_asset:info:nft:token_id) as tokenid
						FROM 
							RE_events e,
							LATERAL FLATTEN(input => event_attributes:order:order:maker) F
						GROUP BY
							block_timestamp,
							tx_id,
							nft_address,
							amount,
							denom,
							tokenid,
							action
					) e
					JOIN RE_takers t
					ON e.tx_id = t.tx_id
				)

				UNION 

				SELECT 
				block_timestamp,
				tx_id,
				'Knowhere' as platform,
				MAX(IFF(event_attributes:bid_amount is not null, 'SELL', 'AUCTION')) as action,
				MAX(IFF(event_type = 'coin_received', COALESCE(event_attributes:"2_receiver", event_attributes:"1_receiver"), '')) as nft_from,
				MAX(IFF(event_attributes:"0_action" = 'settle' AND event_attributes:"1_action" = 'transfer_nft', event_attributes:recipient, '')) as nft_to,
				MAX(IFF(event_attributes:"1_action" is not null, event_attributes:"1_contract_address", ''))::string as nft_address,
				MAX(IFF(event_type = 'coin_received', COALESCE(NVL(event_attributes:"0_amount"[0]:amount,0) + NVL(event_attributes:"1_amount"[0]:amount,0) + NVL(event_attributes:"2_amount"[0]:amount, 0), event_attributes:amount[0]:amount), 0)) / 1e6 as amount,
				MAX(IFF(event_type = 'coin_received', COALESCE(event_attributes:"0_amount"[0]:denom, event_attributes:amount[0]:denom), ''))::string as denom,
				MAX(IFF(event_type = 'wasm', event_attributes:token_id, 0)) as tokenid
				FROM 
				terra.msg_events
				WHERE 
					tx_status = 'SUCCEEDED'
					AND block_timestamp >= CURRENT_DATE - 3
					AND tx_id IN ( 
						SELECT DISTINCT 
							tx_id 
						FROM terra.msgs 
						WHERE 
							msg_value:execute_msg:settle:auction_id is not null 
							AND tx_status = 'SUCCEEDED' 
							AND msg_value:contract = 'terra12v8vrgntasf37xpj282szqpdyad7dgmkgnq60j'
							AND block_timestamp >= CURRENT_DATE - 3
					)
				GROUP BY
					block_timestamp,
					tx_id

				UNION

				SELECT 
				block_timestamp,
				tx_id,
				'Luart' as platform,
				UPPER(event_attributes:order_type) as action,
				event_attributes:order_creator as nft_from, -- for sells, no info about other types yet
				event_attributes:recipient as nft_to,
				event_attributes:nft_contract_address as nft_address,
				event_attributes:price / 1e6 as amount,
				event_attributes:denom::string as denom,
				event_attributes:"0_token_id" as tokenid
				FROM terra.msg_events
				WHERE 
				event_type = 'from_contract'
				AND event_attributes:action = 'transfer_nft'
				AND event_attributes:method = 'execute_order'
				AND event_attributes:"0_contract_address" = 'terra1fj44gmt0rtphu623zxge7u3t85qy0jg6p5ucnk'
				AND block_timestamp >= CURRENT_DATE - 3
			)
			WHERE nft_address IN (
				'terra13nccm82km0ttah37hkygnvz67hnvkdass24yzv',
				'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2',
				'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k',
				'terra1vhuyuwwr4rkdpez5f5lmuqavut28h5dt29rpn6',
				'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7',
				'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg',
				'terra14gfnxnwl0yz6njzet4n33erq5n70wt79nm24el',
				'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v'
			)
		)

		select * from allsales
	'''
	sales = ctx.cursor().execute(query)
	sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
	sales = clean_colnames(sales)
	# tokens = pd.read_csv('./data/tokens.csv')
	# tokens['tmp'] = tokens.token_id.apply(lambda x: (str(x)[:5]))
	# tokens[tokens.collection == 'Galactic Punks'].to_csv('~/Downloads/tmp.csv', index=False)
	sales.tokenid.values[:4]
	sales['tokenid'] = sales.tokenid.apply(lambda x: str(int(float(x))) )
	# tokens['token_id'] = tokens.token_id.astype(str)

	# s = sales[sales.nft == 'Galactic Punks']
	# t = tokens[tokens.collection == 'Galactic Punks'].token_id.values
	# s[s.tokenid.isin(t)]

	
	sales = sales.rename(columns={
		'nft':'collection'
		, 'block_timestamp': 'sale_date'
		, 'amount': 'price'
		, 'tokenid': 'token_id'
	})
	sales = clean_token_id(sales)

	assert(len(sales[sales.price.isnull()]) == 0)

	old = pd.read_csv('./data/sales.csv')
	go = old.groupby('collection').token_id.count().reset_index().rename(columns={'token_id':'n_old'})
	l0 = len(old)
	app = old[ (old.collection.isin(sales.collection.unique())) ].append(sales)
	assert(len(app[app.tx_id.isnull()]) == 0)
	app = app.drop_duplicates('tx_id')
	old = old[ -(old.collection.isin(sales.collection.unique())) ]
	old = old.append(app)
	old = old[[ 'collection','token_id','sale_date','price','tx_id' ]]

	# check changes
	l1 = len(old)
	gn = old.groupby('collection').token_id.count().reset_index().rename(columns={'token_id':'n_new'})
	g = gn.merge(go, how='outer', on=['collection']).fillna(0)
	g['dff'] = g.n_new - g.n_old
	g = g[g.dff != 0].sort_values('dff', ascending=0)
	print(g)
	print('Added {} sales'.format(l1 - l0))
	old.to_csv('./data/sales.csv', index=False)
	return(old)

def rarity_tools(browser):
	data = []
	collection = 'boredapeyachtclub'
	collection = 'mutant-ape-yacht-club'
	collection = 'bored-ape-kennel-club'
	url = 'https://rarity.tools/{}'.format(collection)
	browser.get(url)
	for i in range(201):
		print(i, len(data))
		sleep(0.1)
		soup = BeautifulSoup(browser.page_source)
		for div in soup.find_all('div', class_='bgCard'):
			rk = div.find_all('div', class_='font-extrabold')
			img = div.find_all('img')
			if len(rk) and len(img):
				# try:
				rk = int(just_float(rk[0].text))
				img_url = re.split('\?', img[0].attrs['src'])[0]
				token_id = int(re.split('/|\.', img_url)[6])
				data += [[ collection, token_id, img_url, rk ]]
				# except:
				# 	pass
		# bs = browser.find_elements_by_class_name('smallBtn')
		browser.find_elements_by_class_name('smallBtn')[4 + int(i > 0)].click()
		sleep(0.1)
		# for i in range(len(bs)):
		# 	print(i, browser.find_elements_by_class_name('smallBtn')[i].text)
	df = pd.DataFrame(data, columns=['collection','token_id','image_url','nft_rank']).drop_duplicates()
	len(df)
	df['chain'] = 'Ethereum'
	df['clean_token_id'] = df.token_id
	df['collection'] = df.collection.apply(lambda x: clean_name(x) )
	len(df)
	old = pd.read_csv('./data/tokens.csv')
	l0 = len(old)
	old = old[-old.collection.isin(df.collection.unique())]
	old = old.append(df)
	l1 = len(old)
	print('Added {} rows'.format(format_num(l1 - l0)))
	old.tail()
	old[old.chain == 'Ethereum'].collection.unique()
	old.to_csv('./data/tokens.csv', index=False)

def eth_metadata_api():
	old = pd.read_csv('./data/metadata.csv')
	collection = 'BAYC'
	collection = 'MAYC'
	seen = []
	seen = sorted(old[old.collection == collection].token_id.unique())
	a_data = []
	t_data = []
	errs = []
	# for i in range(10000):
	it = 0
	for i in ids[21:]:
		sleep(.1)
		it += 1
		if it % 1 == 0:
			print(i, len(t_data), len(a_data), len(errs))
		if i in seen:
			continue
		# try:
		url = 'https://boredapeyachtclub.com/api/mutants/{}'.format(i)
		try:
			j = requests.get(url).json()
			t_data += [[ i, j['image'] ]]
			for a in j['attributes']:
				a_data += [[ i, a['trait_type'], a['value'] ]]
		except:
			print('Re-trying once...')
			sleep(30)
			try:
				j = requests.get(url).json()
				t_data += [[ i, j['image'] ]]
				for a in j['attributes']:
					a_data += [[ i, a['trait_type'], a['value'] ]]
			except:
				print('Re-trying twice...')
				sleep(30)
				j = requests.get(url).json()
				t_data += [[ i, j['image'] ]]
				for a in j['attributes']:
					a_data += [[ i, a['trait_type'], a['value'] ]]
	# errs.append(i)
	new_mdf = pd.DataFrame(a_data, columns=['token_id','feature_name','feature_value'])
	new_mdf['collection'] = 'MAYC'
	new_mdf['chain'] = 'Ethereum'
	old = old.append(new_mdf)
	old.to_csv('./data/metadata.csv', index=False)

	new_tdf = pd.DataFrame(t_data, columns=['token_id','image_url'])
	new_tdf['collection'] = 'MAYC'
	m = pd.read_csv('./data/metadata.csv')
	old = pd.read_csv('./data/tokens.csv')
	l0 = len(old)
	old = old.merge(new_tdf, on=['collection', 'token_id'], how='left')
	old[old.image_url_y.notnull()]
	old[old.image_url_y.notnull()][['image_url_x','image_url_y']]
	old['image_url'] = old.image_url_y.fillna(old.image_url_x)
	del old['image_url_x']
	del old['image_url_y']
	l1 = len(old)
	print('Adding {} rows'.format(l1 - l0))
	old.to_csv('./data/tokens.csv', index=False)


	tmp = old[old.collection == 'MAYC']
	tmp['tmp'] = tmp.image_url.apply(lambda x: int('nft-media' in x) )
	tmp[tmp.tmp == 1].merge(m[['token_id']].drop_duplicates())[['token_id']].drop_duplicates()
	ids = tmp[tmp.tmp == 1].merge(m[['token_id']].drop_duplicates()).token_id.unique()
	a = old[old.collection == 'MAYC'].token_id.unique()
	b = new_tdf.token_id.unique()
	[x for x in b if not x in a]
	new_mdf['collection'] = 'MAYC'
	new_mdf['chain'] = 'Ethereum'
	old = old.append(new_mdf)
	old.to_csv('./data/metadata.csv', index=False)

	collection = 'BAYC'
	data = []
	for i in range(0, 1000):
		if i % 100 == 1:
			print(i, len(data))
		url = 'https://ipfs.io/ipfs/QmeSjSinHpPnmXmspMjwiXyN6zS4E9zccariGR3jxcaWtq/{}'.format(i)
		# try:
		j = requests.get(url, verify=False, timeout=1).json()
		data += [[ collection, i, j['image'] ]]
		# except:
		# 	print(i)


def eth_metadata():

	query = '''
		SELECT contract_name
		, token_id
		, token_metadata:Background AS background
		, token_metadata:Clothes AS clother
		, token_metadata:Earring AS earring
		, token_metadata:Eyes AS eyes
		, token_metadata:Fur AS fur
		, token_metadata:Hat AS hat
		, token_metadata:Mouth AS mouth
		, image_url
		FROM ethereum.nft_metadata
		WHERE contract_name IN ('MutantApeYachtClub','bayc')
	'''
	metadata = ctx.cursor().execute(query)
	metadata = pd.DataFrame.from_records(iter(metadata), columns=[x[0] for x in metadata.description])
	metadata = clean_colnames(metadata)
	metadata['collection'] = metadata.contract_name.apply(lambda x: x[0].upper()+'AYC' )
	metadata['image_url'] = metadata.image_url.apply(lambda x: 'https://ipfs.io/ipfs/{}'.format(re.split('/', x)[-1]) if 'ipfs' in x else x )
	# metadata['image_url'] = metadata.tmp
	old = pd.read_csv('./data/tokens.csv')
	old = old.merge( metadata[[ 'collection','token_id','image_url' ]], how='left', on=['collection','token_id'] )
	old[old.image_url_y.notnull()]
	old['image_url'] = old.image_url_y.fillna(old.image_url_x)
	del old['image_url_x']
	del old['image_url_y']
	del metadata['image_url']
	old.to_csv('./data/tokens.csv', index=False)

	ndf = pd.DataFrame()
	e = [ 'contract_name', 'token_id', 'collection' ]
	for c in [ c for c in metadata.columns if not c in e ]:
		cur = metadata[['collection','token_id',c]]
		cur.columns = [ 'collection','token_id','feature_value' ]
		cur['feature_name'] = c.title()
		cur.feature_value.unique()
		cur['feature_value'] = cur.feature_value.apply(lambda x: x[1:-1] if x else 'None' )
		ndf = ndf.append(cur)
	ndf = ndf.drop_duplicates()
	ndf['chain'] = 'Ethereum'
	g = ndf.groupby(['collection', 'feature_name', 'feature_value']).token_id.count().reset_index()


	old = pd.read_csv('./data/metadata.csv')
	old.head()
	l0 = len(old)
	old = old.append(ndf)
	l1 = len(old)
	print('Adding {} rows'.format(l1 - l0))
	old.to_csv('./data/metadata.csv', index=False)


	t_data = []
	a_data = []
	for i in range(10000):
		if i % 100 == 1:
			print(i, len(t_data), len(a_data))
		token_id = i + 1
		url = 'https://us-central1-bayc-metadata.cloudfunctions.net/api/tokens/{}'.format(i)
		j = requests.get(url).json()
		t_data += [[ token_id, j['image'] ]]
		for a in j['attributes']:
			a_data += [[ token_id, a['trait_type'], a['value'] ]]
	df = pd.DataFrame(t_data, columns=['token_id',''])


######################################
#     Grab Data From OpenSea API     #
######################################
def load_api_data():
	headers = {
		'Content-Type': 'application/json'
		, 'X-API-KEY': '2b7cbb0ebecb468bba431aefb8dbbebe'
	}
	data = []
	traits_data = []
	contract_address = '0x23581767a106ae21c074b2276d25e5c3e136a68b'
	# url = 'https://api.opensea.io/api/v1/assets?asset_contract_address=0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d&limit=20&token_ids=8179'
	# for o in [ 'asc', 'desc' ]:
	for o in [ 'asc' ]:
		l = 1
		it = 0
		offset = 0
		while l and offset <= 10000:
			if offset % 1000 == 0:
				print("#{}/{}".format(offset, 20000))
			r = requests.get('https://api.opensea.io/api/v1/assets?asset_contract_address={}&order_by=pk&order_direction={}&offset={}&limit=50'.format(contract_address, o, offset), headers = headers)
			# r = requests.get(url)
			assets = r.json()['assets']
			l = len(assets)
			for a in assets:
				token_id = a['token_id']
				for t in a['traits']:
					traits_data += [[ contract_address, token_id, t['trait_type'], t['value']  ]]
				data += [[ contract_address, token_id, a['image_url'] ]]
			offset += 50
	opensea_data = pd.DataFrame(data, columns=['contract_address','token_id','image_url']).drop_duplicates()
	len(opensea_data.token_id.unique())
	traits = pd.DataFrame(traits_data, columns=['contract_address','token_id','trait_type','trait_value']).drop_duplicates()
	# a = set(range(opensea_data.token_id.min(), opensea_data.token_id.max()))
	# b = set(opensea_data.token_id.unique())
	# a.difference(b)
	# len(opensea_data)
	# sorted(traits.trait_type.unique())
	traits = traits[(traits.trait_type != 'Token ID')]
	traits['token_id'] = traits.token_id.astype(int)
	traits.to_csv('./data/moonbird_traits.csv', index=False)
	opensea_data.to_csv('./data/moonbird_data.csv', index=False)

	traits = pd.read_csv('./data/mayc_traits.csv')
	opensea_data = pd.read_csv('./data/mayc_data.csv')

	len(traits.token_id.unique())
	opensea_data['token_id'] = opensea_data.token_id.astype(int)
	opensea_data.token_id.max()
	len(opensea_data)

	it = 0
	max_it = 9458
	for row in opensea_data.iterrows():
		it += 1
		if it % 100 == 0:
			print('#{}/{}'.format(it, len(opensea_data)))
		if it < max_it:
			continue
		row = row[1]
		urllib.request.urlretrieve(row['image_url'], './viz/www/img/{}.png'.format(row['token_id']))

def load_api_data():
	results = []
	contract_address = '0x60e4d786628fea6478f785a6d7e704777c86a7c6'
	for o in [ 'asc', 'desc' ]:
		l = 1
		it = 0
		offset = 0
		while l and offset <= 10000:
			if offset % 1000 == 0:
				print("#{}/{}".format(offset, 20000))
			r = requests.get('https://api.opensea.io/api/v1/assets?asset_contract_address={}&order_by=pk&order_direction={}&offset={}&limit=50'.format(contract_address, o, offset))
			assets = r.json()['assets']
			for a in assets:
				token_metadata = {}
				for t in a['traits']:
					token_metadata[t['trait_type']] = t['value']
				token_id = a['token_id']
				d = {
					'commission_rate': None
					, 'contract_address': a['asset_contract']['address']
					, 'contract_name': a['asset_contract']['name']
					, 'created_at_block_id': 0
					, 'created_at_timestamp': re.sub('T', ' ', str(a['asset_contract']['created_date']))
					, 'created_at_tx_id': ''
					, 'creator_address': a['creator']['address'] if a['creator'] else a['asset_contract']['address']
					, 'creator_name': a['creator']['address'] if a['creator'] else a['asset_contract']['name']
					, 'image_url': a['image_url']
					, 'project_name': a['asset_contract']['name']
					, 'token_id': token_id
					, 'token_metadata': token_metadata
					, 'token_metadata_uri': a['image_original_url']
					, 'token_name': '{} #{}'.format(a['asset_contract']['symbol'], token_id)
				}
				results.append(d)
			offset += 50

	n = 50
	r = math.ceil(len(results) / n)
	blockchain = 'ethereum'
	directory = 'mayc'
	for i in range(r):
		newd = {
			"model": {
				"blockchain": blockchain,
				"sinks": [
					{
						"destination": "{database_name}.silver.nft_metadata",
						"type": "snowflake",
						"unique_key": "blockchain || contract_address || token_id"
					}
				],
			},
			"results": results[(i * n):((i * n)+r)]
		}
		with open('./data/metadata/{}/{}.txt'.format(directory, i), 'w') as outfile:
			outfile.write(json.dumps(newd))