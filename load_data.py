import re
import os
import json
import math
from pandas.io.pytables import Selection
import requests
import pandas as pd
import urllib.request
import snowflake.connector

os.chdir('/Users/kellenblumberg/git/nft-deal-score')

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

d_market = {
	'Galactic Punks': 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k',
	'LunaBulls': 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2',
	'Levana Dragon Eggs': 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg',
	'Levana Dust': 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7',
	'Levana Meteors': 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v',
}

###################################
#     Define Helper Functions     #
###################################
def clean_colnames(df):
	names = [ x.lower() for x in df.columns ]
	df.columns = names
	return(df)

def manual_clean():
	for c in [ 'pred_price', 'attributes', 'feature_values', 'model_sales', 'listings', 'coefsdf', 'tokens' ]:
		df = pd.read_csv('./data/{}.csv'.format(c))
		df['chain'] = 'Solana'
		if c == 'tokens':
			df['clean_token_id'] = df.token_id
		df.to_csv('./data/{}.csv'.format(c), index=False)

def run_queries():
	for c in [ 'Levana Dragon Eggs','Levana Meteors','Levana Dust' ]:
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
		# old = pd.read_csv('./data/metadata.csv')
		# old = old[-old.collection.isin(metadata.collection.unique())]
		# old = old[[ 'token_id', 'collection', 'feature_name', 'feature_value' ]]
		# old = old.append(metadata)
		# old = old.drop_duplicates()
		# print(old.groupby(['chain','collection']).token_id.count())
		# print(old[['chain','collection','token_id']].drop_duplicates().groupby(['chain','collection']).token_id.count())
		# old.to_csv('./data/metadata.csv', index=False)
		# old = old[-old.collection == c]

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
	for c in tokens.columns:
		tokens[c] = tokens[c].apply(lambda x: re.sub('"', '', x) )
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
			else:
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
			cols = [ 'token_id', 'background', 'horns', 'body', 'nose', 'outfit', 'eyes', 'headwear', 'nosepiece' ]
			metadata = pd.read_csv('./data/metadata/{}.csv'.format(collection))
			if 'Levana' in collection:
				metadata = metadata.rename(columns={'rank':'collection_rank'})
			metadata = clean_colnames(metadata).rename(columns={'tokenid':'token_id'})
			cols = [ c for c in metadata.columns if not c in [ 'block_timestamp','block_id','tx_id','collection','chain','name','image' ] ]
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
		metadata.groupby('cracking_date').token_id.count()
		metadata.groupby('weight').token_id.count()
		metadata[metadata.cracking_date=='2471-12-22'][['token_id']]
		for c in list(metadata.columns) + ['attribute_count']:
			if c in ['token_id','collection','pct','levana_rank','meteor_id']:
				continue
			if c == 'attribute_count' and not incl_att_count:
				continue
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
		metadata['rank'] = metadata.pct.rank()
		metadata['rarity_score'] = metadata.pct.apply(lambda x: 1.0 / (x**0.07) )
		mn = metadata.rarity_score.min()
		mx = metadata.rarity_score.max()
		metadata = metadata.sort_values('token_id')
		metadata['rarity_score'] = metadata.rarity_score.apply(lambda x: ((x - mn) * 99 / (mx - mn)) + 1)
		metadata['rarity_score_rank'] = metadata.rarity_score.rank(ascending=0, method='first').astype(int)
		metadata.sort_values('rarity_score', ascending=0).head(20)[['token_id','collection_rank','rarity_score','rarity_score_rank']]
		metadata.sort_values('rarity_score', ascending=0).tail(20)[['token_id','collection_rank','rarity_score']]
		len(metadata[metadata.rarity_score<=2.4]) / len(metadata)
		metadata[metadata.token_id==6157].sort_values('rarity_score', ascending=0).tail(20)[['token_id','collection_rank','rarity_score','rank']]
		metadata[metadata['rank']>=3000].groupby('weight').token_id.count()

		metadata.rarity_score.max()
		metadata.rarity_score.min()
		metadata.sort_values('rank')[['rank','pct','rarity_score']]

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
		if not 'chain' in old.columns:
			old['chain'] = old.collection.apply(lambda x: 'Terra' if x in [ 'Galactic Punks', 'LunaBulls' ] else 'Solana' )
		old = old[-old.collection.isin(m.collection.unique())]
		old = old.append(m)
		old = old.drop_duplicates(subset=['collection','token_id','feature_name'])
		old = old[-(old.feature_name.isin(['last_sale']))]
		# print(old.groupby(['chain','collection']).token_id.count())
		print(old[['chain','collection','token_id']].drop_duplicates().groupby(['chain','collection']).token_id.count())
		old.to_csv('./data/metadata.csv', index=False)

def add_terra_sales():
	# galactic punks
	contracts = [
		'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7'
		, 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v'
		, 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg'
		, 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2'
		, 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k'
	]
	query = '''
	WITH orders AS (
		SELECT tx_id
		, block_timestamp AS sale_date
		, msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:token_id AS token_id
		, msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:contract_addr::string AS contract
		, msg_value:execute_msg:execute_order:order:order:taker_asset:amount::decimal/pow(10,6) AS price
		FROM terra.msgs 
		WHERE msg_value:contract::string = 'terra1eek0ymmhyzja60830xhzm7k7jkrk99a60q2z2t' 
		AND tx_status = 'SUCCEEDED'
		AND msg_value:execute_msg:execute_order IS NOT NULL
		AND contract IN ( '{}' )
	), Lorders AS (
		SELECT tx_id
		, block_timestamp AS sale_date
		, msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:maker_asset:info:nft:token_id AS token_id
		, msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:maker_asset:info:nft:contract_addr::string AS contract
		, msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:taker_asset:amount::decimal/pow(10,6) AS price
		FROM terra.msgs 
		WHERE msg_value:contract::string = 'terra1eek0ymmhyzja60830xhzm7k7jkrk99a60q2z2t' 
		AND tx_status = 'SUCCEEDED'
		AND msg_value:execute_msg:ledger_proxy:msg:execute_order IS NOT NULL
		AND contract IN ( '{}' )
	), unioned AS (
		SELECT * FROM orders
		UNION ALL 
		SELECT * FROM Lorders
	)
	SELECT CASE 
		WHEN contract = 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' THEN 'Levana Dust'
		WHEN contract = 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' THEN 'Levana Meteors'
		WHEN contract = 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' THEN 'Levana Dragon Eggs'
		WHEN contract = 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2' THEN 'LunaBulls'
		WHEN contract = 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k' THEN 'Galactic Punks'
		ELSE 'Other' 
	END AS collection
	, sale_date
	, token_id
	, tx_id
	, price
	FROM unioned
	'''.format( '\', \''.join(contracts), '\', \''.join(contracts) )
	sales = ctx.cursor().execute(query)
	sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
	sales = clean_colnames(sales)
	sales.head()[[ 'token_id','sale_date','price' ]]
	sales.columns
	sales['chain'] = 'Terra'
	sales = sales[[ 'chain','collection','token_id','sale_date','price','tx_id' ]]
	sales['token_id'] = sales.token_id.apply(lambda x: re.sub('"', '', x) )
	old = pd.read_csv('./data/sales.csv')
	if not 'chain' in old.columns:
		old['chain'] = 'Solana'
	old = old[ -(old.collection.isin(sales.collection.unique())) ]
	old = old.append(sales)
	old = old[[ 'chain','collection','token_id','sale_date','price','tx_id' ]]
	old = old[-(old.collection == 'Levana Dragons')]
	print(old.groupby(['chain','collection']).token_id.count())
	old.to_csv('./data/sales.csv', index=False)

######################################
#     Grab Data From OpenSea API     #
######################################
def load_api_data():
	data = []
	traits_data = []
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
	traits.to_csv('./data/mayc_traits.csv', index=False)
	opensea_data.to_csv('./data/mayc_data.csv', index=False)
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