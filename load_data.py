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

def add_eth_sales():
	# BAYC and MAYC sales
	query = '''
	'''
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
	tokens['clean_token_id'] = tokens.name.apply(lambda x: re.split('#', x)[1] )
	tokens['collection'] = 'Galactic Punks'
	tokens['chain'] = 'Terra'
	tokens['image_url'] = tokens.image.apply(lambda x: 'https://ipfs.io/ipfs/'+re.split('/', x)[-1] )
	old = pd.read_csv('./data/tokens.csv')
	old = old.drop_duplicates(subset=['collection','token_id'], keep='first')
	# g = old.groupby(['collection','token_id']).clean_token_id.count().reset_index().sort_values('clean_token_id', ascending=0)
	# g.head(3)[['collection','token_id']].merge(old)
	# g.head(3)[['collection','token_id']].merge(old).image_url.values
	# g.head()
	# len(old)
	# len(old.drop_duplicates())
	# len(old[['collection','token_id']].drop_duplicates())
	tokens = tokens[list(old.columns)]
	old = old.append(tokens)
	old.to_csv('./data/tokens.csv', index=False)

def add_terra_metadata():
	# query = '''
	# SELECT token_id, token_metadata:traits AS traits
	# FROM terra.nft_metadata
	# WHERE contract_address in ('terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k')
	# '''
	# metadata = ctx.cursor().execute(query)
	# metadata = pd.DataFrame.from_records(iter(metadata), columns=[x[0] for x in metadata.description])
	# data = []
	# sorted(list(set(data)))
	# for row in metadata.iterrows():
	# 	row = row[1]
	# 	try:
	# 		data += [ re.split('"', re.split(':', x)[0])[1] for x in re.split(',', row['TRAITS'])]
	# 	except:
	# 		print(row['TOKEN_ID'])
	query = '''
	SELECT CASE 
		WHEN contract_address = 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' THEN 'Levana Dragons'
		ELSE 'Galactic Punks' 
	END AS collection
	, token_id
	, token_metadata:traits:backgrounds as backgrounds
	, token_metadata:traits:face as face
	, token_metadata:traits:glasses as glasses
	, token_metadata:traits:hair as hair
	, token_metadata:traits:headware as headware
	, token_metadata:traits:jewelry as jewelry
	, token_metadata:traits:species as species
	, token_metadata:traits:suits as suits
	FROM terra.nft_metadata
	WHERE contract_address in ('terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k')
	'''
	metadata = ctx.cursor().execute(query)
	metadata = pd.DataFrame.from_records(iter(metadata), columns=[x[0] for x in metadata.description])
	none_col = 'None'
	metadata = metadata.fillna(none_col)
	for c in [ x for x in metadata.columns if type(metadata[x].values[0])==str]:
		metadata[c] = metadata[c].apply(lambda x: re.sub('"', '', x) )
	metadata = clean_colnames(metadata)
	glitches = [ 'messy pink','messy blue','ponytail red','messy brown','neat brown','ponytail black','neat red','messy blonde','neat black','neat blonde','ponytail blonde' ]
	metadata['glitch_trait'] = metadata.hair.apply(lambda x: 'Yes' if x in glitches else 'No' )
	metadata[metadata.glitch_trait == 'Yes']
	sorted(metadata[metadata.glitch_trait=='Yes'].hair.unique())
	sorted(metadata[metadata.glitch_trait=='No'].hair.unique())
	metadata.head()
	metadata['pct'] = 1
	metadata['attribute_count'] = 0
	l = len(metadata)
	for c in list(metadata.columns) + ['attribute_count']:
		if c in ['token_id','collection','pct']:
			continue
		g = metadata.groupby(c).token_id.count().reset_index()
		g['cur_pct'] = g.token_id / l
		metadata = metadata.merge(g[[c, 'cur_pct']])
		metadata['pct'] = metadata.pct * metadata.cur_pct
		if not c in ['attribute_count','glitch_trait']:
			metadata['attribute_count'] = metadata.attribute_count + metadata[c].apply(lambda x: int(x != none_col) )
		del metadata['cur_pct']
		# cur = metadata[[ 'collection','token_id', c ]].rename(columns={c: 'feature_value'})
		# cur['feature_name'] = c
		# m = m.append(cur)
	metadata.groupby('attribute_count').token_id.count().reset_index()
	metadata.groupby('backgrounds').token_id.count().reset_index().token_id.sum()
	metadata['rank'] = metadata.pct.rank()
	metadata['score'] = metadata.pct.apply(lambda x: 1.0 / x )
	mn = metadata.score.min()
	metadata['score'] = metadata.score.apply(lambda x: x / mn )
	metadata.score.max()
	metadata.sort_values('rank')[['rank','pct','score']]

	m = pd.DataFrame()
	for c in metadata.columns:
		if c in [ 'token_id','collection' ]:
			continue
		cur = metadata[[ 'token_id','collection', c ]].rename(columns={c: 'feature_value'})
		cur['feature_name'] = c
		m = m.append(cur)
	m['chain'] = 'Terra'
	m.groupby('feature_name').feature_value.count()
	m[m.feature_name=='face'].groupby('feature_value').token_id.count()
	print(len(m.token_id.unique()))
	g = m.groupby('feature_value').feature_name.count().reset_index().sort_values('feature_name').tail(50)
	old = pd.read_csv('./data/metadata.csv')
	if not 'chain' in old.columns:
		old['chain'] = 'Solana'
	old = old[-old.collection.isin(m.collection.unique())]
	old = old.append(m)
	print(old.groupby(['chain','collection']).token_id.count())
	old = old.drop_duplicates()
	old.to_csv('./data/metadata.csv', index=False)

	tokens = pd.read_csv('./data/tokens.csv')
	old[ (old.feature_name == 'glitch_trait') & (old.feature_value == 'Yes') ].merge(tokens)
	old[ (old.feature_name == 'attribute_count') ].merge(tokens[tokens.clean_token_id == 5326])


def add_terra_sales():
	# galactic punks
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
		AND contract IN ( 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k','terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' )
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
		AND contract IN ( 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k','terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' )
	), unioned AS (
		SELECT * FROM orders
		UNION ALL 
		SELECT * FROM Lorders
	)
	SELECT CASE 
		WHEN contract = 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' THEN 'Levana Dragons'
		ELSE 'Galactic Punks' 
	END AS collection
	, sale_date
	, token_id
	, tx_id
	, price
	FROM unioned
	'''
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