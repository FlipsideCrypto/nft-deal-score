import collections
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

def add_solana_sales():
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
	query = '''
		SELECT tx_id
		, n.mint
		, n.block_timestamp AS sale_date
		, (inner_instruction:instructions[0]:parsed:info:lamports 
		+ inner_instruction:instructions[1]:parsed:info:lamports 
		+ inner_instruction:instructions[2]:parsed:info:lamports 
		+ inner_instruction:instructions[3]:parsed:info:lamports) / POWER(10, 9) AS price
		FROM solana.nfts n
		LEFT JOIN crosschain.address_labels l ON LOWER(n.mint) = LOWER(l.address)
		WHERE block_timestamp >= CURRENT_DATE - 200
		AND instruction:data like '3UjLyJvuY4%'
		AND l.project_name ilike 'degods'
	'''
	sales = ctx.cursor().execute(query)
	sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
	sales = clean_colnames(sales)
	print('Queried {} sales'.format(len(sales)))
	sales['chain'] = 'Solana'
	sales['collection'] = 'DeGods'
	m = sales.merge(df, how='left', on=['mint'])
	s_df = pd.read_csv('./data/sales.csv')
	l0 = len(s_df)
	s_df = s_df[-s_df.collection.isin(sales.collection.unique())]
	s_df = s_df.append(m)
	print(s_df.groupby('collection').token_id.count())
	l1 = len(s_df)
	print('Added {} sales'.format(l1 - l0))
	for c in [ 'mint','tmp' ]:
		if c in s_df:
			del s_df[c]
	s_df.to_csv('./data/sales.csv', index=False)
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
		# d = {}
		# for row in ps.iterrows():
		# pct = pct.sort_values('pct')
		# pct['rk'] = pct.pct.rank()
		# pct.head()
		# pct[ pct.token_id == 1355 ]
		# pct[ pct.token_id == 2387 ]
		# pct[ pct.token_id == 4024 ]
		# cur[ cur.token_id == 1355 ]

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
	len(tokens)
	for c in tokens.columns:
		tokens[c] = tokens[c].apply(lambda x: re.sub('"', '', x) )
	collection = 'Levana Dragon Eggs'
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
			metadata.columns = [ x.lower() for x in metadata.columns ]
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
	# query = '''
	# WITH orders AS (
	# 	SELECT tx_id
	# 	, block_timestamp AS sale_date
	# 	, msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:token_id AS token_id
	# 	, msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:contract_addr::string AS contract
	# 	, msg_value:execute_msg:execute_order:order:order:taker_asset:amount::decimal/pow(10,6) AS price
	# 	FROM terra.msgs 
	# 	WHERE msg_value:contract::string = 'terra1eek0ymmhyzja60830xhzm7k7jkrk99a60q2z2t' 
	# 	AND tx_status = 'SUCCEEDED'
	# 	AND msg_value:execute_msg:execute_order IS NOT NULL
	# 	AND contract IN ( '{}' )
	# ), Lorders AS (
	# 	SELECT tx_id
	# 	, block_timestamp AS sale_date
	# 	, msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:maker_asset:info:nft:token_id AS token_id
	# 	, msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:maker_asset:info:nft:contract_addr::string AS contract
	# 	, msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:taker_asset:amount::decimal/pow(10,6) AS price
	# 	FROM terra.msgs 
	# 	WHERE msg_value:contract::string = 'terra1eek0ymmhyzja60830xhzm7k7jkrk99a60q2z2t' 
	# 	AND tx_status = 'SUCCEEDED'
	# 	AND msg_value:execute_msg:ledger_proxy:msg:execute_order IS NOT NULL
	# 	AND contract IN ( '{}' )
	# ), unioned AS (
	# 	SELECT * FROM orders
	# 	UNION ALL 
	# 	SELECT * FROM Lorders
	# )
	# SELECT CASE 
	# 	WHEN contract = 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' THEN 'Levana Dust'
	# 	WHEN contract = 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' THEN 'Levana Meteors'
	# 	WHEN contract = 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' THEN 'Levana Dragon Eggs'
	# 	WHEN contract = 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2' THEN 'LunaBulls'
	# 	WHEN contract = 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k' THEN 'Galactic Punks'
	# 	ELSE 'Other' 
	# END AS collection
	# , sale_date
	# , token_id
	# , tx_id
	# , price
	# FROM unioned
	# '''.format( '\', \''.join(contracts), '\', \''.join(contracts) )
	# sales = ctx.cursor().execute(query)
	# sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
	# sales = clean_colnames(sales)
	# sales.head()
	# tokens.to_csv('~/Downloads/tmp3.csv', index=False)
	# tokens[tokens.token_id == '25984997114855639851202718743284654443']
	

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
				
		),

		RE_takers AS (
			SELECT DISTINCT
				tx_id,
				msg_value:sender as taker
			FROM
				terra.msgs
			WHERE
				tx_id IN (SELECT DISTINCT tx_id FROM RE_events)
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
			WHEN 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' THEN 'Levana Meteor Dust'
			WHEN 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' THEN 'Levana Eggs'
			WHEN 'terra14gfnxnwl0yz6njzet4n33erq5n70wt79nm24el' THEN 'Levana Loot'
			WHEN 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' THEN 'Levana Meteors'
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
					AND tx_id IN ( 
						SELECT DISTINCT 
							tx_id 
						FROM terra.msgs 
						WHERE 
							msg_value:execute_msg:settle:auction_id is not null 
							AND tx_status = 'SUCCEEDED' 
							AND msg_value:contract = 'terra12v8vrgntasf37xpj282szqpdyad7dgmkgnq60j'
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
			)
			WHERE nft_address IN (
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
	# tmp = sales.merge(tokens[['collection','token_id','clean_token_id']])
	# sales[sales.tx_id.isin(['6CA1966B42D02F07D1FB6A839B8276D501FDF3EF048DECA5601C74D82EBB9D12',
    #    'F5643C0C805F3236F67CFF1A6AC1FC50CF9DB61B846B3CE6F9D4CD3806284D4E',
    #    'BFD1D2571B303CEC9BA6B2C67590242799000E3B8D4560792CD16E31BF5D5D1E'])]
	# sales.head()
	# sales.columns
	sales['chain'] = 'Terra'
	sales = sales[[ 'chain','collection','token_id','sale_date','price','tx_id' ]]
	print(sales.groupby(['chain','collection']).token_id.count())
	sales['token_id'] = sales.token_id.apply(lambda x: re.sub('"', '', x) )
	sales['collection'] = sales.collection.apply(lambda x: 'Levana Dragon Eggs' if x=='Levana Eggs' else x )
	old = pd.read_csv('./data/sales.csv')
	# print(old.groupby(['chain','collection']).token_id.count())
	l0 = len(old)
	if not 'chain' in old.columns:
		old['chain'] = 'Solana'
	old = old[ -(old.collection.isin(sales.collection.unique())) ]
	old = old.append(sales)
	old = old[[ 'chain','collection','token_id','sale_date','price','tx_id' ]]
	old = old.drop_duplicates(subset=['collection','token_id','price'])
	# old = old[-(old.collection == 'Levana Dragons')]
	# old = old[-(old.collection == 'Levana Dragon Eggs')]
	l1 = len(old)
	print('Added {} sales'.format(l1 - l0))
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