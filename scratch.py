import os
import json
import psycopg2
import pandas as pd
import requests

os.chdir('/Users/kellenblumberg/git/nft-deal-score')

COLLECTION = 'Stoned Ape Crew'

def thorchain():
	r = requests.get('https://thornode.ninerealms.com/thorchain/pool/TERRA.LUNA/liquidity_providers?height=5461281')
	df_luna = pd.DataFrame(r.json())
	df_luna['pre_attack_luna_value'] = df_luna.asset_deposit_value

	r = requests.get('https://thornode.ninerealms.com/thorchain/pool/TERRA.UST/liquidity_providers?height=5461281')
	df_ust = pd.DataFrame(r.json())
	df_ust['pre_attack_ust_value'] = df_ust.asset_deposit_value

	df = df_luna[['asset_address','pre_attack_luna_value']].dropna().merge(df_ust[['asset_address','pre_attack_ust_value']].dropna(), how='outer').fillna(0)
	for c in [ 'pre_attack_luna_value','pre_attack_ust_value' ]:
		df[c] = df[c].astype(int)
	df = df[ (df.pre_attack_luna_value > 0) | (df.pre_attack_ust_value > 0) ]

	df.to_csv('~/Downloads/pre_attack_terra.csv', index=False)

def f():
	conn = psycopg2.connect("dbname=suppliers user=postgres password=postgres")
	conn = psycopg2.connect("dbname=suppliers user=postgres password=postgres")
	conn = psycopg2.connect(
		host="vic5o0tw1w-repl.twtim97jsb.tsdb.cloud.timescale.com",
		user="tsdbadmin",
		password="yP4wU5bL0tI0kP3k"
	)

query = '''
	SELECT from_addr
	, to_addr
	, asset
	, amount_e8
	, block_timestamp
	, COUNT(1) AS n
	FROM midgard.transfer_events
	WHERE block_timestamp < 1650000000000000000
	AND block_timestamp >=  1640000000000000000
	GROUP BY 1, 2, 3, 4, 5
	HAVING COUNT(1) > 1
'''
df = pd.read_sql_query(query, conn)
cur.execute(query)

it = 0
qs = []
for i in range(1618000000000000000, 1657000000000000000, 3000000000000000):
	print(i)
	it += 1
	query = '''
		SELECT from_addr
		, to_addr
		, asset
		, amount_e8
		, block_timestamp
		, COUNT(1) AS n
		FROM midgard.transfer_events
		WHERE block_timestamp >= {}
		AND block_timestamp <  {}
		GROUP BY 1, 2, 3, 4, 5
		HAVING COUNT(1) > 1
	'''.format(i, i + 3000000000000000)
	with open('/Users/kellenblumberg/Downloads/query_{}.txt'.format(it), 'w') as f:
		f.write(query)


def read_tokenlist():
	with open('./data/solana_tokenlist.json', 'r') as f:
		j = json.loads(f.read())
	j.keys()
	df = pd.DataFrame(j['tokens'])
	df[[ 'chainId','address','symbol','name','decimals','logoURI' ]].to_csv('~/Downloads/tokenlist.csv', index=False)

def idk():
	sales = pd.read_csv('./data/sales.csv')
	sales = pd.read_csv('./data/model_sales.csv')
	sorted(sales.collection.unique())
	# sales = sales[sales.exclude == 0]
	metadata = pd.read_csv('./data/metadata.csv')
	metadata = metadata[metadata.collection == COLLECTION]
	metadata[metadata.feature_name=='Role'].groupby('feature_value').token_id.count()
	sorted(metadata.feature_name.unique())
	sales = sales[sales.collection == COLLECTION]
	features = sorted(metadata.feature_name.unique())
	metadata = metadata.pivot( ['collection','token_id'], ['feature_name'], ['feature_value'] ).reset_index()
	metadata.columns = [ 'collection','token_id' ] + features
	sales['token_id'] = sales.token_id.astype(int)
	metadata['token_id'] = metadata.token_id.astype(int)
	df = sales.merge(metadata)
	# df.sort_values('sale_date', ascending=0).head()
	df = df.fillna('None')
	df['id'] = len(df)
	df['rel_price_0'] = (df.price - df.mn_20).apply(lambda x: max(0, x))
	df['rel_price_1'] = (df.price / df.mn_20).apply(lambda x: max(0, x-1))
	df[ (df.rel_price_0.notnull()) ].to_csv('./data/tableau_data.csv', index=False)

