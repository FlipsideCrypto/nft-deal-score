import re
import os
import json
import pandas as pd
import snowflake.connector

os.chdir('/Users/kellenblumberg/git/nft-deal-score')

from solana_model import get_sales
from scrape_sol_nfts import clean_name

def get_ctx():
	usr = os.getenv('SNOWFLAKE_USR')
	pwd = os.getenv('SNOWFLAKE_PWD')
	# with open('snowflake.pwd', 'r') as f:
	# 	pwd = f.readlines()[0].strip()
	# with open('snowflake.usr', 'r') as f:
	# 	usr = f.readlines()[0].strip()

	ctx = snowflake.connector.connect(
		user=usr,
		password=pwd,
		account='vna27887.us-east-1'
	)
	return(ctx)

def clean_colnames(df):
	names = [ x.lower() for x in df.columns ]
	df.columns = names
	return(df)

def overlap():
	query = '''
		WITH sales AS (
			SELECT l.label AS collection, SUM(sales_amount) AS volume, MIN(block_timestamp::date) AS first_sale_date
			FROM solana.fact_nft_sales s
			JOIN solana.dim_labels l ON LOWER(l.address) = LOWER(s.mint)
			WHERE block_timestamp >= CURRENT_DATE - 30
			GROUP BY 1
		), base AS (
			SELECT *
			, ROW_NUMBER() OVER (ORDER BY volume DESC) AS volume_rank
			FROM sales
			ORDER BY volume DESC
			LIMIT 50
		), b2 AS (
			SELECT DISTINCT collection, first_sale_date, volume_rank, purchaser, mint
			FROM solana.fact_nft_sales s
			JOIN solana.dim_labels l ON LOWER(l.address) = LOWER(s.mint)
			JOIN base b ON b.collection = l.label
			UNION 
			SELECT DISTINCT collection, first_sale_date, volume_rank, purchaser, mint
			FROM solana.fact_nft_mints m
			JOIN solana.dim_labels l ON LOWER(l.address) = LOWER(m.mint)
			JOIN base b ON b.collection = l.label
		)
		SELECT DISTINCT INITCAP(collection) AS collection, first_sale_date, date_trunc('month', first_sale_date) AS first_sale_month, volume_rank, purchaser, mint
		FROM b2
	'''
	ctx = get_ctx()
	df = ctx.cursor().execute(query)
	df = pd.DataFrame.from_records(iter(df), columns=[x[0] for x in df.description])
	df = clean_colnames(df)
	df[df.collection == 'okay bears']
	len(df[df.collection == 'okay bears'].mint.unique())
	data = []
	list(df.collection.unique()).index(a)
	list(df.collection.unique()).index(b)
	cur = df[df.volume_rank <= 50]
	for a in cur.collection.unique():
		print(a)
		a1 = set(cur[cur.collection == a].purchaser.unique())
		ar = cur[cur.collection == a].volume_rank.values[0]
		am = cur[cur.collection == a].first_sale_month.values[0]
		# for b in cur[cur.collection > a].collection.unique():
		for b in cur.collection.unique():
			b1 = set(cur[cur.collection == b].purchaser.unique())
			br = cur[cur.collection == b].volume_rank.values[0]
			bm = cur[cur.collection == b].first_sale_month.values[0]
			data += [[ a, b, int(a < b), am, bm, ar, br, len(a1), len(b1), len(a1.intersection(b1)) ]]
	cur = pd.DataFrame(data, columns=['col_1','col_2','include','am','bm','r_1','r_2','n_1','n_2','n_int'])
	cur['pct'] = cur.apply(lambda x: x['n_int'] / min(x['n_1'], x['n_2']), 1 )
	cur = cur[cur.n_int.notnull()]
	cur.to_csv('~/Downloads/overlap.csv', index=False)
	cur.include.unique()


def add_back_metadata():
	query = '''
		SELECT *
		FROM solana.dim_nft_metadata
		WHERE LOWER(project_name) IN (
		'degods'
		, 'astrals'
		, 'solstein'
		, 'solgods'
		, 'okay bears'
		, 'meerkat millionaires'
		, 'catalina whale mixer'
		, 'citizens by solsteads'
		, 'defi pirates'
		)
	'''
	ctx = get_ctx()
	mdf = ctx.cursor().execute(query)
	mdf = pd.DataFrame.from_records(iter(mdf), columns=[x[0] for x in mdf.description])
	print('Loaded {} metadata'.format(len(mdf)))
	mdf = clean_colnames(mdf)
	mdf = mdf[[ 'contract_name','token_id','token_metadata' ]]
	m = json.loads(mdf.token_metadata.values)
	m = [json.loads(x) for x in mdf.token_metadata.values]
	data = []
	collection = mdf.contract_name.values
	token_id = mdf.token_id.values
	for i in range(len(m)):
		for k, v in m[i].items():
			data += [[ collection[i], token_id[i], k, v ]]
	old = pd.read_csv('./data/metadata.csv')
	metadata = pd.DataFrame(data, columns=['collection','token_id','feature_name','feature_value'])
	del old['chain']
	old = old.append(metadata)
	old['collection'] = old.collection.apply(lambda x: clean_name(x) )
	old = old.drop_duplicates(subset=['collection','token_id','feature_name'], keep='last')
	old[old.collection == 'Cets On Creck'].feature_name.unique()
	old[old.collection == 'Cets on Creck'].feature_name.unique()
	tmp = old[['collection','feature_name']].drop_duplicates().groupby('collection').feature_name.count().reset_index()
	tmp.to_csv('~/Downloads/tmp-1.csv', index=False)
	old.to_csv('./data/metadata.csv', index=False)

def add_sf_metadata():
	old = pd.read_csv('./data/metadata.csv')
	l0 = len(old)
	sf_metadata = pd.read_csv('./data/sf_metadata.csv')
	old.head()
	sf_metadata.head()
	sf_metadata['collection'] = sf_metadata.collection.apply(lambda x: clean_name(x) )
	sorted(sf_metadata.collection.unique())
	sf_metadata['chain'] = 'Solana'
	a = sf_metadata[sf_metadata.collection.isin(['DeFi Pirates','Cets On Creck','Astrals'])][list(old.columns)]
	a = sf_metadata[sf_metadata.collection.isin(['Cets on Creck'])][list(old.columns)]
	a['tmp'] = a.feature_value.apply(lambda x: len(x) if type(x) == str else 0 )
	a = a[a.tmp < 35]
	a['feature_value'] = a.feature_value.fillna('None')
	a[a.feature_value.isnull()].feature_name.unique()
	a[a.feature_value.isnull()].groupby('feature_name').head(1)
	a[a.feature_value.isnull()].groupby('feature_name').count()
	del a['tmp']
	# old = old[-(old.collection.isin(a.collection.unique()))]
	old = old.append(a)
	old['collection'] = old.collection.apply(lambda x: clean_name(x))
	old = old.drop_duplicates(subset=['collection','token_id','feature_name'], keep='last')
	l1 = len(old)
	print('Added {} rows'.format(l1 - l0))
	print(sorted(old.collection.unique()))
	old[old.collection == 'Cets on Creck'].sort_values('token_id').head(20)
	old[(old.collection == 'Cets on Creck') & (old.token_id == 2059)]
	old.to_csv('./data/metadata.csv', index=False)
	a.collection.unique()

def add_sf_tokens():
	old = pd.read_csv('./data/tokens.csv')
	old.loc[ old.collection == 'Cets on Creck', 'chain' ] = 'Solana'
	l0 = len(old)
	r = pd.read_csv('./data/solana_rarities.csv')
	old.head()
	r.head()
	r['collection'] = r.collection.apply(lambda x: clean_name(x) )
	sorted(r.collection.unique())
	r['chain'] = 'Solana'
	r['clean_token_id'] = r.token_id
	r['market_url'] = None
	a = r[r.collection.isin(['DeFi Pirates','Cets On Creck','Astrals'])][list(old.columns)]
	a = r[r.collection.isin(['Cets on Creck'])][list(old.columns)]
	sorted(r.collection.unique())
	a.collection.unique()
	old = old[-(old.collection.isin(a.collection.unique()))]
	old = old.append(a)
	l1 = len(old)
	print('Added {} rows'.format(l1 - l0))
	old[old.collection == 'Cets on Creck']
	# old['clean_token_id'] = old.clean_token_id.astype(int)
	old.to_csv('./data/tokens.csv', index=False)

def add_rarity_from_metadata_to_token():
	metadata = pd.read_csv('./data/metadata.csv')
	metadata = metadata[ (metadata.collection.isin(['Cets on Creck'])) & (metadata.feature_name == 'nft_rank' ) ][[ 'collection','token_id','feature_value' ]].rename(columns={'feature_value':'nft_rank'})
	old = pd.read_csv('./data/tokens.csv')
	old = old.merge( metadata, how='left', on=['collection','token_id'] )
	old['nft_rank'] = old.nft_rank_y.fillna(old.nft_rank_x)
	del old['nft_rank_x']
	del old['nft_rank_y']
	old['clean_token_id'] = old.clean_token_id.fillna(old.token_id).astype(int)
	old.to_csv('./data/tokens.csv', index=False)
	old[old.collection == 'Cets on Creck']

def add_matching():
	old = pd.read_csv('./data/metadata.csv')
	cur = old[old.collection == 'Solana Monkey Business']
	cur['token_id'] = cur.token_id.astype(int)
	hat = cur[ (cur.feature_name == 'Hat') & (cur.feature_value != 'White Headset') ]
	hat['color'] = hat.feature_value.apply(lambda x: re.split(' ', x)[0] )
	sorted(hat.feature_value.unique())
	sorted(hat.color.unique())
	clothes = cur[ cur.feature_name == 'Clothes' ]
	clothes['color'] = clothes.feature_value.apply(lambda x: 'White' if 'Beige Smoking' in x else re.split(' ', x)[0] )
	matching = hat[['token_id','color']].merge(clothes[['token_id','color']])
	a = cur[['collection','token_id','chain']].drop_duplicates()
	a['feature_name'] = 'Matching'
	a['feature_value'] = a.token_id.apply( lambda x: 'Yes' if x in matching.token_id.unique() else 'No')
	old = old.append(a)
	old.to_csv('./data/metadata.csv', index=False)

def add_tokens():
	old = pd.read_csv('./data/tokens.csv')
	l0 = len(old)
	old = old[ old.collection != 'Galactic Angels' ]
	tmp = pd.read_csv('./data/metadata/Galactic Angels.csv')[['TOKEN_ID','IMAGE_URL']]
	tmp.columns = [ x.lower() for x in tmp.columns ]
	tmp['token_id'] = tmp.token_id.astype(str)
	metadata = pd.read_csv('./data/metadata.csv')
	cur = metadata[ (metadata.collection == 'Galactic Angels') & (metadata.feature_name == 'nft_rank') ][['collection','token_id','feature_value']].rename(columns={'feature_value':'nft_rank'})
	cur['chain'] = 'Terra'
	cur['clean_token_id'] = cur.token_id
	cur['token_id'] = cur.token_id.astype(str)
	cur = cur.merge(tmp, how='left', on=['token_id'])
	cur['market_url'] = cur.token_id.apply(lambda x: 'https://randomearth.io/items/terra13nccm82km0ttah37hkygnvz67hnvkdass24yzv_{}'.format(x) )
	old = old.append(cur)
	cur['token_id'] = cur.token_id.astype(str)
	old['token_id'] = old.token_id.astype(str)
	old = old.drop_duplicates(subset=['collection','token_id'], keep='last')
	l1 = len(old)
	old[old.collection == 'Galactic Angels']
	old[old.collection == 'Galactic Angels'].image_url.values[:3]
	print('Adding {} rows'.format(l1 - l0))
	old.to_csv('./data/tokens.csv', index=False)

def add_att_count():
	m_df = pd.read_csv('./data/metadata.csv')
	l0 = len(m_df)
	print(len(m_df))
	collection = 'Cets on Creck'
	cur = m_df[m_df.collection == collection]
	cur['feature_name'] = cur.feature_name.apply(lambda x: 'Clothes' if x == 'Clother' else x )
	cur['feature_value'] = cur.feature_value.fillna('None')
	sorted(cur.feature_name.unique())
	g = cur[(cur.feature_value != 'None') & (-cur.feature_name.isin(['Background','nft_rank','adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2','attribute_count','Attribute Count']))]
	g = g.groupby(['collection','token_id']).feature_value.count().reset_index()
	g.columns = [ 'collection', 'token_id','feature_value' ]
	g.groupby('feature_value').token_id.count()
	g['feature_name'] = 'Attribute Count'
	g['chain'] = 'Solana'
	print(g.groupby('feature_value').token_id.count())
	g[g.feature_value == 3356]
	cur[cur.token_id == 3356]
	# g['chain'] = 'Terra' if False else 'Solana'
	# cur = cur[cur.feature_name != 'Attribute Count']
	m_df = m_df[ -((m_df.collection == collection) & (m_df.feature_name.isin(['attribute_count', 'Attribute Count']))) ]
	# print(len(m_df))
	m_df = m_df.append(g)
	l1 = len(m_df)
	print('Adding {} rows'.format(l1 - l0))
	m_df.to_csv('./data/metadata.csv', index=False)


def tmp():
	m1 = pd.read_csv('./data/metadata.csv')
	m2 = pd.read_csv('./data/metadata_2.csv')
	t1 = pd.read_csv('./data/tokens.csv')
	t2 = pd.read_csv('./data/tokens_2.csv')
	m = m1.append(m2).drop_duplicates(keep='last')
	t = t1.append(t2).drop_duplicates(keep='last')
	t.to_csv('./data/tokens.csv', index=False)
	m.to_csv('./data/metadata.csv', index=False)

def add_rarities():
	include = [ 'DeGods' ]
	m_df = pd.read_csv('./data/metadata.csv')
	# m_df = m_df[-m_df.collection.isin([''])]
	g0 = m_df.groupby('collection').token_id.count().reset_index()

	m_df['collection'] = m_df.collection.apply(lambda x: clean_name(x))
	# m_df = m_df[m_df.collection.isin(include)]
	# m_df['feature_name'] = m_df.feature_name.fillna(m_df.name)
	# m_df['feature_value'] = m_df.feature_value.fillna(m_df.value)
	for c in [ 'name','value','rarity' ]:
		if c in m_df.columns:
			del m_df[c]
	# m_df[m_df.rarity.notnull()]
	# m_df[(m_df.rarity.notnull()) & (m_df.token_id == '6435')]
	m_df[(m_df.collection == 'SOLGods') & (m_df.token_id == 6435)]
	l0 = len(m_df)
	# m_df['tmp'] = m_df.apply(lambda x: x['collection'] in ['Cets on Creck'] and type(x['feature_value']) == str and len(x['feature_value']) > 35, 1 )
	# m_df[m_df.tmp == 1]
	# m_df = m_df[m_df.tmp == 0]
	# del m_df['tmp']
	# m_df[ (m_df.collection == 'Cets on Creck') & (m_df.feature_name == 'nft_rank') ]
	# a = len(m_df[ (m_df.collection == 'Cets on Creck') & (m_df.feature_name == 'nft_rank') ].token_id.unique())
	# b = len(m_df[ (m_df.collection == 'Cets on Creck') ].token_id.unique())
	# print('a = {}. b = {}'.format(a, b))
	# l0 = len(m_df)
	# m_df[m_df.collection == 'BAYC'].feature_name.unique()

	tokens = pd.read_csv('./data/tokens.csv')[['collection','token_id','nft_rank']]
	tokens['collection'] = tokens.collection.apply(lambda x: clean_name(x))
	# tokens = tokens[tokens.collection.isin(include)]
	tokens[((tokens.collection == 'Pesky Penguins')) & (tokens.token_id=='6437')]
	tokens[((tokens.collection == 'Pesky Penguins')) & (tokens.token_id==6437)]
	tokens[tokens.collection == 'SOLGods']
	solana_rarities = pd.read_csv('./data/solana_rarities.csv')
	sorted(solana_rarities.collection.unique())
	solana_rarities[solana_rarities.collection == 'cets-on-creck']
	solana_rarities = solana_rarities[solana_rarities.collection != 'cets-on-creck']
	solana_rarities = solana_rarities[solana_rarities.collection != 'peskypenguinclub']
	sorted(solana_rarities.collection.unique())
	ga_ranks = pd.read_csv('./data/metadata/Galactic Angels.csv')[['nft_rank','token_id']]
	ga_ranks['collection'] = 'Galactic Angels'
	ga_ranks[ (ga_ranks.collection == 'Galactic Angels') & (ga_ranks.token_id == 1) ]
	lp_ranks = pd.read_csv('./data/lp_ranks.csv')
	gp_ranks = pd.read_csv('./data/gp_ranks.csv')

	lev_egg_ranks = m_df[m_df.feature_name == 'collection_rank'][['collection','token_id','feature_value']].rename(columns={'feature_value':'nft_rank'})
	lev_egg_ranks['nft_rank'] = lev_egg_ranks.nft_rank.astype(int)
	m = m_df[m_df.feature_name == 'nft_rank'][['collection','token_id','feature_value']].rename(columns={'feature_value':'nft_rank'})
	# ga_ranks = m_df[ (m_df.collection == 'Galactic Angels') & (m_df.feature_name == 'nft_rank')][['collection','token_id','feature_value']].rename(columns={'feature_value':'nft_rank'})
	# ga_ranks['nft_rank'] = ga_ranks.nft_rank.astype(float).astype(int)
	if False:
		metadata = pd.read_csv('./data/metadata.csv')
		levana_ranks = metadata[(metadata.collection == 'Levana Dragon Eggs') & (metadata.feature_name == 'collection_rank')]
		levana_ranks['feature_value'] = levana_ranks.feature_value.apply(lambda x: int(x))
		levana_ranks['adj_nft_rank_0'] = levana_ranks.feature_value.apply(lambda x: (x+1) ** -0.2 )
		levana_ranks['adj_nft_rank_1'] = levana_ranks.feature_value.apply(lambda x: (x+1) ** -0.9 )
		levana_ranks['adj_nft_rank_2'] = levana_ranks.feature_value.apply(lambda x: (x+1) ** -1.4 )
		metadata = metadata[ -( (metadata.collection == 'Levana Dragon Eggs') & (metadata.feature_name.isin(['adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2'])) ) ]
		for c in ['adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2']:
			cur = levana_ranks[[ 'collection','token_id',c ]].rename(columns={c:'feature_value'})
			cur['feature_name'] = c
			metadata = metadata.append(cur)
		metadata['chain'] = metadata.collection.apply(lambda x: 'Terra' if x in ['LunaBulls','Galactic Punks','Levana Dragon Eggs'] else 'Solana' )
		metadata.to_csv('./data/metadata.csv', index=False)

	rarities = solana_rarities.append(lp_ranks).append(gp_ranks).append(lev_egg_ranks).append(ga_ranks).append(tokens).append(m)[[ 'collection','token_id','nft_rank' ]].dropna()
	rarities['collection'] = rarities.collection.apply(lambda x: clean_name(x) )
	sorted(rarities.collection.unique())
	rarities['token_id'] = rarities.token_id.astype(str)
	rarities = rarities.drop_duplicates(subset=['collection','token_id'], keep='first')
	rarities['nft_rank'] = rarities.nft_rank.astype(int)
	rarities.loc[ (rarities.collection == 'Solana Monkey Business') & (rarities.token_id == 903) , 'nft_rank' ] = 18
	rarities[ (rarities.collection == 'Galactic Angels') & (rarities.token_id == '1') ]
	rarities[ (rarities.collection == 'Cets on Creck') & (rarities.token_id == '1554') ]
	rarities[ (rarities.collection == 'Cets on Creck') & (rarities.token_id == '1877') ]

	rarities['adj_nft_rank_0'] = rarities.nft_rank.apply(lambda x: (x+1) ** -0.2 )
	rarities['adj_nft_rank_1'] = rarities.nft_rank.apply(lambda x: (x+1) ** -0.9 )
	rarities['adj_nft_rank_2'] = rarities.nft_rank.apply(lambda x: (x+1) ** -1.4 )
	print(rarities.groupby('collection').count())

	print(solana_rarities.groupby('collection').token_id.count())
	print(rarities.groupby('collection').token_id.count())
	print(rarities[rarities.collection.isin(['Catalina Whale Mixer','Okay Bears'])].groupby('collection').token_id.count())

	# m_df[ m_df.feature_name.isin(['nft_rank','adj_nft_rank_0','adj_nft_rank_1']) ].groupby('collection').token_id.count()
	m_df = m_df[ -m_df.feature_name.isin(['Nft Rank','nft_rank','adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2']) ]

	m_df['token_id'] = m_df['token_id'].astype(str)
	rarities['token_id'] = rarities['token_id'].astype(str)
	m_df = m_df.drop_duplicates()
	m_df = m_df.fillna('None')
	print(m_df[(m_df.token_id=='1') & (m_df.collection == 'Solana Monkey Business')])
	print(m_df[(m_df.token_id=='10') & (m_df.collection == 'Aurory')])

	sorted(m_df.feature_name.unique())

	g = m_df[m_df.collection == 'BAYC'].sort_values(['token_id','feature_name'])
	g.head(20)
	len(g)
	len(g.feature_name.unique())

	fill_missing_metadata = False
	if fill_missing_metadata:
		c = 'Solana Monkey Business'
		c = 'Meerkat Millionaires'
		for c in m_df.collection.unique():
			l1 = len(m_df)
			cur = m_df[m_df.collection == c]
			base = cur[[ 'collection','token_id','chain' ]].drop_duplicates()
			for f in cur.feature_name.unique():
				exists = set(cur[cur.feature_name==f].token_id.unique())
				missing = sorted(list(set(base.token_id).difference(exists)))
				a = base[ base.token_id.isin(missing) ]
				a['feature_name'] = f
				a['feature_value'] = 'None'
				m_df = m_df.append(a)
			print('{}: Adding {} rows'.format(c, len(m_df) - l1))

	print(m_df[(m_df.token_id=='1') & (m_df.collection == 'Solana Monkey Business')])
	print(m_df[(m_df.token_id=='10') & (m_df.collection == 'Aurory')])

	for c in [ 'nft_rank','adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2' ]:
		cur = rarities[[ 'collection','token_id',c ]].rename(columns={c: 'feature_value'})
		cur['feature_name'] = c
		m_df = m_df[ m_df.feature_name != c ]
		m_df = m_df.append(cur)


	m_df = m_df.drop_duplicates(subset=['collection','token_id','feature_name'], keep='last')

	print(m_df[(m_df.token_id=='1') & (m_df.collection == 'Solana Monkey Business')])
	print(m_df[(m_df.token_id=='10') & (m_df.collection == 'Aurory')])

	m_df['feature_value'] = m_df.feature_value.apply(lambda x: x.strip() if type(x) == str else x )
	m_df['chain'] = m_df.collection.apply(lambda x: 'Terra' if x in ['LunaBulls','Galactic Punks','Levana Dragon Eggs'] else 'Solana' )

	g = m_df[['collection','token_id']].drop_duplicates().groupby('collection').token_id.count().reset_index()
	a = m_df.groupby('collection').token_id.count().reset_index().rename(columns={'token_id':'atts'})
	g = g.merge(a)
	g['rat'] = g.atts / g.token_id
	print(g)

	m_df[m_df.collection == 'Stoned Ape Crew'].feature_name.unique()
	# m_df[m_df.collection == 'Levana Dragon Eggs'].feature_name.unique()
	m_df = m_df[-((m_df.collection == 'Cets on Creck') & (m_df.token_id == '0'))]
	m_df[((m_df.collection == 'Cets on Creck') & (m_df.token_id == '1'))]
	m_df[((m_df.collection == 'SOLGods') & (m_df.token_id == '1'))]
	m_df[((m_df.collection == 'Meerkat Millionaires'))]
	m_df[((m_df.collection == 'Pesky Penguins')) & (m_df.token_id=='6437')]
	sorted(m_df.collection.unique())

	l1 = len(m_df)
	g1 = m_df.groupby('collection').token_id.count().reset_index()
	g = g0.merge(g1, how='outer', on=['collection'])
	g['dff'] = g.token_id_y - g.token_id_x
	print(g[g.dff != 0].sort_values('dff', ascending=0))
	print('Adding {} rows'.format(l1 - l0))
	# m_df[m_df.collection == 'Galactic Angels']
	# m_df[ (m_df.collection == 'Galactic Angels') & (m_df.token_id == '1') ]
	m_df.to_csv('./data/metadata.csv', index=False)

