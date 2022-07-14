from curses import meta
import re
import os
import math
import json
from typing import Collection
from nbformat import write
import pandas as pd
import snowflake.connector

os.chdir('/Users/kellenblumberg/git/nft-deal-score')
from utils import merge
from scrape_sol_nfts import clean_name


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

def solana_2():
	mints = pd.read_csv('./data/solana_mints_3.csv')

	collection = mints.collection.values[0]
	collection_dir = re.sub(' ', '', collection)
	mints['image_url'] = 'https://wblrifk3oz3qpmqbxsunfvtmzi3c6vafn3un4f57ysesotlale.arweave.net/sFcUFVt2dweyAbyo0tZsyj-YvVAVu6N4Xv8SJJ01gWQ?ext=gif'
	mints['token_metadata_uri'] = 'https://arweave.net/8pJZCyLAY9TTAyuOB__2P_OwsmLzQEeKhLo5Ojiwflk'
	results = []
	token_id = 0
	for row in mints.iterrows():
		row = row[1]
		token_metadata = {}
		mint_address = row['mint_address']

		d = {
			'commission_rate': None
			, 'mint_address': mint_address
			, 'token_id': token_id
			, 'contract_address': mint_address
			, 'contract_name': row['collection']
			, 'created_at_block_id': 0
			, 'created_at_timestamp': str('2021-01-01')
			, 'created_at_tx_id': ''
			, 'creator_address': mint_address
			, 'creator_name': row['collection']
			, 'image_url': row['image_url']
			, 'project_name': row['collection']
			, 'token_id': int(token_id)
			, 'token_metadata': token_metadata
			, 'token_metadata_uri': row['token_metadata_uri']
			, 'token_name': row['collection']
		}
		results.append(d)
		token_id += 1
	print('Uploading {} results'.format(len(results)))

	dir = './data/metadata/{}/'.format(collection)
	if not os.path.exists(dir):
		os.makedirs(dir)

	n = 50
	r = math.ceil(len(results) / n)
	for i in range(r):
		newd = {
			"model": {
				"blockchain": "solana",
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
		with open('./data/metadata/{}/{}.txt'.format(collection, i), 'w') as outfile:
			outfile.write(json.dumps(newd))

def levana():
	query = '''
		WITH legendary_traits AS (
			SELECT block_timestamp,
			block_id,
			tx_id,
			msg_value:execute_msg:mint:extension:name::string as name,
			msg_value:execute_msg:mint:extension:image::string as image,
			msg_value:execute_msg:mint:token_id::float as tokenid,
			msg_value:execute_msg:mint:extension:attributes[0]:value::string as rarity,
			msg_value:execute_msg:mint:extension:attributes[1]:value::string as rank,
			msg_value:execute_msg:mint:extension:attributes[2]:value::string as origin,
			msg_value:execute_msg:mint:extension:attributes[3]:value::string as cracking_date,
			msg_value:execute_msg:mint:extension:attributes[4]:value::string as essence,
			msg_value:execute_msg:mint:extension:attributes[5]:value::string as rune,
			msg_value:execute_msg:mint:extension:attributes[6]:value::string as infusion,
			msg_value:execute_msg:mint:extension:attributes[7]:value::string as affecting_moon,
			msg_value:execute_msg:mint:extension:attributes[8]:value::string as lucky_number,
			msg_value:execute_msg:mint:extension:attributes[9]:value::string as constellation,
			msg_value:execute_msg:mint:extension:attributes[10]:value::string as temperature,
			msg_value:execute_msg:mint:extension:attributes[11]:value::string as weight,
			msg_value:execute_msg:mint:extension:attributes[12]:value::string as family,
			msg_value:execute_msg:mint:extension:attributes[13]:value::string as genus,
			msg_value:execute_msg:mint:extension:attributes[18]:value::string as shower,
			msg_value:execute_msg:mint:extension:attributes[19]:value::string as meteor_id,
			msg_value:execute_msg:mint:extension:attributes[14]:value::string as legendary_composition,
			msg_value:execute_msg:mint:extension:attributes[15]:value::string as ancient_composition,
			msg_value:execute_msg:mint:extension:attributes[16]:value::string as rare_composition,
			msg_value:execute_msg:mint:extension:attributes[17]:value::string as common_composition
			from terra.msgs 
			where msg_value:contract::string = 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' 
			and	msg_value:sender::string = 'terra1awy9ychm2z2hd696kz6yeq67l30l7nxs7n762t'
			and	msg_value:execute_msg:mint is not null
			and	tx_status = 'SUCCEEDED'
			and	rarity = 'Legendary'
		),
		ancient_traits as (
			SELECT block_timestamp,
			block_id,
			tx_id,
			msg_value:execute_msg:mint:extension:name::string as name,
			msg_value:execute_msg:mint:extension:image::string as image,
			msg_value:execute_msg:mint:token_id::float as tokenid,
			msg_value:execute_msg:mint:extension:attributes[0]:value::string as rarity,
			msg_value:execute_msg:mint:extension:attributes[1]:value::string as rank,
			msg_value:execute_msg:mint:extension:attributes[2]:value::string as origin,
			msg_value:execute_msg:mint:extension:attributes[3]:value::string as cracking_date,
			msg_value:execute_msg:mint:extension:attributes[4]:value::string as essence,
			msg_value:execute_msg:mint:extension:attributes[5]:value::string as rune,
			msg_value:execute_msg:mint:extension:attributes[6]:value::string as infusion,
			msg_value:execute_msg:mint:extension:attributes[7]:value::string as affecting_moon,
			msg_value:execute_msg:mint:extension:attributes[8]:value::string as lucky_number,
			msg_value:execute_msg:mint:extension:attributes[9]:value::string as constellation,
			msg_value:execute_msg:mint:extension:attributes[10]:value::string as temperature,
			msg_value:execute_msg:mint:extension:attributes[11]:value::string as weight,
			msg_value:execute_msg:mint:extension:attributes[12]:value::string as family,
			msg_value:execute_msg:mint:extension:attributes[13]:value::string as genus,
			msg_value:execute_msg:mint:extension:attributes[17]:value::string as shower,
			msg_value:execute_msg:mint:extension:attributes[18]:value::string as meteor_id,
			null																 as legendary_composition,
			msg_value:execute_msg:mint:extension:attributes[14]:value::string as ancient_composition,
			msg_value:execute_msg:mint:extension:attributes[15]:value::string as rare_composition,
			msg_value:execute_msg:mint:extension:attributes[16]:value::string as common_composition
			from terra.msgs 
			where msg_value:contract::string = 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' 
			and	msg_value:sender::string = 'terra1awy9ychm2z2hd696kz6yeq67l30l7nxs7n762t'
			and	msg_value:execute_msg:mint is not null
			and	tx_status = 'SUCCEEDED'
			and	rarity = 'Ancient'
		),
		rare_traits as (
			SELECT block_timestamp,
			block_id,
			tx_id,
			msg_value:execute_msg:mint:extension:name::string as name,
			msg_value:execute_msg:mint:extension:image::string as image,
			msg_value:execute_msg:mint:token_id::float as tokenid,
			msg_value:execute_msg:mint:extension:attributes[0]:value::string as rarity,
			msg_value:execute_msg:mint:extension:attributes[1]:value::string as rank,
			msg_value:execute_msg:mint:extension:attributes[2]:value::string as origin,
			msg_value:execute_msg:mint:extension:attributes[3]:value::string as cracking_date,
			msg_value:execute_msg:mint:extension:attributes[4]:value::string as essence,
			msg_value:execute_msg:mint:extension:attributes[5]:value::string as rune,
			msg_value:execute_msg:mint:extension:attributes[6]:value::string as infusion,
			msg_value:execute_msg:mint:extension:attributes[7]:value::string as affecting_moon,
			msg_value:execute_msg:mint:extension:attributes[8]:value::string as lucky_number,
			msg_value:execute_msg:mint:extension:attributes[9]:value::string as constellation,
			msg_value:execute_msg:mint:extension:attributes[10]:value::string as temperature,
			msg_value:execute_msg:mint:extension:attributes[11]:value::string as weight,
			msg_value:execute_msg:mint:extension:attributes[12]:value::string as family,
			msg_value:execute_msg:mint:extension:attributes[13]:value::string as genus,
			msg_value:execute_msg:mint:extension:attributes[16]:value::string as shower,
			msg_value:execute_msg:mint:extension:attributes[17]:value::string as meteor_id,
			null																 as legendary_composition,
			null																 as ancient_composition,
			msg_value:execute_msg:mint:extension:attributes[14]:value::string as rare_composition,
			msg_value:execute_msg:mint:extension:attributes[15]:value::string as common_composition
			from terra.msgs 
			where msg_value:contract::string = 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' 
			and	msg_value:sender::string = 'terra1awy9ychm2z2hd696kz6yeq67l30l7nxs7n762t'
			and	msg_value:execute_msg:mint is not null
			and	tx_status = 'SUCCEEDED'
			and	rarity = 'Rare'
		),

		common_traits as (
			SELECT block_timestamp,
			block_id,
			tx_id,
			msg_value:execute_msg:mint:extension:name::string as name,
			msg_value:execute_msg:mint:extension:image::string as image,
			msg_value:execute_msg:mint:token_id::float as tokenid,
			msg_value:execute_msg:mint:extension:attributes[0]:value::string as rarity,
			msg_value:execute_msg:mint:extension:attributes[1]:value::string as rank,
			msg_value:execute_msg:mint:extension:attributes[2]:value::string as origin,
			msg_value:execute_msg:mint:extension:attributes[3]:value::string as cracking_date,
			msg_value:execute_msg:mint:extension:attributes[4]:value::string as essence,
			msg_value:execute_msg:mint:extension:attributes[5]:value::string as rune,
			msg_value:execute_msg:mint:extension:attributes[6]:value::string as infusion,
			msg_value:execute_msg:mint:extension:attributes[7]:value::string as affecting_moon,
			msg_value:execute_msg:mint:extension:attributes[8]:value::string as lucky_number,
			msg_value:execute_msg:mint:extension:attributes[9]:value::string as constellation,
			msg_value:execute_msg:mint:extension:attributes[10]:value::string as temperature,
			msg_value:execute_msg:mint:extension:attributes[11]:value::string as weight,
			msg_value:execute_msg:mint:extension:attributes[12]:value::string as family,
			msg_value:execute_msg:mint:extension:attributes[13]:value::string as genus,
			msg_value:execute_msg:mint:extension:attributes[15]:value::string as shower,
			msg_value:execute_msg:mint:extension:attributes[16]:value::string as meteor_id,
			null																 as legendary_composition,
			null																 as ancient_composition,
			null																 as rare_composition,
			msg_value:execute_msg:mint:extension:attributes[14]:value::string as common_composition
			from terra.msgs 
			where msg_value:contract::string = 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' 
			and	msg_value:sender::string = 'terra1awy9ychm2z2hd696kz6yeq67l30l7nxs7n762t'
			and	msg_value:execute_msg:mint is not null
			and	tx_status = 'SUCCEEDED'
			and	rarity = 'Common'
		),

		combine AS
		(
			SELECT * from legendary_traits
			UNION ALL
			SELECT * from ancient_traits
			UNION ALL
			SELECT * from rare_traits
			UNION ALL
			SELECT * from common_traits
		)

		SELECT * from combine order by tokenid
	'''
	metadata = ctx.cursor().execute(query)
	metadata = pd.DataFrame.from_records(iter(metadata), columns=[x[0] for x in metadata.description])
	metadata = clean_colnames(metadata)
	metadata.head()
	metadata[ metadata.tokenid == '0027' ]
	metadata[ metadata.tokenid == 27 ]
	metadata.columns
	results = []
	for row in metadata.iterrows():
		row = row[1]
		token_metadata = {}
		for a in list(metadata.columns):
			if not a in [ 'block_timestamp','block_id','tx_id','name','tokenid','image' ]:
				token_metadata[a] = row[a]
		d = {
			'commission_rate': None
			, 'contract_address': 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg'
			, 'contract_name': 'levana_dragon_eggs'
			, 'created_at_block_id': 0
			, 'created_at_timestamp': str(row['block_timestamp'])
			, 'created_at_tx_id': row['tx_id']
			, 'creator_address': 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg'
			, 'creator_name': 'levana_dragon_eggs'
			, 'image_url': row['image']
			, 'project_name': 'levana_dragon_eggs'
			, 'token_id': row['tokenid']
			, 'token_metadata': token_metadata
			, 'token_metadata_uri': row['image']
			, 'token_name': row['name']
		}
		results.append(d)
	print('Uploading {} results'.format(len(results)))

	n = 50
	r = math.ceil(len(results) / n)
	for i in range(r):
		newd = {
			"model": {
				"blockchain": "terra",
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
		with open('./data/metadata/Levana Dragon Eggs/{}.txt'.format(i), 'w') as outfile:
			outfile.write(json.dumps(newd))


def mint_to_token_id():
	data = []
	collection = 'DeGods'
	dir = './data/mints/DeGods/'
	for fname in os.listdir(dir):
		mint_address = fname[:-5]
		path = dir+fname
		with open(path) as f:
			s = f.read()
			d = json.loads(s)
			try:
				token_id = int(re.split('#', d['name'])[1])
				data += [[ collection, token_id, mint_address ]]
			except:
				pass
	data += [[ collection, 2508, 'CcWedWffikLoj3aYtgSK46LqF8LkLCgpu4RCo8RMxF3e' ]]
	df = pd.DataFrame(data, columns=['collection','token_id','mint_address']).drop_duplicates(subset=['token_id'], keep='last')
	[ x for x in range(1, 10001) if not x in df.token_id.unique() ]
	g = df.groupby('token_id').collection.count().reset_index()
	g[g.collection > 1]
	df.to_csv('./data/mint_to_token_id_map.csv', index=False)

def solana():
	mints = pd.read_csv('./data/solana_rarities.csv')
	sorted(mints.collection.unique())
	mints[mints.collection == 'smb'][['mint_address']].to_csv('~/Downloads/tmp.csv', index=False)
	collection_info = pd.read_csv('./data/collection_info.csv')
	metadata = pd.read_csv('./data/metadata.csv')
	metadata.collection.unique()
	tokens = pd.read_csv('./data/tokens.csv')
	tokens['token_id'] = tokens.token_id.astype(str)
	metadata['token_id'] = metadata.token_id.astype(str)
	metadata = metadata.merge(tokens)
	# metadata = metadata.merge(collection_info)
	metadata['token_id'] = metadata.clean_token_id.fillna(metadata.token_id)
	metadata = metadata[-metadata.feature_name.isin(['nft_rank','adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2'])]

	metadata['token_id'] = metadata.token_id.astype(int)
	mints['token_id'] = mints.token_id.astype(int)
	mints['collection'] = mints.collection.apply(lambda x: clean_name(x) )

	metadata = pd.read_csv('./data/sf_metadata.csv')
	print(sorted(metadata.collection.unique()))
	# metadata = metadata[-metadata.collection.isin([''])]
	metadata['image_url'] = 'None'
	metadata = metadata.drop_duplicates()

	a = metadata.groupby('collection').token_id.count().reset_index()
	b = metadata[metadata.feature_value.isnull()].groupby('collection').token_id.count().reset_index()
	c = a.merge(b, on=['collection'])
	c['pct'] = c.token_id_y / c.token_id_x
	c.sort_values('pct')
	metadata[ (metadata.feature_value.isnull()) & (metadata.collection == 'mindfolk')]

	metadata = pd.read_csv('./data/solscan_metadata.csv')
	assert(len(metadata[metadata.feature_value.isnull()]) == 0)
	metadata['collection'] = metadata.collection.apply(lambda x: re.sub('-', ' ', x).title() )
	metadata['collection'] = metadata.collection.apply(lambda x: re.sub(' Cc', ' CC', x) )
	metadata['collection'] = metadata.collection.apply(lambda x: re.sub('Og ', 'OG ', x) )

	collection = 'Cets On Creck'
	collection = 'Astrals'



	query = '''
		SELECT DISTINCT project_name
		FROM solana.dim_nft_metadata
	'''
	seen = ctx.cursor().execute(query)
	seen = pd.DataFrame.from_records(iter(seen), columns=[x[0] for x in seen.description])
	seen = clean_colnames(seen)
	seen = list(seen.project_name.values)
	seen = [ x.lower() for x in seen ]

	metadata = pd.read_csv('./data/metadata.csv')
	len(metadata)
	# print(sorted(metadata.collection.unique()))
	# metadata = metadata[metadata.collection == collection]
	# print(sorted(metadata.collection.unique()))
	metadata = metadata[-(metadata.feature_name.isin(['adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2','nft_rank']))]
	metadata[['collection']].drop_duplicates().to_csv('~/Downloads/tmp.csv', index=False)
	len(metadata.token_id.unique())
	# id_map = pd.read_csv('./data/mint_to_token_id_map.csv')
	id_map = pd.read_csv('./data/tokens.csv')
	tokens = pd.read_csv('./data/tokens.csv')
	tokens.collection.unique()
	len(tokens.collection.unique())
	cs = [ x for x in id_map.collection.unique() if not x.lower() in seen ]
	len(id_map.collection.unique())
	len(cs)
	id_map = id_map[id_map.collection.isin(cs)]
	metadata = metadata[metadata.collection.isin(cs)]

	# cs = metadata[metadata.chain.fillna('Solana') == 'Solana'].collection.unique()
	cs = metadata.collection.unique()
	id_map = id_map[id_map.collection.isin(cs)]
	metadata = metadata[metadata.collection.isin(cs)]
	sorted(id_map.collection.unique())
	sorted(metadata.collection.unique())

	# id_map['token_id'] = id_map.token_id.astype(int)
	# metadata['token_id'] = metadata.token_id.astype(int)
	id_map['token_id'] = id_map.token_id.astype(str)
	metadata['token_id'] = metadata.token_id.astype(str)

	metadata = merge(metadata, id_map[['collection','token_id','mint_address','image_url']], ensure = False)
	metadata = metadata[metadata.collection.isin(cs)]

	metadata['feature_name'] = metadata.feature_name.apply(lambda x: x.title() )
	# metadata['image_url'] = metadata.token_id.apply(lambda x: 'https://metadata.degods.com/g/{}.png'.format(x - 1) )
	metadata.head()
	metadata = metadata[-(metadata.feature_name.isin(['Nft_Rank','Adj_Nft_Rank_0','Adj_Nft_Rank_1','Adj_Nft_Rank_2']))]
	# print(metadata.groupby('feature_name').token_id.count().reset_index().sort_values('token_id', ascending=0).head(10))

	metadata = metadata[metadata.feature_name != 'L3G3Nd4Ry']

	# print(sorted(metadata.collection.unique()))
	# sorted(metadata[metadata.collection == collection].feature_name.unique())
	# sorted(metadata.feature_name.unique())

	# metadata[['collection']].drop_duplicates().to_csv('~/Downloads/tmp.csv', index=False)
	# Python code to convert into dictionary
	def Convert(tup, di):
		di = dict(tup)
		return di

	metadata = metadata[-metadata.collection.isin(['LunaBulls', 'Levana Dragon Eggs'])]
	metadata['token_id'] = metadata.token_id.astype(float)
	metadata['token_id'] = metadata.token_id.astype(int)
	metadata.groupby(['collection','feature_name']).token_id.count()
	metadata.head()
	metadata[metadata.mint_address.isnull()].collection.unique()
	assert(len(metadata[metadata.mint_address.isnull()]) == 0)
	dirs = sorted(list(set(os.listdir('./data/metadata/')).intersection(set(metadata.collection.unique()))))
	sorted(list(metadata.collection.unique()))
	# collection = 'Bubblegoose Ballers'
	it = 0
	tot = len(metadata.collection.unique())
	data = []
	for collection in metadata.collection.unique()[:1]:
		print('#{} / {}: {}'.format(it, tot, collection))
		mdf = metadata[metadata.collection == collection]
		df.groupby('Column1')[['Column2', 'Column3']].apply(lambda g: g.values.tolist()).to_dict()
		mdf.head(20).groupby(['collection','image_url','token_id'])[[ 'feature_name','feature_value' ]].apply(lambda g: g.values.tolist()).to_dict()

		mdf.head(20).groupby(['collection','image_url','token_id'])[[ 'feature_name','feature_value' ]].apply(lambda g: list(map(tuple, g.values.tolist())) ).to_dict()

		mdf.head(20).groupby(['collection','image_url','token_id'])[[ 'feature_name','feature_value' ]].apply(lambda g: Convert(list(map(tuple, g.values.tolist())), {}) ).to_dict()
		a = mdf.head(20).groupby(['collection','mint_address','token_id','image_url'])[[ 'feature_name','feature_value' ]].apply(lambda g: Convert(list(map(tuple, g.values.tolist())), {}) ).reset_index()


		a = metadata.groupby(['collection','mint_address','token_id','image_url'])[[ 'feature_name','feature_value' ]].apply(lambda g: Convert(list(map(tuple, g.values.tolist())), {}) ).reset_index()
		a.columns = ['collection','mint_address','token_id','image_url', 'token_metadata']
		a['commission_rate'] = None
		a['contract_address'] = a.mint_address
		a['contract_name'] = a.collection
		a['created_at_block_id'] = 0
		a['created_at_timestamp'] = '2021-01-01'
		a['created_at_tx_id'] = ''
		a['creator_address'] = a.mint_address
		a['creator_name'] = a.collection
		a['project_name'] = a.collection
		a['token_metadata_uri'] = a.image_url
		a['token_name'] = a.collection
		a.to_csv('./data/metadata/results.csv', index=False)
		a['n'] = range(len(a))
		a['n'] = a.n.apply(lambda x: int(x/50) )
		a['token_id'] = a.token_id.astype(int)
		cols = ['collection', 'mint_address', 'token_id', 'image_url', 'token_metadata',
       'commission_rate', 'contract_address', 'contract_name',
       'created_at_block_id', 'created_at_timestamp', 'created_at_tx_id',
       'creator_address', 'creator_name', 'project_name', 'token_metadata_uri',
       'token_name']

		n = 100000
		tot = int(len(a) / n) + 1
		for i in range(0, len(a), n):
			ind = int(i/n)
			print('#{} / {}'.format(ind, tot))
			g = a.head(i+n).tail(n).to_dict('records')
			txt  = [
				{
					"model": {
						"blockchain": "solana",
						"sinks": [
							{
								"destination": "{database_name}.silver.nft_metadata",
								"type": "snowflake",
								"unique_key": "blockchain || contract_address || token_id"
							}
						],
					},
					"results": g[x:x+50]
				}
				for x in range(0, len(g), 50)
			]
			w = pd.DataFrame({'ind': range(len(txt)), 'results':[json.dumps(x) for x in txt] })
			# w['results'] = w.results.apply(lambda x: x[1:-1] )
			w.to_csv('./data/metadata/results/{}.csv'.format(ind), index=False)
			# with open('./data/metadata/results/{}.json'.format(i), 'w') as outfile:
			# 	json.dump(results[i:i+100000], outfile)

		g = a.head(200).groupby('n')[cols].apply(lambda g: Convert(list(map(tuple, g.values.tolist())), {}) ).to_dict()
		g = a.head(200).groupby('n')[cols].apply(lambda g: (list(map(tuple, g.values.tolist())), {}) )
		g = a.head(200).groupby('n')[cols].apply(lambda g: g.values.tolist()).reset_index()
		g = a.head(200).to_dict('records')
		sorted(a.collection.unique())
		g = a[a.collection == 'Jungle Cats'].head(20000).to_dict('records')
		txt  = [
			{
				"model": {
					"blockchain": "solana",
					"sinks": [
						{
							"destination": "{database_name}.silver.nft_metadata",
							"type": "snowflake",
							"unique_key": "blockchain || contract_address || token_id"
						}
					],
				},
				"results": g[i:i+50]
			}
			for i in range(0, len(g), 50)
		]
		w = pd.DataFrame({'ind': range(len(txt)), 'results':[json.dumps(x) for x in txt] })
		# w['results'] = w.results.apply(lambda x: x[1:-1] )
		w.to_csv('./data/metadata/results.csv', index=False)
		with open('./data/metadata/results.txt', 'w') as outfile:
			outfile.write(json.dumps(txt))
		g = list(a.head(200).values)
		results = a.to_dict('records')
		for i in range(0, len(results), 100000):
			print(i)
			with open('./data/metadata/results/{}.json'.format(i), 'w') as outfile:
				json.dump(results[i:i+100000], outfile)

		n = 50
		r = math.ceil(len(results) / n)
		for i in range(r):
			print('#{} / {}'.format(i, r))
			newd = {
				"model": {
					"blockchain": "solana",
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
			data += [ json.dumps(newd) ]
			with open('./data/metadata/results/{}.txt'.format(collection, i), 'w') as outfile:
				outfile.write(json.dumps(newd))


		# results = []
		# for token_id in sorted(mdf.token_id.unique()):
		# 	if token_id % 1000 == 1:
		# 		print(token_id, len(results))
		# 	cur = mdf[mdf.token_id == token_id]
		# 	token_metadata = {}
		# 	# m = mints[(mints.collection == collection) & (mints.token_id == token_id) ]
		# 	m = metadata[(metadata.collection == collection) & (metadata.token_id == token_id) ]
		# 	m = m.fillna('None')
		# 	if not len(m):
		# 		print(token_id)
		# 		continue
		# 	# mint_address = m.mint_address.values[0] if 'mint_address' in m.columns else ''
		# 	mint_address = m.mint_address.values[0]
		# 	for row in cur.iterrows():
		# 		row = row[1]
		# 		token_metadata[row['feature_name']] = row['feature_value']

		# 	d = {
		# 		'commission_rate': None
		# 		, 'mint_address': mint_address
		# 		, 'token_id': token_id
		# 		, 'contract_address': mint_address
		# 		, 'contract_name': row['collection']
		# 		, 'created_at_block_id': 0
		# 		, 'created_at_timestamp': str('2021-01-01')
		# 		, 'created_at_tx_id': ''
		# 		, 'creator_address': mint_address
		# 		, 'creator_name': row['collection']
		# 		, 'image_url': row['image_url']
		# 		, 'project_name': row['collection']
		# 		, 'token_id': int(token_id)
		# 		, 'token_metadata': token_metadata
		# 		, 'token_metadata_uri': row['image_url']
		# 		, 'token_name': row['collection']
		# 	}
		# 	results.append(d)
		print('Uploading {} results'.format(len(results)))

		dir = './data/metadata/{}/'.format(collection)
		if not os.path.exists(dir):
			os.makedirs(dir)

		n = 50
		r = math.ceil(len(results) / n)
		for i in range(r):
			newd = {
				"model": {
					"blockchain": "solana",
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
			data += [ json.dumps(newd) ]
			with open('./data/metadata/{}/{}.txt'.format(collection, i), 'w') as outfile:
				outfile.write(json.dumps(newd))

def bayc():
	with open('./data/bayc.json') as f:
		j = json.load(f)
	results = []
	for row in j:
		token_metadata = {}
		for a in row['metadata']['attributes']:
			token_metadata[a['trait_type']] = a['value']
		d = {
			'commission_rate': None
			, 'contract_address': '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'
			, 'contract_name': 'bayc'
			, 'created_at_block_id': row['blockNumber']
			, 'created_at_timestamp': '2021-04-30 14:21:08.000'
			, 'created_at_tx_id': row['transactionHash']
			, 'creator_address': '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'
			, 'creator_name': 'bayc'
			, 'image_url': row['metadata']['image']
			, 'project_name': 'bored_ape_yacht_club'
			, 'token_id': row['id']
			, 'token_metadata': token_metadata
			, 'token_metadata_uri': row['uri']
			, 'token_name': 'BAYC #{}'.format(row['id'])
		}
		results.append(d)
	df = pd.DataFrame(results)
	g = df.groupby('token_id').contract_address.count().reset_index()
	g = g.sort_values('contract_address', ascending=0)
	# df = pd.DataFrame([[str(newd['model']), str(results[:2])]], columns=['model','results'])
	# df.to_csv('./data/bayc.csv', index=False)

	# s = str(json.dumps(newd))
	# str(newd)

	# s = str(json.loads(json.dumps(newd)))
	# s = re.sub('\\', '', s)


	# json.dumps(json.JSONDecoder().decode(newd))

	n = 50
	r = math.ceil(len(results) / n)
	for i in range(r):
		newd = {
			"model": {
				"blockchain": "ethereum",
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
		with open('./data/metadata/bayc/{}.txt'.format(i), 'w') as outfile:
			outfile.write(json.dumps(newd))

