from curses import meta
import re
import os
import math
import json
import pandas as pd
import snowflake.connector

os.chdir('/Users/kellenblumberg/git/nft-deal-score')
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

	print(metadata.collection.unique())

	# metadata[['collection']].drop_duplicates().to_csv('~/Downloads/tmp.csv', index=False)

	for collection in metadata.collection.unique():
		print(collection)
		mdf = metadata[metadata.collection == collection]
		results = []
		for token_id in sorted(mdf.token_id.unique()):
			if token_id % 1000 == 1:
				print(token_id, len(results))
			cur = mdf[mdf.token_id == token_id]
			token_metadata = {}
			# m = mints[(mints.collection == collection) & (mints.token_id == token_id) ]
			m = metadata[(metadata.collection == collection) & (metadata.token_id == token_id) ]
			m = m.fillna('None')
			if not len(m):
				print(token_id)
				continue
			mint_address = m.mint_address.values[0] if 'mint_address' in m.columns else ''
			for row in cur.iterrows():
				row = row[1]
				token_metadata[row['feature_name']] = row['feature_value']

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
				, 'image_url': 'None'
				, 'project_name': row['collection']
				, 'token_id': int(token_id)
				, 'token_metadata': token_metadata
				, 'token_metadata_uri': row['image_url']
				, 'token_name': row['collection']
			}
			results.append(d)
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

