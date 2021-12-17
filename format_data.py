import re
import os
import math
import json
import pandas as pd
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
		with open('./data/metadata/levana_dragon_eggs/{}.txt'.format(i), 'w') as outfile:
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

