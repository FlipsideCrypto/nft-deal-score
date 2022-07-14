import re
import os
import json
import time
# import math
import requests
import pandas as pd
# import urllib.request
import snowflake.connector
from time import sleep

# from solana_model import just_float
# from utils import clean_name, clean_token_id, format_num, merge

############################
#     Define Constants     #
############################
BASE_PATH = '/home/data-science'
DATA_FOLDER = '/rstudio-data/nft_labels'
RPC = 'https://red-cool-wildflower.solana-mainnet.quiknode.pro/a1674d4ab875dd3f89b34863a86c0f1931f57090/'



##############################
#     Load DB Connection     #
##############################
with open('{}/data_science/util/snowflake.pwd'.format(BASE_PATH), 'r') as f:
    pwd = f.readlines()[0].strip()
with open('{}/data_science/util/snowflake.usr'.format(BASE_PATH), 'r') as f:
    usr = f.readlines()[0].strip()

ctx = snowflake.connector.connect(
	user=usr,
	password=pwd,
	account='vna27887.us-east-1'
)


############################
#     Helper Functions     #
############################
def read_csv(data_folder, fname):
	return(pd.read_csv('{}/{}.csv'.format(data_folder, fname)))

def write_csv(data_folder, fname, df, verbose = True):
	df.to_csv('{}/{}.csv'.format(data_folder, fname), index=False)
	if verbose:
		print('Wrote {} rows to {}'.format(len(df), fname))

def clean_colnames(df):
	names = [ x.lower() for x in df.columns ]
	df.columns = names
	return(df)

def clean_collection_name(x):
	x = re.sub('\|', '-', x).strip()
	x = re.sub('\)', '', x).strip()
	x = re.sub('\(', '', x).strip()
	x = re.sub('\'', '', x).strip()
	return(x)

def merge(left, right, on=None, how='inner', ensure=True, verbose=True):
	df = left.merge(right, on=on, how=how)
	if len(df) != len(left) and (ensure or verbose):
		print('{} -> {}'.format(len(left), len(df)))
		cur = left.merge(right, on=on, how='left')
		cols = set(right.columns).difference(set(left.columns))
		print(cols)
		col = list(cols)[0]
		missing = cur[cur[col].isnull()]
		print(missing.head())
		if ensure:
			assert(False)
	return(df)

def Convert(tup, di):
	di = dict(tup)
	return di


####################################
#     Metadata From HowRare.Is     #
####################################
def how_rare_is_api():
	query = '''
		SELECT DISTINCT LOWER(project_name) AS lower_collection
		FROM solana.core.dim_nft_metadata
	'''
	df = ctx.cursor().execute(query)
	df = pd.DataFrame.from_records(iter(df), columns=[x[0] for x in df.description])

	url = 'https://api.howrare.is/v0.1/collections'
	r = requests.get(url)
	j = r.json()
	c_df = pd.DataFrame(j['result']['data']).sort_values('floor_marketcap', ascending=0)
	c_df['lower_collection'] = c_df.url.apply(lambda x: x.lower().strip() )
	seen = sorted(df.LOWER_COLLECTION.apply(lambda x: re.sub(' |_|\'', '', x) ).values)
	c_df['seen_1'] = c_df.url.apply(lambda x: re.sub(' |_|\'', '', x[1:]).lower() in seen ).astype(int)
	c_df['seen_2'] = c_df.name.apply(lambda x: re.sub(' |_|\'', '', x).lower() in seen ).astype(int)
	c_df['seen'] = (c_df.seen_1 + c_df.seen_2 > 0).astype(int)
	seen = seen + [ 'smb','aurory','degenapes','thugbirdz','degods','okay_bears','catalinawhalemixer','cetsoncreck','stonedapecrew','solgods' ]
	c_df = c_df[-(c_df.url.isin([ '/'+x for x in seen]))]
	c_df = c_df[c_df.seen == 0]
	it = 0
	tot = len(c_df)
	m_data = []
	print('Pulling metadata for {} collections'.format(tot))
	for row in c_df.iterrows():
		it += 1
		row = row[1]
		collection = row['name']
		print('#{} / {}: {}'.format(it, tot, collection))
		url = row['url'][1:]
		if it > 1:
			assert(len(m_data))
		url = 'https://api.howrare.is/v0.1/collections/'+url
		r = requests.get(url)
		j = r.json()
		n_errors = 0
		for i in j['result']['data']['items']:
			try:
				token_id = int(i['id'])
				mint = i['mint']
				image = i['image']
				for d in i['attributes']:
					d['token_id'] = token_id
					d['collection'] = collection
					d['mint_address'] = mint
					d['image_url'] = image
					m_data += [ d ]
			except:
				# print('Error')
				n_errors += 1
				pass
		if n_errors:
			print('{} errors'.format(n_errors))
	metadata = pd.DataFrame(m_data).rename(columns={'name':'feature_name', 'value':'feature_value'})

	write_csv(DATA_FOLDER, 'howrare_labels', metadata[['collection','mint_address']])

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
	a['n'] = range(len(a))
	a['n'] = a.n.apply(lambda x: int(x/50) )
	a['token_id'] = a.token_id.astype(int)

	# remove existing files
	fnames = os.listdir(DATA_FOLDER+'/metadata/results/')
	print('fnames')
	print(fnames)
	for f in fnames:
		os.remove(DATA_FOLDER+'/metadata/results/'+f)

	# write new metadata incrementally to upload to solana.core.dim_nft_metadata
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
		write_csv( DATA_FOLDER, 'metadata/results/{}'.format(ind), w )
	return


#################################
#     Load Data From ME API     #
#################################
def mints_from_me():
	##################################
	#     Get All ME Collections     #
	##################################
	headers = {
		# 'Authorization': 'Bearer 9c39e05c-db3c-4f3f-ac48-84099111b813'
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
	write_csv(DATA_FOLDER, 'me_collections', df)
	# df.to_csv('{}/me_collections.csv'.format(DATA_FOLDER), index=False)
	df = read_csv(DATA_FOLDER, 'me_collections')
	# df = pd.read_csv('./data/me_collections.csv')

	###########################################
	#     Get 1 Mint From Each Collection     #
	###########################################
	it = 0
	l_data = []
	# old_l_df = pd.read_csv('./data/me_mints.csv')
	old_l_df = read_csv(DATA_FOLDER, 'me_mints')
	seen = list(old_l_df.symbol.unique())
	print('We\'ve already seen {} / {} mints from ME'.format(len(seen), len(df)))
	df = df[ -df.symbol.isin(seen) ]
	df = df[ (df.symbol.notnull()) & (df.symbol != '') ]
	df = df.sort_values('symbol')
	tot = len(df)
	start = time.time()
	for row in df.iterrows():
		sleep(0.5)
		it += 1
		row = row[1]
		# print('Listings on {}...'.format(row['symbol']))
		url = 'https://api-mainnet.magiceden.dev/v2/collections/{}/activities?offset=0&limit=1'.format(row['symbol'])
		if row['symbol'] in seen:
			print('Seen')
			continue
		try:
			r = requests.get(url, headers=headers)
			j = r.json()
		except:
			try:
				print('Re-trying in 10s')
				sleep(10)
				r = requests.get(url, headers=headers)
				j = r.json()
			except:
				try:
					print('Re-trying in 60s')
					sleep(60)
					r = requests.get(url, headers=headers)
					j = r.json()
				except:
					print('Re-trying in 60s (again!)')
					sleep(60)
					r = requests.get(url, headers=headers)
					j = r.json()
		if len(j):
			l_data += [[ row['symbol'], row['name'], j[0]['tokenMint'] ]]
		if it == 1 or it % 10 == 0:
			print('#{} / {} ({} records in {} secs)'.format(it, tot, len(l_data), round(time.time() - start)))
			# l_df = pd.DataFrame(l_data, columns=['symbol','name','mint_address'])
			# l_df.to_csv('./data/me_mints.csv', index=False)
	l_df = pd.DataFrame(l_data, columns=['symbol','name','mint_address'])
	l_df = pd.concat([l_df, old_l_df]).drop_duplicates(subset=['symbol'])
	print('Adding {} rows to me_mints'.format(len(l_df) - len(old_l_df)))
	# l_df.to_csv('./data/me_mints.csv', index=False)
	write_csv(DATA_FOLDER, 'me_mints', l_df)


	######################################################
	#     Get Update Authorities For All Collections     #
	######################################################
	# l_df = pd.read_csv('./data/me_mints.csv')
	# m_old = pd.read_csv('./data/me_update_authorities.csv')
	m_old = read_csv(DATA_FOLDER, 'me_update_authorities')
	m_old['seen'] = 1
	m_data = list(m_old[['symbol','name','update_authority','seen']].values)
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
			m_data += [[ row['symbol'], row['name'], j['updateAuthority'], 0 ]]
		if it % 10 == 0:
			print('it#{}: {}'.format(it, len(m_data)))
			# m_df = pd.DataFrame(m_data, columns=['symbol','name','update_authority'])
			# m_df.to_csv('./data/me_update_authorities.csv', index=False)
	m_df = pd.DataFrame(m_data, columns=['symbol','name','update_authority','seen'])
	m_df = m_df.drop_duplicates()
	print('Adding {} rows to me_update_authorities'.format(len(m_df) - len(m_old)))
	write_csv(DATA_FOLDER, 'me_update_authorities', m_df)
	# m_df.to_csv('./data/me_update_authorities.csv', index=False)

def pull_from_metaboss():

	######################################################
	#     Get Update Authorities For All Collections     #
	######################################################
	# m_df = pd.read_csv('./data/me_update_authorities.csv')
	m_df = read_csv(DATA_FOLDER, 'me_update_authorities')
	n_auth = m_df.groupby('update_authority').name.count().reset_index().rename(columns={'name':'n_auth'})
	m_df = m_df.merge(n_auth)
	l1 = len(m_df[ (m_df.seen == 0) & (m_df.n_auth == 1)])
	l2 = len(m_df[ (m_df.seen == 0) & (m_df.n_auth > 1)])
	print('{} with 1 update_authority; {} with 2+ update_authority'.format(l1, l2))

	need = list(m_df[ (m_df.seen == 0) & (m_df.n_auth == 1) ].update_authority.unique())
	need = m_df[m_df.update_authority.isin(need)]
	# l_df = pd.read_csv('./data/me_mints.csv')
	l_df = read_csv(DATA_FOLDER, 'me_mints')
	fix = need.merge(l_df[[ 'name','mint_address' ]])
	need = fix.copy().rename(columns={'name':'collection'})
	# need = need.drop_duplicates(subset=['update_authority']).sort_values('collection').head(7).tail(1)
	need['collection'] = need.collection.apply(lambda x: clean_collection_name(x) )
	need = need.drop_duplicates(subset=['update_authority']).sort_values('collection')
	# need = need.head(2)

	mfiles = ['/data/mints/{}/{}_mint_accounts.json'.format(re.sub(' |-', '_', collection), update_authority) for collection, update_authority in zip(need.collection.values, need.update_authority.values) ]
	seen = [ x for x in mfiles if os.path.exists(x) ]
	seen = []

	# for update authorities that have only 1 collection, we can just check metaboss once
	mfolder = '{}/mints/'.format(DATA_FOLDER)
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

		dir = '{}{}/'.format(mfolder, collection_dir)
		mfile = '{}{}_mint_accounts.json'.format(dir, update_authority)
		if not os.path.exists(dir):
			print(collection)
			os.makedirs(dir)
		# elif len(os.listdir(dir)) and os.path.exists(mfile):
		# 	print('Already have {}.'.format(collection))
		# 	print('Seen')
		# 	continue
		seen.append(update_authority)
		os.system('metaboss -r {} -T 300 snapshot mints --update-authority {} --output {}'.format(RPC, update_authority, dir))

	# write the mints to csv
	data = []
	for path in os.listdir(mfolder):
		if os.path.isdir('{}{}'.format(mfolder, path)):
			collection = re.sub('_', ' ', path).strip()
			for fname in os.listdir(mfolder+path):
				f = mfolder+path+'/'+fname
				if os.path.isfile(f) and '.json' in f:
					with open(f) as file:
						j = json.load(file)
						for m in j:
							data += [[ collection, m ]]
	df = pd.DataFrame(data, columns=['collection','mint_address'])
	df.collection.unique()
	write_csv(DATA_FOLDER, 'single_update_auth_labels', df)
	# df.to_csv('./data/single_update_auth_labels.csv', index=False)

	################################
	#     Multiple Authorities     #
	################################
	need = list(m_df[ (m_df.seen == 0) & (m_df.n_auth > 1) ].update_authority.unique())
	need = m_df[m_df.update_authority.isin(need)]
	fix = need.merge(l_df[[ 'name','mint_address' ]])
	need = fix.copy().rename(columns={'name':'collection'})
	need['collection'] = need.collection.apply(lambda x: clean_collection_name(x) )
	need = need.sort_values('collection').drop_duplicates(subset=['update_authority'], keep='first')
	# need = need.head(2)
	it = 0
	a = []
	for row in need.iterrows():
		it += 1
		print('#{}/{}'.format(it, len(need)))
		row = row[1]
		collection = row['collection']
		update_authority = row['update_authority']
		print('Working on {}...'.format(collection))
		collection_dir = re.sub(' |-', '_', collection)

		dir = '{}{}/'.format(mfolder, collection_dir)
		mfile = '{}{}_mint_accounts.json'.format(dir, update_authority)
		if not os.path.exists(dir):
			print(collection)
			os.makedirs(dir)
		a.append(update_authority)
		os.system('metaboss -r {} -T 300 snapshot mints --update-authority {} --output {}'.format(RPC, update_authority, dir))

		odir = dir+'output/'
		if not os.path.exists(odir):
			print('Making dir {}'.format(odir))
			os.makedirs(odir)
		os.system('metaboss -r {} -T 300 decode mint --list-file {} --output {}'.format(RPC, mfile, odir ))

	##################################################
	#     Load All The Mints for Each Collection     #
	##################################################
	# now that we have the mints, create a data frame with the info for each mint in each collection
	mfolder = '{}/mints/'.format(DATA_FOLDER)
	data = []
	seen = [ x[1] for x in data ]
	it = 0
	dirs = sorted(os.listdir(mfolder))
	dirs = [ x for x in dirs if not x in ['3D_Sniping_Demons']]
	tot = len(dirs)
	for path in dirs:
		print('{} / {} ({} records)'.format(it, tot, len(data)))
		it += 1
		if os.path.isdir(mfolder+path):
			collection = re.sub('_', ' ', path).strip()
			print('Found {}'.format(collection))
			if not os.path.exists(mfolder+path+'/output/'):
				print('No output')
				continue
			fnames = os.listdir(mfolder+path+'/output/')
			print('{} files found'.format(len(fnames)))
			for fname in fnames:
				f = mfolder+path+'/output/'+fname
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
	symbol_only = [x for x in symbol_only if not x in collection_only]

	# now get the info for each collection-name-symbol combo
	g1 = new_mints[ (-(new_mints.collection.isin(symbol_only))) & (-(new_mints.collection.isin(collection_only))) ].groupby(['collection','clean_name','symbol']).head(1).reset_index()
	g2 = new_mints[ ((new_mints.collection.isin(symbol_only))) & (-(new_mints.collection.isin(collection_only))) ].groupby(['collection','symbol']).head(1).reset_index()
	g3 = new_mints[ (-(new_mints.collection.isin(symbol_only))) & ((new_mints.collection.isin(collection_only))) ].groupby(['collection']).head(1).reset_index()
	g = pd.concat([g1, g2, g3]).drop_duplicates(subset=['mint_address'])
	print('{} Total: {} all, {} collection-symbol {} collection'.format(len(g), len(g1), len(g2), len(g3)))
	# g.to_csv('~/Downloads/tmp-g.csv', index=False)

	# iterate over each row to get what collection they are actually in
	# by pulling data from the uri
	uri_data = []
	it = 0
	tot = len(g)
	print(tot)
	errs = []
	seen = [ x['uri'] for x in uri_data ]
	# for row in g[ -(g.uri.isin(seen)) ].iterrows():
	for row in g.iterrows():
		row = row[1]
		it += 1
		# if it % 100 == 0:
		# 	uri_df = pd.DataFrame(uri_data)[[ 'collection','name','symbol','row_symbol','row_collection','uri','row_clean_name','mint_address' ]]
		# 	uri_df.to_csv('~/Downloads/uri_df.csv', index=False)
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
	write_csv(DATA_FOLDER, 'uri_df', uri_df)
	# uri_df.to_csv('~/Downloads/uri_df.csv', index=False)

	# for each row, parse the json from the uri
	# uri_df = pd.read_csv('~/Downloads/uri_df.csv')
	# read_csv(DATA_FOLDER, 'uri_df')
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
	# uri_df[uri_df.use_collection == 'nan'][['use_collection','parsed_collection','parsed_family','clean_name','name','collection','symbol','row_symbol','row_collection']].head()
	# uri_df[uri_df.use_collection == 'nan'][['use_collection','parsed_collection','parsed_family','clean_name','name','collection','symbol','row_symbol','row_collection']].to_csv('~/Downloads/tmp.csv', index=False)
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
	uri_df['use_collection'] = uri_df.apply(lambda x: f1(x), 1 )

	# clean the mint_address
	uri_df['mint_address'] = uri_df.mint_address.apply(lambda x: re.sub('.json','', x))
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
	m = pd.concat( [m_1, m_2, m_3] )
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

	# m.to_csv('./data/mult_update_auth_labels.csv', index=False)
	write_csv(DATA_FOLDER, 'mult_update_auth_labels', m)

def compile():
	single_update_auth_labels = read_csv(DATA_FOLDER, 'single_update_auth_labels')
	mult_update_auth_labels = read_csv(DATA_FOLDER, 'mult_update_auth_labels')
	howrare_labels = read_csv(DATA_FOLDER, 'howrare_labels')
	df = pd.concat([howrare_labels, single_update_auth_labels, mult_update_auth_labels])
	df = df[ (df.collection != 'Nan') & (df.collection != 'nan') & (df.collection.notnull()) ]
	df = df.drop_duplicates(subset=['mint_address'], keep='first')
	print(len(df[df.collection == 'DegenTown']))
	write_csv(DATA_FOLDER, 'solana_nft_labels', df[['mint_address','collection']])

# print('Loaded!')
# mints_from_me()
# pull_from_metaboss()
# compile()
# how_rare_is_api()