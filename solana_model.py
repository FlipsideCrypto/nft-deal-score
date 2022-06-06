import os
import re
import json
import time
import pickle
from textwrap import indent
import warnings
import requests
import numpy as np
import pandas as pd
import kutils as ku
import urllib.request
import tensorflow as tf
import snowflake.connector

from curses import meta
from copy import deepcopy
from datetime import datetime
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, RidgeCV, Lasso, Ridge
from sklearn.model_selection import train_test_split, KFold, GridSearchCV, RandomizedSearchCV

os.chdir('/Users/kellenblumberg/git/nft-deal-score')
from utils import merge, clean_name

warnings.filterwarnings('ignore')

# 1-att (naked 1-att?)
# matching aesthetics
# type
# laser eyes
# vipers
# pirate hat
# sombrero
# cowboy hat
# admiral hat

###################################
#     Define Helper Functions     #
###################################
def standardize_df(df, cols, usedf=None, verbose=False):
	for c in cols:
		if type(usedf) != type(pd.DataFrame()):
			usedf = df
		mu = usedf[c].mean()
		sd = usedf[c].std()
		if verbose:
			print(c)
		if len(df[c].unique()) == 2 and df[c].max() == 1 and df[c].min() == 0:
			# df['std_{}'.format(c)] = df[c].apply(lambda x: (x*2) - 1 )
			df['std_{}'.format(c)] = df[c]
		else:
			df['std_{}'.format(c)] = (df[c] - mu) / sd
	return(df)

def just_float(x):
	x = re.sub('[^\d\.]', '', str(x))
	return(float(x))

def calculate_percentages(df, cols=[]):
	add_pct = not 'pct' in df.columns
	if not len(cols):
		cols = df.columns
	if add_pct:
		df['pct'] = 1
	for c in cols:
		g = df[c].value_counts().reset_index()
		g.columns = [ c, 'N' ]
		col = '{}_pct'.format(c)
		g[col] = g.N / g.N.sum()
		df = df.merge( g[[ c, col ]] )
		if add_pct:
			df['pct'] = df.pct * df[col]
	return(df)

def get_sales(check_exclude = True, exclude=[]):

	s_df = pd.read_csv('./viz/nft_deal_score_sales.csv').rename(columns={'sale_date':'block_timestamp'})
	sorted(s_df.collection.unique())
	s_df['token_id'] = s_df.token_id.astype(str)
	s_df['collection'] = s_df.collection.apply(lambda x: clean_name(x))
	# s_df['collection'] = s_df.collection_x.fillna(s_df.collection_y).fillna(s_df.collection).apply(lambda x: clean_name(x))
	s_df = s_df.drop_duplicates(subset=['token_id','collection','price'])
	s_df = s_df[-s_df.collection.isin(['Levana Meteors','Levana Dust'])]
	s_df = s_df[ -s_df.collection.isin(['boryokudragonz', 'Boryoku Dragonz']) ]
	s_df = s_df[[ 'collection','block_timestamp','token_id','price','tx_id' ]]
	for e in exclude:
		s_df = s_df[-( (s_df.collection == e[0]) & (s_df.token_id == str(e[1])) & (s_df.price == e[2]) )]
	s_df = s_df[ -((s_df.collection == 'smb') & (s_df.price < 1)) ]

	# exclude wierd data points
	if not check_exclude:
		train_exclude = pd.read_csv('./data/train_exclude.csv')
		include = pd.read_csv('./data/include.csv')
		include['include'] = 1
		train_exclude['train_exclude'] = 1
		exclude = pd.read_csv('./data/exclude.csv')
		exclude['collection'] = exclude.collection.apply(lambda x: clean_name(x))
		exclude['token_id'] = exclude.token_id.astype(str)
		include['token_id'] = include.token_id.astype(str)
		train_exclude['token_id'] = train_exclude.token_id.astype(str)
		s_df = s_df.merge(exclude, how='left')
		s_df = s_df.merge(include, how='left')
		s_df = s_df.merge(train_exclude, how='left')
		s_df = s_df[ - ((s_df.include.isnull()) & (s_df.exclude.notnull())) ]
		del s_df['exclude']
		del s_df['include']

	###########################
	#     Calculate Floor     #
	###########################
	s_df['block_timestamp'] = s_df.block_timestamp.apply(lambda x: datetime.strptime(str(x)[:19], '%Y-%m-%d %H:%M:%S') if len(x) > 10 else datetime.strptime(x[:10], '%Y-%m-%d') )
	s_df['timestamp'] = s_df.block_timestamp.astype(int)
	s_df['days_ago'] = s_df.block_timestamp.apply(lambda x: (datetime.today() - x).days ).astype(int)

	# lowest price in last 20 sales
	s_df = s_df.sort_values(['collection','block_timestamp'])
	s_df['mn_20'] = s_df.groupby('collection').price.shift(1)
	s_df = s_df.sort_values(['collection','block_timestamp'])
	s_df['md_20'] = s_df.groupby('collection')['mn_20'].rolling(20).quantile(.01).reset_index(0,drop=True)

	# exclude sales that are far below the existing floor
	s_df = s_df[ (s_df.price) >= (s_df.md_20 * 0.70) ]

	# 10%ile of last 20 sales
	s_df = s_df.sort_values(['collection','block_timestamp'])
	s_df['mn_20'] = s_df.groupby('collection').price.shift(1)
	s_df = s_df.sort_values(['collection','block_timestamp'])
	# s_df['mn_20'] = s_df.groupby('collection')['mn_20'].rolling(20).quantile(.0525).reset_index(0,drop=True)
	s_df['mn_20'] = s_df.groupby('collection')['mn_20'].rolling(20).quantile(.0525).reset_index(0,drop=True)
	s_df['sim'] = 0
	s_df['tmp'] = s_df.block_timestamp.apply(lambda x: str(x)[:10] )
	s_df.groupby(['collection','tmp']).mn_20.mean().reset_index().to_csv('~/Downloads/mn_20.csv', index=False)
	return(s_df)

def get_coefs(cols, coef):
	coefs = []
	for a, b in zip(cols, coef):
		coefs += [[a,b]]
	coefs = pd.DataFrame(coefs, columns=['col','coef']).sort_values('coef', ascending=0)
	# coefs.to_csv('~/Downloads/{}_lin_coefs.csv'.format(collection), index=False)
	# coefs['tmp'] = coefs.col.apply(lambda x: 'nft_rank' in x )
	# coefs['mult'] = coefs.col.apply(lambda x: -1 if x == 'std_nft_rank' else 1 )
	def f(x):
		if x['col'] in ['std_nft_rank','collection_rank']:
			return(-1)
		pos = ['adj_nft_rank','is_top_','y_pred_','matching','naked_1_att']
		for p in pos:
			if p in x['col']:
				return(1)
		if x['coef'] >= 0:
			return(1)
		return(-1)
	coefs['mult'] = coefs.apply(lambda x: f(x), 1 )
	coefs['val'] = coefs.mult * coefs.coef
	coefs = coefs.sort_values('val', ascending=0)
	return(coefs)

def train_model(check_exclude=False, supplement_with_listings=True, use_saved_params=True):
	exclude = [
		( 'aurory', 2239, 3500 )
		, ( 'aurory', 1876, 789 )
		, ( 'aurory', 2712, 500 )
		, ( 'aurory', 5368, 500 )
		, ( 'aurory', 9239, 1700 )
		, ( 'BAYC', 3231, 267500 )
		, ( 'BAYC', 3485, 250000 )
		, ( 'BAYC', 4037, 150000 )
		, ( 'BAYC', 318, 5850 )
		, ( 'BAYC', 1159, 4000 )
		, ( 'BAYC', 6538, 2400 )
		, ( 'BAYC', 232, 1032.05895 )
		, ( 'BAYC', 6326, 800 )
		, ( 'BAYC', 6924, 666 )
		, ( 'BAYC', 9198, 500 )
		, ( 'BAYC', 3001, 500 )
		, ( 'BAYC', 3562, 430 )
	]
	s_df = get_sales(check_exclude, exclude)
	s_df.groupby('collection').block_timestamp.max()
	s_df[s_df.collection == 'BAYC'].sort_values('block_timestamp', ascending=0).head()[['token_id','block_timestamp','price']]
	s_df[s_df.collection == 'MAYC'].sort_values('price', ascending=0).head()
	# s_df[s_df.collection == 'Solana Monkey Business'].to_csv('./tableau/data/smb_sales.csv', index=False)
	# s_df = s_df[-s_df.collection.isin(['BAYC','MAYC'])]
	# s_df[s_df.collection.isnull()]
	# s_df = pd.read_csv('./data/sales.csv').rename(columns={'sale_date':'block_timestamp'})
	# s_df['collection'] = s_df.collection.apply(lambda x: clean_name(x))
	# s_df = s_df[-s_df.collection.isin(['Levana Meteors','Levana Dust'])]
	# s_df = s_df[ -s_df.collection.isin(['boryokudragonz', 'Boryoku Dragonz']) ]
	# s_df = s_df[[ 'chain','collection','block_timestamp','token_id','price','tx_id' ]]
	# for e in exclude:
	#     s_df = s_df[-( (s_df.collection == e[0]) & (s_df.token_id == e[1]) & (s_df.price == e[2]) )]
	# s_df = s_df[ -((s_df.collection == 'smb') & (s_df.price < 1)) ]

	# # exclude wierd data points
	# if not check_exclude:
	#     exclude = pd.read_csv('./data/exclude.csv')
	#     exclude['collection'] = exclude.collection.apply(lambda x: clean_name(x))
	#     s_df = s_df.merge(exclude, how='left')
	#     s_df = s_df[s_df.exclude.isnull()]
	#     del s_df['exclude']

	#########################
	#     Load Metadata     #
	#########################
	m_df = pd.read_csv('./data/metadata.csv')
	m_df['feature_name'] = m_df.feature_name.apply(lambda x: 'Clothes' if x == 'Clother' else x )
	# m_df[m_df.collection == 'DeGods'][['feature_name']].drop_duplicates()
	# sorted(m_df.collection.unique())
	# m_df[m_df.collection == 'Aurory'][['collection','feature_name']].drop_duplicates().to_csv('~/Downloads/tmp.csv', index=False)
	# sorted([x for x in m_df.feature_name.unique() if 'nft_' in x])
	m_df['token_id'] = m_df.token_id.astype(str)
	m_df['collection'] = m_df.collection.apply(lambda x: clean_name(x))
	sorted(m_df.collection.unique())
	# remove ones that are not actually metadata
	m_df = m_df[ -m_df.feature_name.isin([ 'price','last_sale','feature_name','feature_value' ]) ]
	m_df['feature_value'] = m_df.feature_value.apply(lambda x: re.split("\(", re.sub("\"", "", x))[0] if type(x)==str else x )
	m_df[(m_df.feature_name=='rank') & (m_df.collection == 'Levana Dragon Eggs')]
	sorted(m_df[ (m_df.collection == 'Solana Monkey Business') ].feature_name.unique())
	sorted(m_df.collection.unique())


	#####################################
	#     Exclude Special LunaBulls     #
	#####################################
	tokens = pd.read_csv('./data/tokens.csv')
	tokens['collection'] = tokens.collection.apply(lambda x: clean_name(x))
	tokens['token_id'] = tokens.token_id.astype(str)
	m_df = merge(m_df, tokens[['collection','token_id','clean_token_id']].dropna().drop_duplicates() , how='left', ensure=True, on=['collection','token_id'], message='m_df x tokens')
	m_df['token_id'] = m_df.clean_token_id.fillna(m_df.token_id).astype(float).astype(int).astype(str)
	s_df = merge(s_df, tokens[['collection','token_id','clean_token_id']].drop_duplicates(), how='left', ensure=True, on=['collection','token_id'], message='s_df x tokens')
	s_df[s_df.token_id.isnull()]
	sorted(s_df.collection.unique())
	# np.isinf(s_df).values.sum()
	# s_df['clean_token_id'] = s_df.clean_token_id.fillna(s_df.token_id)
	# s_df['token_id'] = (s_df.clean_token_id).apply(lambda x: re.sub('"', '', str(x))).astype(float).astype(int).astype(str)
	s_df['token_id'] = (s_df.clean_token_id.replace('nan', None).fillna(s_df.token_id.replace('nan', None))).apply(lambda x: re.sub('"', '', str(x)))
	s_df = s_df[s_df.token_id != 'None']
	s_df['token_id'] = s_df.token_id.astype(float).astype(int).astype(str)
	# s_df[s_df.token_id == 'None'].groupby('collection').token_id.count()


	lunabullsrem = tokens[tokens.clean_token_id>=10000].token_id.unique()
	m_df = m_df[ -((m_df.collection == 'LunaBulls') & (m_df.token_id.isin(lunabullsrem))) ]
	s_df = s_df[ -((s_df.collection == 'LunaBulls') & (s_df.token_id.isin(lunabullsrem))) ]
	s_df = s_df.drop_duplicates(subset=['collection','token_id','price'])


	###########################
	#     Calculate Floor     #
	###########################
	# s_df['block_timestamp'] = s_df.block_timestamp.apply(lambda x: datetime.strptime(str(x)[:19], '%Y-%m-%d %H:%M:%S') if len(x) > 10 else datetime.strptime(x[:10], '%Y-%m-%d') )
	# s_df['timestamp'] = s_df.block_timestamp.astype(int)
	# s_df['days_ago'] = s_df.block_timestamp.apply(lambda x: (datetime.today() - x).days ).astype(int)

	# # lowest price in last 20 sales
	# s_df = s_df.sort_values(['collection','block_timestamp'])
	# s_df['mn_20'] = s_df.groupby('collection').price.shift(1)
	# s_df = s_df.sort_values(['collection','block_timestamp'])
	# s_df['md_20'] = s_df.groupby('collection')['mn_20'].rolling(20).quantile(.01).reset_index(0,drop=True)

	# # exclude sales that are far below the existing floor
	# s_df = s_df[ (s_df.price) >= (s_df.md_20 * 0.70) ]

	# # 10%ile of last 20 sales
	# s_df = s_df.sort_values(['collection','block_timestamp'])
	# s_df['mn_20'] = s_df.groupby('collection').price.shift(1)
	# s_df = s_df.sort_values(['collection','block_timestamp'])
	# s_df['mn_20'] = s_df.groupby('collection')['mn_20'].rolling(20).quantile(.1).reset_index(0,drop=True)
	# s_df['sim'] = 0
	# s_df['tmp'] = s_df.block_timestamp.apply(lambda x: str(x)[:10] )
	# s_df.groupby(['collection','tmp']).mn_20.mean().reset_index().to_csv('~/Downloads/mn_20.csv', index=False)

	listings = pd.read_csv('./data/listings.csv')
	listings = pd.read_csv('./viz/nft_deal_score_listings.csv')
	sorted(listings.collection.unique())
	if supplement_with_listings:
		pred_price = pd.read_csv('./data/pred_price.csv')
		pred_price['collection'] = pred_price.collection.apply(lambda x: clean_name(x))
		pred_price['token_id'] = pred_price.token_id.astype(str)
		listings['collection'] = listings.collection.apply(lambda x: clean_name(x))
		listings['block_timestamp'] = s_df.block_timestamp.max()
		listings['token_id'] = listings.token_id.astype(str)
		# listings = listings[listings.collection.isin(pred_price.collection.unique())]
		floor = s_df.sort_values('timestamp').groupby('collection').tail(1)[['collection','mn_20']]
		tmp = merge(listings, pred_price, ensure=False)
		tmp = tmp[ (tmp.price < tmp.pred_price) | ((tmp.collection == 'Galactic Angels') & (tmp.pred_price >= 20) & (tmp.price < (tmp.pred_price * 2))) ]
		tmp_1 = tmp[ (tmp.price * 1.25) < tmp.pred_price ]
		tmp_2 = tmp[ (tmp.price * 1.50) < tmp.pred_price ]
		tmp = tmp.append(tmp_1).append(tmp_2)
		# tmp[tmp.block_timestamp.isnull()]
		# tmp.block_timestamp = s_df.timestamp.max()
		tmp['timestamp'] = tmp.block_timestamp.astype(int)
		tmp['days_ago'] = tmp.block_timestamp.apply(lambda x: (datetime.today() - x).days ).astype(int)
		tmp = merge(tmp, floor, ensure=False)

		n = round(len(s_df) / 5000)
		n = max(1, min(3, n))
		print('Supplement with {}x listings'.format(n))
		# n = 1
		for _ in range(n):
			s_df = s_df.append(tmp[[ 'block_timestamp','timestamp','collection','token_id','price','mn_20' ]])
			# tmp_1 = tmp[tmp.price <= 0.8 * tmp.pred_price]
			# s_df = s_df.append(tmp_1[[ 'block_timestamp','timestamp','collection','token_id','price','mn_20' ]])
			# tmp_2 = tmp[tmp.price <= 0.6 * tmp.pred_price]
			# tmp_2 = s_df.append(tmp_2[[ 'block_timestamp','timestamp','collection','token_id','price','mn_20' ]])


	########################
	#     Other Things     #
	########################
	# coefsdf = pd.DataFrame()
	# salesdf = pd.DataFrame()
	# attributes = pd.DataFrame()
	# pred_price = pd.DataFrame()
	# feature_values = pd.DataFrame()
	coefsdf = pd.read_csv('./data/coefsdf.csv')
	salesdf = pd.read_csv('./data/model_sales.csv')
	attributes = pd.read_csv('./data/attributes.csv')
	pred_price = pd.read_csv('./data/pred_price.csv')
	pred_price[ (pred_price.collection == 'Solana Monkey Business') & (pred_price.token_id == 1141)]
	feature_values = pd.read_csv('./data/feature_values.csv')
	# non-binary in model: collection_rank, temperature, weight
	# non-binary in model; exclude from rarity: pct, rank, score
	# exclude from model: lucky_number, shower
	# exclude from model and rarity %: meteor_id, attribute_count, cracking_date
	ALL_NUMERIC_COLS = ['rank','score','pct','Pct']
	ALL_NUMERIC_COLS = ['nft_rank','adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2']
	MODEL_EXCLUDE_COLS = {
		# 'Levana Dragon Eggs': ['collection_rank','meteor_id','shower','lucky_number','cracking_date','attribute_count','weight','temperature']
		'Levana Dragon Eggs': ['meteor_id','shower','lucky_number','cracking_date','attribute_count','rarity_score_rank','rarity_score','weight','collection_rank_group']
		, 'Solana Monkey Business': ['Clothes_Diamond']
	}
	MODEL_INCLUDE_COLS = {
		# 'Solana Monkey Business': ['std_Hat_Strawhat','std_Hat_Space Warrior Hair','std_Clothes_Diamond','std_Eyes_Solana Vipers','std_Eyes_Vipers','std_Hat_Sombrero','std_Eyes_3D Glasses','std_Hat_Cowboy Hat','std_Eyes_Laser Eyes','std_matching_cop','std_matching_white','std_matching_black']
		'Solana Monkey Business': ['std_Hat_Space Warrior Hair','std_matching_cop','std_Hat_Cowboy Hat','std_Hat_Sombrero','std_Hat_Solana Backwards Cap','std_Eyes_Solana Vipers','std_Eyes_Laser Eyes','std_Type_Solana']
	}
	RARITY_EXCLUDE_COLS = {
		# 'Levana Dragon Eggs': ['collection_rank','meteor_id','shower','lucky_number','cracking_date','attribute_count','weight','temperature']
		'Levana Dragon Eggs': ['meteor_id','attribute_count','collection_rank','transformed_collection_rank','rarity_score','rarity_score_rank','collection_rank_group']
	}
	NUMERIC_COLS = {
		'Levana Dragon Eggs': ['collection_rank','temperature','transformed_collection_rank']
	}
	ATT_EXCLUDE_COLS = {
		'Levana Dragon Eggs': ['attribute_count','transformed_collection_rank','collection_rank_group']
	}

	collection = 'Aurory'
	collection = 'Solana Monkey Business'
	collection = 'DeGods'
	# for collection in [ 'Solana Monkey Business' ]:
	# for collection in [ 'Aurory' ]:
	# for collection in [ 'Aurory','Solana Monkey Business' ]:
	sorted(pred_price.collection.unique())
	sorted(s_df.collection.unique())
	# print(sorted(m_df.collection.unique()))
	# for collection in s_df.collection.unique():
	saved_params = {}
	file_to_store = open('./objects/saved_params.pickle', 'rb')
	saved_params = pickle.load(file_to_store)
	collection = 'Aurory'
	collection = 'Levana Dragon Eggs'
	collection = 'Galactic Punks'
	collection = 'Stoned Ape Crew'
	collections = ['Levana Dragon Eggs']
	collections = ['Solana Monkey Business']
	collections = ['Galactic Angels']
	# collections = [ x for x in collections if not x in ['BAYC','MAYC','Bakc'] ]
	collection = 'BAYC'
	collection = 'MAYC'
	collections = ['BAYC']
	collections = ['MAYC']
	collections = [ x for x in collections if not x in ['Bakc','BAKC','MAYC'] ]
	collections = [ x for x in collections if not x in ['Astrals','Cets on Cleck','DeFi Pirates'] ]
	s_df.groupby('collection').block_timestamp.max()
	m_df[m_df.collection.isin(['Okay Bears','Catalina Whale Mixer'])]
	s_df[s_df.collection.isin(['Okay Bears','Catalina Whale Mixer'])]
	collections = ['Okay Bears']
	collections = ['Cets on Creck']
	collections = list(s_df[['collection']].drop_duplicates().merge(m_df[['collection']].drop_duplicates()).collection.unique())
	collections = [ c for c in collections if not c in [ 'Galactic Punks','LunaBulls','Galactic Angels','Levana Dragon Eggs','BAKC','BAYC','Astrals','MAYC' ] ]
	collections = [ c for c in collections if not c in [ 'Okay Bears','Stoned Ape Crew','Cets on Creck' ] ]
	collections = ['SOLGods']
	collections = ['Cets on Creck','Pesky Penguins']
	print(sorted(collections))
	# collections = [ 'Catalina Whale Mixer', 'Okay Bears', 'Pesky Penguins' ]
	for collection in collections:
		# if collection in ['Astrals','Bakc','BAKC']+[ 'Catalina Whale Mixer', 'Okay Bears', 'Pesky Penguins' ]:
		if collection in ['Astrals','Bakc','BAKC']:
			continue
		if not collection in saved_params.keys():
			saved_params[collection] = {}
		coefsdf = coefsdf[coefsdf.collection != collection]
		salesdf = salesdf[salesdf.collection != collection]
		attributes = attributes[attributes.collection != collection]
		pred_price = pred_price[pred_price.collection != collection]
		feature_values = feature_values[feature_values.collection != collection]
		print('\nWorking on collection {}'.format(collection))
		sales = s_df[ s_df.collection == collection ]
		sales[sales.sim==0].block_timestamp.max()
		metadata = m_df[ m_df.collection == collection ].drop_duplicates(subset=['token_id','feature_name'], keep='last')
		metadata = metadata[metadata.feature_name != 'Genesis Role?']
		metadata[metadata.token_id=='1']
		metadata[metadata.feature_name=='Genesis Role?'].feature_value.unique()
		sorted(metadata.feature_name.unique())
		# metadata.groupby(['feature_name','feature_value']).token_id.count().reset_index().to_csv('~/Downloads/tmp.csv', index=False)
		# metadata[metadata.token_id == '1']
		metadata['feature_name'] = metadata.feature_name.apply(lambda x: x.strip() )
		metadata[metadata.token_id == '1']
		metadata[metadata.feature_name == 'rank']
		metadata = metadata[-metadata.feature_name.isin(['rank','pct','Pct','ipfs_image'])]
		metadata.feature_name.unique()
		metadata[(metadata.token_id=='1') & (metadata.collection == 'Solana Monkey Business')]

		# categorize columns
		all_names = sorted(metadata.feature_name.unique())
		model_exclude = MODEL_EXCLUDE_COLS[collection] if collection in MODEL_EXCLUDE_COLS.keys() else []
		num_features = sorted((NUMERIC_COLS[collection] if collection in NUMERIC_COLS.keys() else []) + ALL_NUMERIC_COLS)
		num_features = [ x for x in num_features if x in metadata.feature_name.unique() ]
		num_metadata = metadata[metadata.feature_name.isin(num_features)]
		num_metadata[num_metadata.feature_name == 'nft_rank']
		cat_features = sorted([ x for x in all_names if not x in (model_exclude + num_features) ])
		cat_metadata = metadata[metadata.feature_name.isin(cat_features)]

		# create dummies for binary variables
		num_metadata = num_metadata.pivot( ['collection','token_id'], ['feature_name'], ['feature_value'] ).reset_index()
		num_metadata.columns = [ 'collection','token_id' ] + num_features

		# create dummies for binary variables
		cat_metadata = cat_metadata.pivot( ['collection','token_id'], ['feature_name'], ['feature_value'] ).reset_index()
		cat_metadata.columns = [ 'collection','token_id' ] + cat_features
		# cat_metadata = calculate_percentages( cat_metadata, cat_features )
		dummies = pd.get_dummies(cat_metadata[cat_features])
		# dummies.head(1).to_csv('~/Downloads/tmp2.csv', index=False)
		if collection == 'Solana Monkey Business':
			# dummies['matching_white'] = ((dummies['Clothes_Beige Smoking'] == 1) & ((dummies['Hat_White Fedora 1'] + dummies['Hat_White Fedora 2']) == 1)).astype(int)
			# dummies['matching_black'] = ((dummies['Clothes_Black Smoking'] == 1) & ((dummies['Hat_Black Fedora 1'] + dummies['Hat_Black Fedora 2'] + dummies['Hat_Black Top Hat']) == 1)).astype(int)
			# dummies['matching_top'] = ((dummies['matching_black'] == 1) | (dummies['matching_white']== 1)).astype(int)
			# dummies['matching_cop'] = ((dummies['Clothes_Cop Vest'] == 1) & ((dummies['Hat_Cop Hat']==1))).astype(int)
			# dummies['matching_green'] = ((dummies['Clothes_Green Smoking'] == 1) & ((dummies['Hat_Green Top Hat']) == 1)).astype(int)
			dummies['naked_1_att'] = ((dummies['Attribute Count_1'] == 1) & (dummies['Clothes_None'] == 1)).astype(int)
			# dummies['naked_1_att_hat'] = ((dummies['Attribute Count_1'] == 1) & (dummies['Hat_None'] == 0)).astype(int)
			dummies['fedora'] = (dummies['Hat_Black Fedora 1'] + dummies['Hat_Black Fedora 2'] + dummies['Hat_White Fedora 1'] + dummies['Hat_White Fedora 2'] + dummies['Hat_White Fedora 2'] >= 1 ).astype(int)
			dummies['backwards_cap'] = (dummies['Hat_Black Backwards Cap'] + dummies['Hat_Blue Backwards Cap'] + dummies['Hat_Green Backwards Cap'] + dummies['Hat_Orange Backwards Cap'] + dummies['Hat_Purple Backwards Cap'] + dummies['Hat_Solana Backwards Cap'] >= 1 ).astype(int)
			# del dummies['matching_white']
			# del dummies['matching_black']

		cat_metadata = pd.concat([ cat_metadata.reset_index(drop=True), dummies.reset_index(drop=True) ], axis=1)
		# del cat_metadata['pct']

		for c in model_exclude:
			if c in dummies.columns:
				del dummies[c]
		pred_cols = num_features + list(dummies.columns)
		pred_cols = [ c for c in pred_cols if not c in model_exclude+['Matching_No'] ]

		if len(sales) < 1000:
			pred_cols = [ x for x in pred_cols if 'rank' in x or 'is_top_' in x ]

		# create training df
		sales['token_id'] = sales.token_id.astype(str)
		num_metadata['token_id'] = num_metadata.token_id.astype(str)
		df = merge(sales, num_metadata, ['collection','token_id'], ensure=False)
		df = merge(df, cat_metadata, ['collection','token_id'], ensure=False)
		assert(len(df.columns) < 1000)

		# test dataFrame
		ensure = not collection in ['Aurory','Stoned Ape Crew']
		test = merge(num_metadata, cat_metadata, ['collection','token_id'], ensure=False)

		# if collection == 'Solana Monkey Business':
		# 	hat = metadata[ metadata.feature_name == 'Hat' ]
		# 	hat['color'] = hat.feature_value.apply(lambda x: re.split(' ', x)[0] )
		# 	clothes = metadata[ metadata.feature_name == 'Clothes' ]
		# 	clothes['color'] = clothes.feature_value.apply(lambda x: re.split(' ', x)[0] )
		# 	matching = hat[['token_id','color']].merge(clothes[['token_id','color']])
		# 	app = cat_metadata[ (dummies.matching_top == 1) | (dummies.matching_cop == 1) ][['token_id']]
		# 	matching = matching[['token_id']].append(app[['token_id']]).drop_duplicates()
		# 	matching['matching'] = 1
		# 	del dummies['matching_cop']
		# 	del dummies['matching_top']
		# 	# dummies = merge(dummies, matching, on=['token_id'], how='left').fillna(0)
		# 	df = merge(df, matching, on=['token_id'], how='left').fillna(0)
		# 	test = merge(test, matching, on=['token_id'], how='left').fillna(0)
		# 	pred_cols.append('matching')

		for c in num_features:
			df[c] = df[c].apply(lambda x: just_float(x))
			test[c] = test[c].apply(lambda x: just_float(x) )

		#################################
		#     Create Test DataFrame     #
		#################################
		tail = df.sort_values('timestamp').tail(1)
		if collection == 'Solana Monkey Business':
			test.loc[ test.token_id == '903', 'nft_rank' ] = 18
		for c in [ 'std_timestamp','mn_20','log_mn_20' ]:
			if c in tail.columns:
				test[c] = tail[c].values[0]

		for tmp in [df, test]:
			for i in [100, 250, 1000]:
				if collection in ['Levana Dragon Eggs']:
					tmp['is_top_{}'.format(i)] = (tmp.collection_rank <= i).astype(int)
				else:
					tmp['is_top_{}'.format(i)] = (tmp.nft_rank <= i).astype(int)
		pred_cols += [ 'is_top_100','is_top_250','is_top_1000' ]
		if 'collection_rank' in pred_cols:
			pred_cols = [ x for x in pred_cols if not x in ['nft_rank'] ]
		df.sort_values('price', ascending=0)[['price']].head(20)
		# df.groupby(['rarity','weight']).price.mean()

		# create target cols
		target_col = 'adj_price'
		df[target_col] = df.apply(lambda x: max(0.7 * (x['mn_20'] - 0.2), x['price']), 1 )
		# df['mn_20'] = df.mn_20 * 1.01
		df = df[df[target_col].notnull()]
		df['log_price'] = df[target_col].apply(lambda x: np.log(x) )
		df['rel_price_0'] = df[target_col] - df.mn_20
		df['rel_price_1'] = df[target_col] / df.mn_20
		df = df[df.mn_20 > 0]
		df['log_mn_20'] = np.log(df.mn_20)
		print('Training on {} sales'.format(len(df)))
		df = standardize_df(df, pred_cols)
		test = standardize_df(test, pred_cols, df)

		std_pred_cols_0 = [ 'std_{}'.format(c) for c in pred_cols ]
		std_pred_cols = [ 'std_{}'.format(c) for c in pred_cols ]
		df.sort_values('rel_price_0', ascending=0).head()[['token_id','nft_rank','price','rel_price_0']]

		#########################
		#     Run the Model     #
		#########################
		tmp = df[std_pred_cols].count().reset_index()
		tmp.columns = ['a','b']
		tmp.sort_values('b').head(20)
		rem = list(tmp[tmp.b==0].a.values)
		std_pred_cols = [ c for c in std_pred_cols if not c in rem ]
		# if collection == 'Levana Dragon Eggs':
		# 	std_pred_cols = [ 'std_essence_Dark','std_collection_rank_group_0','std_rarity_Legendary','std_rarity_Rare','std_rarity_Ancient','std_collection_rank','std_transformed_collection_rank' ]
		mn = df.timestamp.min()
		mx = df.timestamp.max()
		a_week_ago = (time.time() * 1000000000) - (60 * 60 * 24 * 7 * 1000000000)
		df['wt'] = df.timestamp.apply(lambda x: 4.0 ** ((x - mn) / (mx - mn)) )
		df.loc[ df.timestamp >= a_week_ago, 'wt' ] = 5
		df['wt'] = df.apply(lambda x: 0 if (x['train_exclude']==1 and (x['train_exclude_price'] != x['train_exclude_price'] or x['train_exclude_price'] == x['price'])) else x['wt'], 1 )
		df.loc[ (df.collection == 'Aurory') & (df.block_timestamp <= '2021-09-05'), 'wt' ] = 0.05
		if collection == 'Levana Dragon Eggs':
			df['wt'] = 1
		#     df['wt'] = df.price.apply(lambda x: 1.0 / (x ** 0.9) )
		#     df.sort_values('price', ascending=0)[['price','wt']].head(20)
		# std_pred_cols = [ 'std_Hat_Crown','std_adj_nft_rank_0','std_Hat_None','std_Eyes_None','std_Clothes_None','std_Attribute Count_4','std_Mouth_None','std_adj_nft_rank_1','std_Type_Dark','std_Ears_None','std_Background_Light purple','std_Hat_Black Fedora 2','std_Hat_White Fedora 2','std_Attribute Count_0','std_Type_Skeleton','std_Attribute Count_2','std_Attribute Count_1','std_Hat_Protagonist Black Hat','std_Clothes_Sailor Vest','std_Mouth_Pipe','std_Hat_Protagonist White Hat','std_Clothes_Pirate Vest','std_Hat_Roman Helmet','std_Type_Solana','std_Clothes_Beige Smoking','std_Hat_Military Helmet','std_Hat_White Fedora 1','std_naked_1_att','std_Type_Zombie','std_Clothes_Roman Armor','std_Eyes_3D Glasses','std_Clothes_Orange Kimono','std_Hat_Green Punk Hair','std_Hat_Sombrero','std_Clothes_Military Vest','std_Hat_Space Warrior Hair','std_Hat_Blue Punk Hair','std_Clothes_Orange Jacket','std_Ears_Earing Silver','std_Eyes_Laser Eyes','std_Eyes_Vipers','std_Type_Alien','std_Type_Red','std_Hat_Admiral Hat' ]
		# cur_std_pred_cols = [ 'std_adj_nft_rank_0','std_Hat_Crown','std_adj_nft_rank_1','std_Type_Skeleton','std_Type_Alien','std_Clothes_None','std_Eyes_Vipers','std_Hat_Space Warrior Hair','std_Type_Zombie','std_Clothes_Pirate Vest','std_Clothes_Orange Kimono','std_Eyes_Laser Eyes','std_Type_Solana','std_Hat_Ninja Bandana','std_Hat_Solana Backwards Cap','std_Eyes_Solana Vipers','std_Attribute Count_0','std_Attribute Count_1','std_Attribute Count_2','std_Attribute Count_3','std_Attribute Count_5','std_Hat_Strawhat','std_Hat_Admiral Hat','std_matching_top','std_Hat_Sombrero','std_matching_cop','std_Hat_Cowboy Hat','std_Hat_None' ]
		# cur_std_pred_cols = deepcopy(std_pred_cols)
		# g = df[std_pred_cols].sum().reset_index()
		# g.columns = [ 'col','cnt' ]
		# g = g.sort_values('cnt')
		# g.head(20)
		if collection == 'Solana Monkey Busines':
			df.loc[ df.token_id == '903', 'nft_rank' ] = 18
			df[df.token_id=='903']
			df[df.token_id==903]

		# CUR_FLOOR = df.sort_values('block_timestamp', ascending=0).mn_20.values[0]
		CUR_FLOOR = listings[(listings.collection == collection) & (listings.price.notnull())].price.min()
		print('CUR_FLOOR = {}'.format(CUR_FLOOR))
		df['tmp'] = df.nft_rank.apply(lambda x: int(x / 1000) )
		df.groupby('tmp').rel_price_0.mean()
		df.groupby('tmp').rel_price_0.median()
		df.groupby('tmp').rel_price_1.median()
		df.groupby('tmp').rel_price_1.mean()
		df[[ 'nft_rank','rel_price_0' ]].to_csv('~/Downloads/tmp.csv')
		if collection == 'MAYC':
			df = df[-((df.rel_price_0 >= 100) & (df.nft_rank > 1000))]
			df = df[(df.mn_20 >= 1)]
		if collection == 'BAYC':
			df = df[(df.mn_20 >= 10)]
		if collection == 'MAYC':
			df = df[(df.mn_20 >= 2)]
		df.sort_values('price', ascending=0).head(30)[['token_id','price','rel_price_0','rel_price_1','nft_rank','block_timestamp']]
		df.sort_values('rel_price_1', ascending=0).head(30)[['token_id','price','rel_price_0','rel_price_1','nft_rank','block_timestamp']]
		df.sort_values('rel_price_1', ascending=0).head(100)[['token_id','mn_20','price','rel_price_0','rel_price_1','nft_rank','block_timestamp']].to_csv('~/Downloads/tmp.csv', index=False)

		df = df.reset_index(drop=True)
		X = df[std_pred_cols].values
		wt = df['wt'].values
		y_0 = df.rel_price_0.values
		y_1 = df.rel_price_1.values
		df.sort_values('price', ascending=0).head(15)[['price','token_id','nft_rank','block_timestamp']]
		df[df.sim == 0].block_timestamp.max()

		for target_col in [ 'rel_price_0', 'rel_price_1' ]:
			it = target_col[-1]
			y_val = df[target_col].values
			print('target_col = {}'.format(target_col))
			mn = -1
			cols = [ 'std_nft_rank','std_adj_nft_rank_0','std_adj_nft_rank_1','std_adj_nft_rank_2' ]
			clf = Ridge(alpha = 1)
			# while mn < 0 and len(cols):
			# 	clf.fit(df[cols].values, y_val, df.wt.values)
			# 	coefs = get_coefs(cols, clf.coef_)
			# 	mn = min(coefs.val) if len(coefs) else 0
			# 	if mn < 0:
			# 		cols.remove(coefs.col.values[-1])

			col = 'rarity_value_'+it
			model = 'ridge'
			df[col] = 0
			test[col] = 0
			# df, bst_p, bst_r = ku.get_bst_params( model, df, df[cols].values, y_val, target_col, col, verbose = True, wt_col='wt'  )
			# test = ku.apply_model( model, bst_p, df, test, cols, target_col, col)

			# df['rarity_value_'+it] = clf.predict(df[cols].values)
			rar_adj_target_col = 'rar_adj_'+target_col
			df[rar_adj_target_col] = df[target_col] - df['rarity_value_'+it]
			# test[rar_adj_target_col] = test[target_col] - test['rarity_value_'+it]
			y_val_rar_adj = df[rar_adj_target_col].values
			models = ['las','ridge'] if target_col == 'rel_price_1' or len(sales) < 1000 else ['las','ridge','rfr']
			for model in models:
				cur_std_pred_cols = deepcopy(std_pred_cols)
				print(model)
				y = y_val_rar_adj if model in ['rfr'] else y_val
				col = 'y_pred_{}_{}'.format(model, it)
				params = [saved_params[collection][col]] if col in saved_params[collection].keys() and use_saved_params else []
				df, bst_p, bst_r = ku.get_bst_params( model, df, X, y, target_col, col, verbose = True, wt_col='wt', params = params )
				saved_params[collection][col] = bst_p

				# if model == 'ridge':
				# 	while len(cur_std_pred_cols) > 50:
				# 		coefs = get_coefs(cur_std_pred_cols, clf.coef_)
				# 		cur_std_pred_cols.remove(coefs.col.values[-1])
				# 		new_X = df[cur_std_pred_cols].values
				# 		clf = ku.get_model(model, bst_p)
				# 		clf.fit(new_X, y)
				# 		# coefs.to_csv('./data/coefs/{}_{}_{}.csv'.format(collection, model, it))
				# 	new_X = df[cur_std_pred_cols].values
				# 	df, bst_p, bst_r = ku.get_bst_params( model, df, new_X, y, target_col, col, verbose = True, wt_col='wt' )

				if model in ['las','ridge']:
					clf = ku.get_model(model, bst_p)
					clf.fit(X, y, wt)
					coefs = get_coefs(cur_std_pred_cols, clf.coef_)
					mn = coefs.val.min()
					while mn < 0:
						cur_std_pred_cols = [ c for c in coefs[coefs.val >= 0 ].col.unique() ]
						X_new = df[cur_std_pred_cols].values
						clf.fit(X_new, y, wt)
						coefs = get_coefs(cur_std_pred_cols, clf.coef_)
						mn = coefs.val.min()
						if mn >= 0:
							df, bst_p, bst_r = ku.get_bst_params( model, df, X_new, y, target_col, col, verbose = True, wt_col='wt', params = [bst_p] )
					coefs['col'] = coefs.col.apply(lambda x: re.sub('std_', '', x) )
					coefs['n'] = 0
					n = pd.DataFrame()
					for c in cat_metadata.columns:
						if not c in [ 'collection','token_id' ]:
							coefs.loc[ coefs.col == c, 'n' ] = len(cat_metadata[cat_metadata[c] == 1])
					coefs.to_csv('./data/coefs/{}_{}_{}.csv'.format(collection, model, it), index=False)

				test = ku.apply_model( model, bst_p, df, test, cur_std_pred_cols, target_col, col)
				if model in ['rfr']:
					df[col] = df[col] + df['rarity_value_'+it]
					test[col] = test[col] + test['rarity_value_'+it]

			mn = -1
			cols = [ c for c in df.columns if c[:7] == 'y_pred_' and c[-1] == it ]
			clf = LinearRegression()
			df[cols].mean()
			df[cols].median()
			test[cols].mean()
			test[cols].median()
			while mn < 0 and len(cols):
				clf.fit(df[cols].values, df[target_col].values)
				coefs = get_coefs(cols, clf.coef_)
				mn = min(coefs.val) if len(coefs) else 0
				if mn < 0:
					cols.remove(coefs.col.values[-1])
				else:
					print(coefs)
			if it == '0':
				df['pred_lin'] = clf.predict(df[cols].values) + df.mn_20
				test['pred_lin'] = clf.predict(test[cols].values) + CUR_FLOOR
				# df['pred_lin'] = df.pred_lin.apply(lambda x: max(0, x)) + df.mn_20
			else:
				df['pred_log'] = clf.predict(df[cols].values)
				df['pred_log'] = df.pred_log.apply(lambda x: max(1, x)) * df.mn_20
				test['pred_log'] = clf.predict(test[cols].values)
				test['pred_log'] = test.pred_log.apply(lambda x: max(1, x)) * CUR_FLOOR

		clf = LinearRegression(fit_intercept=False)
		target_col = 'adj_price'
		clf.fit( df[['pred_lin','pred_log']].values, df[target_col].values, df.wt.values )
		score = clf.score( df[['pred_lin','pred_log']].values, df[target_col].values, df.wt.values )
		tmp = df[['token_id','block_timestamp','wt','mn_20','pred_lin','pred_log','price','nft_rank']]
		tmp['block_timestamp'] = tmp.block_timestamp.apply(lambda x: str(x)[:10] )
		tmp['err_0'] = tmp.pred_lin - tmp.price
		tmp['err_1'] = tmp.pred_log / tmp.price
		tmp.to_csv('~/Downloads/tmp.csv', index=False)
		print('R-Sq: {}'.format(round(score * 100, 1)))
		# df[['pred_lin','pred_log',target_col]].mean()
		# df[['pred_lin','pred_log',target_col]].median()
		# test[['pred_lin','pred_log']].mean()
		# test[['pred_lin','pred_log']].median()
		
		print('Price = {} * lin + {} * log'.format( round(clf.coef_[0], 2), round(clf.coef_[1], 2) ))
		tmp = pd.DataFrame([[collection, clf.coef_[0], clf.coef_[1], CUR_FLOOR]], columns=['collection','lin_coef','log_coef','floor_price'])
		if clf.coef_[0] < 0:
			print('Only using log')
			df['pred'] = df.pred_log
			test['pred'] = test.pred_log
			tmp['lin_coef'] = 0
			tmp['log_coef'] = 1
		elif clf.coef_[1] < 0:
			print('Only using lin')
			df['pred'] = df.pred_lin
			test['pred'] = test.pred_lin
			tmp['lin_coef'] = 1
			tmp['log_coef'] = 0
		else:
			print('Only using BOTH!')
			df['pred'] = clf.predict( df[['pred_lin','pred_log']].values )
			test['pred'] = clf.predict( test[['pred_lin','pred_log']].values )
		coefsdf = coefsdf.append(tmp)
		df['err'] = (df.pred / df[target_col]).apply(lambda x: abs(x-1) )
		df['err'] = df[target_col] - df.pred
		df.head()
		# df[df['std_Attribute count_4']==1]['err']
		df['w_err'] = df.err * df.wt
		# df[df['std_Attribute count_4']==1].sort_values('timestamp')[['err','w_err']].mean()
		# df[(df['std_Attribute count_4']==1)].sort_values('timestamp')[['err','w_err']].mean()
		# df[(df['std_Attribute count_4']==1) & (df.wt>=15)].sort_values('timestamp')[['err','w_err']].mean()
		# df[(df['std_Attribute count_4']==1) & (df.wt>=15)].sort_values('timestamp')[['err','w_err']].sum()
		# df[(df['std_Attribute count_4']==1) & (df.wt<15)].sort_values('timestamp')[['err','w_err']].mean()
		# df[(df['std_Attribute count_4']==1) & (df.wt<15)].sort_values('timestamp')[['err','w_err']].sum()
		# df[df['std_Attribute count_4']==1].sort_values('timestamp')[['err','price','pred','block_timestamp']].tail(20)
		# df[(df['std_Attribute count_4']==1) & (df.wt>=1)].sort_values('timestamp')[['err','price','pred','block_timestamp','wt','w_err']].tail(50)
		# df[df.wt >= 15].wt.sum() / df.wt.sum()
		# df[df.wt < 15].wt.sum()

		recent_errs = []
		recent = df[df.timestamp >= a_week_ago]
		for c in [ c for c in cur_std_pred_cols if len(df[c].unique()) == 2]:
			a = recent[recent[c] == 1]
			recent_errs += [[ c, len(a), a.err.mean(), a.err.sum(), a.err.sum() / recent.price.sum() ]]
		recent_errs = pd.DataFrame(recent_errs, columns=['col','n','avg','tot','rat']).sort_values('tot')
		recent_errs['abs_rat'] = abs(recent_errs.rat)
		recent_errs.sort_values('rat')
		correct = recent_errs[(recent_errs.abs_rat > 0.003) & (recent_errs.n >= 10)]
		if len(correct):
			mx = max(0.001, correct.abs_rat.max())
			correct['chg'] = (correct.abs_rat / mx).apply(lambda x: min(1, x) * .7 ) * correct.avg
			correct['abs_chg'] = abs(correct.chg)
			print(correct.sort_values('chg'))
		for row in correct.iterrows():
			row = row[1]
			c = row['col']
			test['pred'] = test.apply(lambda x: x['pred'] if x[c] == 0 else x['pred'] + row['chg'], 1 )

		# print out some summary stats
		df['err'] = df[target_col] - df.pred
		df['q'] = (df.pred.rank() ** 1.5 * .2) / len(df)
		df['q'] = df.q.apply(lambda x: int(round(x)) )
		df['pct_err'] = (df[target_col] / df.pred) - 1
		pe_mu = df.pct_err.mean()
		pe_sd = df[ (df.pct_err > -.9) & (df.pct_err < 0.9) & (df.days_ago<=50) ].pct_err.std()
		if pe_sd != pe_sd:
			pe_sd = df[ (df.pct_err > -.9) & (df.pct_err < 0.9) ].pct_err.std()
		df['pred_price'] = df.pred#.apply(lambda x: x*(1+pe_mu) )
		if collection == 'Levana Dragon Eggs':
			df['pred_price'] = df.pred.apply(lambda x: x*1.01 )
		df['pred_sd'] = df.pred * pe_sd
		# print(df[df.wt >= df.wt.median()].groupby('q')[['err','pred',target_col]].mean())
		# print(df.groupby('q')[['err','pred',target_col]].mean())
		# df.err.mean()
		# df[df.weight >= 3.5].err.mean()
		df[df.pred < 200].err.mean()
		df['collection'] = collection
		print('Avg err last 100: {}'.format(round(df.sort_values('block_timestamp').head(100).err.mean(), 2)))
		# salesdf = salesdf.append( df.rename(columns={'collection_rank':'nft_rank'}).merge(s_df[s_df.sim == 0][['collection','token_id','block_timestamp','price']] )[[ 'collection','token_id','block_timestamp','price','pred','mn_20','nft_rank' ]].sort_values('block_timestamp', ascending=0) )
		salesdf = salesdf.append( df.merge(s_df[s_df.sim == 0][['collection','token_id','block_timestamp','price']] )[[ 'collection','token_id','block_timestamp','price','pred','mn_20','nft_rank' ]].sort_values('block_timestamp', ascending=0) )

		############################################################
		#     Create Predictions for Each NFT in The Collection    #
		############################################################
		# test = merge(num_metadata, cat_metadata, ['collection','token_id'])
		# for c in num_features:
		# 	test[c] = test[c].apply(lambda x: just_float(x) )
		# tail = df.sort_values('timestamp').tail(1)
		# test.loc[ test.token_id == '903', 'nft_rank' ] = 18
		# for c in [ 'std_timestamp','mn_20','log_mn_20' ]:
		# 	if c in tail.columns:
		# 		test[c] = tail[c].values[0]
		# test = standardize_df(test, pred_cols, df)

		# test['pred_lin'] = clf_lin.predict(test[lin_std_pred_cols].values)
		# test['pred_lin'] = test.pred_lin.apply(lambda x: max(0, x) + l)
		# test['pred_log'] = clf_log.predict(test[log_std_pred_cols].values)
		# test['pred_log'] = test.pred_log.apply(lambda x: max(1, x)) * l

		# test['pred_price'] = test.pred.apply(lambda x: x if x < 400 else (x-400)**0.96 + 400 )
		def f(p):
			c = CUR_FLOOR * 2.5
			if collection == 'Degen Apes':
				return( p if p <= c else c+((p-c) ** 0.94) )
			return( p if p <= c else c+((p-c) ** 0.95) )
		test['pred_price'] = test.pred.apply(lambda x: f(x) )
		len(test[test.pred <= CUR_FLOOR * 1.01])
		len(test[test.pred <= CUR_FLOOR * 1.02])
		if not check_exclude:
			test['pred_price'] = test.pred_price.apply(lambda x: (x*0.985) )
		if collection == 'BAYC':
			test['pred_price'] = test.pred_price.apply(lambda x: (x*1.03) )
		if collection == 'Galactic Angels':
			test['pred_price'] = test.pred_price.apply(lambda x: (x** 1.05) * 1.2 ) 

		tmp = listings[listings.collection == collection][['token_id','price']].merge(test[['token_id','pred_price']])
		tmp['ratio'] = (tmp.pred_price / tmp.price)
		tmp['is_deal'] = (tmp.ratio > 1).astype(int)
		mx = tmp.ratio.max()
		if mx < 1.15:
			test['pred_price'] = test.pred_price * 1.15 / mx

		# make sure the lowest pred price is the floor
		dff = test.pred_price.min() - CUR_FLOOR
		if dff > 0:
			test['pred_price'] = test.pred_price - dff

		test['pred_sd'] = test.pred_price * pe_sd
		test = test.sort_values(['collection','token_id'])
		test['rk'] = test.pred_price.rank(ascending=0, method='first')
		test['collection'] = collection
		if 'collection_rank' in test.columns and (not 'nft_rank' in test.columns or len(test[test.nft_rank.notnull()]) < len(test[test.collection_rank.notnull()])):
			test['nft_rank'] = test.collection_rank
		pred_price = pred_price.append( test[[ 'collection','token_id','nft_rank','rk','pred_price','pred_sd' ]].sort_values('pred_price') ).drop_duplicates(subset=['collection','token_id'], keep='last')


		imp = []
		# a = [ 'matching' ] if collection == 'Solana Monkey Business' else []
		for c in list(dummies.columns):
			md = test[test[c] == 1].pred_price.median()
			md_0 = test.pred_price.quantile(0.475)
			imp += [[ collection, c, md_0, md ]]
		# imp = pd.DataFrame(imp, columns=['collection','feature_name',''])
		imp = pd.DataFrame(imp, columns=['collection','col','col_md','md']).sort_values('md', ascending=0)
		imp['pct_vs_baseline'] = ((imp.md / imp.col_md) - 1).apply(lambda x: max(0, x))
		imp['feature_name'] = imp.col.apply(lambda x: re.split('_', x)[0].title() )
		imp['feature_value'] = imp.col.apply(lambda x: re.split('_', x)[1] if '_' in x else None )
		sorted(imp.feature_name.unique())
		imp.loc[imp.col == 'Matching_No', 'pct_vs_baseline'] = 0
		imp[imp.feature_name == 'Attribute Count']
		# if 'matching' in a:
		# 	imp.loc[imp.feature_name == 'Matching', 'feature_value'] = 'Yes'
		# 	test[test.matching==1].to_csv('~/Downloads/tmp1.csv', index=False)
		feature_values = feature_values.append(imp[['collection','feature_name','feature_value','pct_vs_baseline']])

		cols = metadata.feature_name.unique()
		cols = [ x for x in cols if not x in (ATT_EXCLUDE_COLS[collection] if collection in ATT_EXCLUDE_COLS.keys() else []) + ALL_NUMERIC_COLS ]
		exclude = RARITY_EXCLUDE_COLS[collection] if collection in RARITY_EXCLUDE_COLS.keys() else []
		for c in cols:
			cur = metadata[metadata.feature_name == c][['collection','token_id','feature_name','feature_value']]
			l = len(cur.token_id.unique())
			if c in exclude:
				cur['rarity'] = None
			else:
				g = cur.groupby('feature_value').token_id.count().reset_index()
				g['rarity'] = g.token_id / l
				cur = merge(cur, g[['feature_value','rarity']])
			attributes = attributes.append(cur)
	
	attributes['feature_name'] = attributes.feature_name.apply(lambda x: re.sub('_', ' ', x).title().strip() )
	attributes['feature_value'] = attributes.feature_value.apply(lambda x: str(x).strip() )
	sorted(attributes['feature_name'].unique())
	if len(feature_values):
		feature_values['feature_name'] = feature_values.feature_name.apply(lambda x: re.sub('_', ' ', x).title() )
	# feature_values = pd.read_csv('./data/feature_values.csv')
	feature_values = feature_values.merge(attributes[['collection','feature_name']].drop_duplicates())
	# n = feature_values[['collection', 'feature_name']].drop_duplicates().groupby(['collection']).feature_name.count().reset_index().rename(columns={'feature_name': 'n'})
	# feature_values = feature_values.merge(n)
	# feature_values['pct_vs_baseline'] = feature_values.pct_vs_baseline / feature_values.n
	# del feature_values['n']
	feature_values[ (feature_values.collection == 'Galactic Angels') ]
	feature_values[ (feature_values.collection == 'Solana Monkey Business') &  (feature_values.feature_name == 'Clothes')  ]
	feature_values[ (feature_values.collection == 'Solana Monkey Business') & (feature_values.feature_name == 'Clothes') & (feature_values.feature_value == 'Poncho') ]
	feature_values[ (feature_values.collection == 'Okay Bears') & (feature_values.feature_name == 'Attribute Count')]
	attributes[ (attributes.collection == 'Solana Monkey Business') & (attributes.feature_name == 'Clothes') & (attributes.feature_value == 'Poncho') & (attributes.token_id == '1') ]
	attributes[ (attributes.collection == 'Solana Monkey Business') & (attributes.feature_name == 'Clothes') & (attributes.feature_value == 'Poncho') & (attributes.token_id == 1) ]

	coefsdf.to_csv('./data/coefsdf.csv', index=False)
	salesdf.to_csv('./data/model_sales.csv', index=False)
	# salesdf[salesdf.collection]
	salesdf['block_timestamp'] = salesdf.block_timestamp.apply(lambda x: str(x)[:19] )
	salesdf[salesdf.collection == 'BAYC'].sort_values('block_timestamp', ascending=0).head()[['token_id','block_timestamp','price']]
	salesdf[salesdf.block_timestamp.isnull()]
	salesdf.block_timestamp.max()
	salesdf.groupby('collection').block_timestamp.max()
	pred_price[pred_price.collection == 'SOLGods'].to_csv('~/Downloads/tmp1.csv', index=False)
	# old = pd.read_csv('./data/pred_price.csv')
	# old = old[old.collection == 'DeGods']
	# old['token_id'] = old.token_id.astype(str)
	# old = pred_price.merge(old, on=['collection','token_id'])
	# old['ratio'] = old.pred_price_x / old.pred_price_y
	# old = old.sort_values('ratio')
	# old.columns = [ 'collection', 'token_id', 'nft_rank', 'rk_new', 'pred_price_new', 'pred_sd_x', 'rank', 'rk_old', 'pred_price_old', 'pred_sd_y', 'ratio' ]
	# # old.columns = [ 'collection', 'token_id', 'nft_rank', 'rk_new', 'pred_price_new', 'pred_sd_x', 'rank', 'rk_old', 'pred_price_old', 'pred_sd_y', 'clean_token_id', 'ratio' ]
	# m = m_df[(m_df.collection.isin(pred_price.collection.unique())) & (-(m_df.feature_name.isin(['nft_rank','adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2'])))]
	# m_p = m.pivot(['collection','token_id'], ['feature_name'], ['feature_value']).reset_index()
	# m_p.columns = [ 'collection','token_id' ] + sorted(m.feature_name.unique())
	# m_p.head()
	# old = old.merge(m_p, on=['collection','token_id'])
	# if len(old) and 'rank' in old.columns:
	# 	# old = old[[ 'token_id', 'nft_rank', 'rk_old', 'rk_new', 'pred_price_old', 'pred_price_new', 'ratio' ] + [c for c in m_p.columns if not c in ['token_id','collection']]]
	# 	old = old[[ 'token_id', 'nft_rank', 'rk_old', 'rk_new', 'pred_price_old', 'pred_price_new', 'ratio' ] + [c for c in m_p.columns if not c in ['token_id','collection','rank']]]
	# 	old.to_csv('~/Downloads/tmp1.csv', index=False)
	# 	pred_price.head()
	# 	old[old.token_id == '4857']
	# 	old.head()
	# 	old.tail()

	# nft_rank = m_df[m_df.feature_name=='nft_rank'][['collection','token_id','feature_value']].rename(columns={'feature_value': 'nft_rank'})
	# nft_rank['token_id'] = nft_rank.token_id.astype(str)
	# pred_price['token_id'] = pred_price.token_id.astype(str)
	# pred_price = pred_price.merge(nft_rank, how='left', on=['collection','token_id'])
	# pred_price = pred_price[pred_price.collection != 'LunaBulls']
	pred_price['collection'] = pred_price.collection.apply(lambda x: clean_name(x))
	pred_price = pred_price.drop_duplicates(subset=['collection','token_id'], keep='last')
	pred_price.to_csv('./data/pred_price.csv', index=False)
	# pred_price = pd.read_csv('./data/pred_price.csv')
	pred_price.groupby('collection')[['pred_price']].min()
	attributes.to_csv('./data/attributes.csv', index=False)
	attributes = pd.read_csv('./data/attributes.csv')
	attributes[attributes.rarity.isnull()]
	feature_values.to_csv('./data/feature_values.csv', index=False)
	feature_values[feature_values.collection == 'Galactic Angels'].pct_vs_baseline.unique()
	feature_values[ (feature_values.collection == 'Galactic Angels') & (feature_values.feature_name == 'Background')].feature_value.unique()
	attributes[attributes.collection == 'Galactic Angels'].head()

	# metadata = pd.read_csv('./data/metadata.csv')
	# metadata['collection'] = metadata.collection.apply(lambda x: clean_name(x))
	# metadata['token_id'] = metadata.token_id.astype(str)
	# metadata.head()
	# nft_rank = pred_price[[ 'collection','token_id','nft_rank' ]].rename(columns={'nft_rank':'feature_value'})
	# nft_rank['feature_name'] = 'nft_rank'
	# metadata = metadata[metadata.feature_name != 'nft_rank']
	# nft_rank = merge(nft_rank, metadata[['collection','chain']].fillna('Solana').drop_duplicates())
	# metadata = metadata.append(nft_rank)
	# metadata.to_csv('./data/metadata.csv', index=False)


	# feature_values.to_csv('./data/feature_values.csv', index=False)

	file_to_store = open('./objects/saved_params.pickle', 'wb')
	pickle.dump(saved_params, file_to_store)

	if True or check_exclude:
		exclude = pd.read_csv('./data/exclude.csv')
		salesdf['rat'] = salesdf.price / salesdf.pred
		salesdf['dff'] = salesdf.price - salesdf.pred
		salesdf['exclude_1'] = (((salesdf.dff >= 20) & (salesdf.rat > 4)) | ((salesdf.dff >= 40) & (salesdf.rat > 3)) | ((salesdf.dff >= 60) & (salesdf.rat > 2.5)) | ((salesdf.dff >= 80) & (salesdf.rat > 2.5))).astype(int)
		salesdf['rat'] = salesdf.pred / salesdf.price
		salesdf['dff'] = salesdf.pred - salesdf.price
		salesdf['exclude_2'] = (((salesdf.dff >= 20) & (salesdf.rat > 4)) | ((salesdf.dff >= 40) & (salesdf.rat > 3)) | ((salesdf.dff >= 60) & (salesdf.rat > 2.5)) | ((salesdf.dff >= 80) & (salesdf.rat > 2.5))).astype(int)
		salesdf['exclude'] = (salesdf.exclude_1 + salesdf.exclude_2).apply(lambda x: int(x>0))
		# print(salesdf.exclude_1.mean())
		# print(salesdf.exclude_2.mean())
		# print(salesdf.exclude.mean())
		salesdf[salesdf.token_id == '2239'][['collection','price','exclude']]
		exclude = exclude.append(salesdf[salesdf.exclude == 1][[ 'collection','token_id','price','exclude' ]])
		# salesdf[salesdf.exclude == 1][[ 'collection','token_id','price','exclude' ]].to_csv('./data/exclude.csv', index=False)
		exclude.to_csv('./data/exclude.csv', index=False)
	# tokens[tokens.collection == 'Meerkat Millionaires']
	# tokens[tokens.collection == 'Cets on Creck'].sort_values('nft_rank', ascending=0)
	# tokens[tokens.collection == 'Cets on Creck']
	# tokens = tokens.drop_duplicates(subset=['collection','token_id'], keep='last')
	# tokens['chain'] = tokens.chain.fillna('Solana')
	# tokens['clean_token_id'] = tokens.clean_token_id.fillna(tokens.token_id)
	# tokens.to_csv('./data/tokens.csv', index=False)

# train_model(True, False)
# train_model(False, False)
# train_model(False, True)

# train_model()