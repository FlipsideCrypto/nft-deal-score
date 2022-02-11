import collections
import os
import re
import json
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
from scrape_sol_nfts import clean_name

warnings.filterwarnings('ignore')


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

	s_df = pd.read_csv('./data/sales.csv').rename(columns={'sale_date':'block_timestamp'})
	s_df['token_id'] = s_df.token_id.astype(str)
	s_df['collection'] = s_df.collection.apply(lambda x: clean_name(x))
	s_df = s_df[-s_df.collection.isin(['Levana Meteors','Levana Dust'])]
	s_df = s_df[ -s_df.collection.isin(['boryokudragonz', 'Boryoku Dragonz']) ]
	s_df = s_df[[ 'chain','collection','block_timestamp','token_id','price','tx_id' ]]
	for e in exclude:
		s_df = s_df[-( (s_df.collection == e[0]) & (s_df.token_id == e[1]) & (s_df.price == e[2]) )]
	s_df = s_df[ -((s_df.collection == 'smb') & (s_df.price < 1)) ]

	# exclude wierd data points
	if not check_exclude:
		exclude = pd.read_csv('./data/exclude.csv')
		exclude['collection'] = exclude.collection.apply(lambda x: clean_name(x))
		exclude['token_id'] = exclude.token_id.astype(str)
		s_df = s_df.merge(exclude, how='left')
		s_df = s_df[s_df.exclude.isnull()]
		del s_df['exclude']

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
	s_df['mn_20'] = s_df.groupby('collection')['mn_20'].rolling(20).quantile(.1).reset_index(0,drop=True)
	s_df['sim'] = 0
	s_df['tmp'] = s_df.block_timestamp.apply(lambda x: str(x)[:10] )
	s_df.groupby(['collection','tmp']).mn_20.mean().reset_index().to_csv('~/Downloads/mn_20.csv', index=False)
	return(s_df)

def train_model(check_exclude, supplement_with_listings):
	exclude = [
		( 'aurory', 2239, 3500 )
	]
	s_df = get_sales(check_exclude, exclude)
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
	m_df['token_id'] = m_df.token_id.astype(str)
	m_df['collection'] = m_df.collection.apply(lambda x: clean_name(x))
	m_df['token_id'] = m_df.token_id.astype(str)
	# remove ones that are not actually metadata
	m_df = m_df[ -m_df.feature_name.isin([ 'price','last_sale','feature_name','feature_value' ]) ]
	m_df['feature_value'] = m_df.feature_value.apply(lambda x: re.split("\(", re.sub("\"", "", x))[0] if type(x)==str else x )
	m_df[(m_df.feature_name=='rank') & (m_df.collection == 'Levana Dragon Eggs')]
	sorted(m_df[ (m_df.collection == 'Solana Monkey Business') ].feature_name.unique())


	#####################################
	#     Exclude Special LunaBulls     #
	#####################################
	tokens = pd.read_csv('./data/tokens.csv')
	tokens['collection'] = tokens.collection.apply(lambda x: clean_name(x))
	tokens.token_id.unique()
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

	if supplement_with_listings:
		pred_price = pd.read_csv('./data/pred_price.csv')
		pred_price['collection'] = pred_price.collection.apply(lambda x: clean_name(x))
		listings = pd.read_csv('./data/listings.csv')
		listings['collection'] = listings.collection.apply(lambda x: clean_name(x))
		listings['block_timestamp'] = s_df.block_timestamp.max()
		floor = s_df.sort_values('timestamp').groupby('collection').tail(1)[['collection','mn_20']]
		tmp = merge(listings, pred_price, ensure=False)
		tmp = tmp[tmp.price < tmp.pred_price]
		tmp['timestamp'] = tmp.block_timestamp.astype(int)
		tmp['days_ago'] = tmp.block_timestamp.apply(lambda x: (datetime.today() - x).days ).astype(int)
		tmp = merge(tmp, floor)

		n = round(len(s_df) / 5000)
		n = max(1, min(2, n))
		# n = 1
		for _ in range(n):
			s_df = s_df.append(tmp[[ 'block_timestamp','timestamp','collection','token_id','price','mn_20' ]])
			# tmp_1 = tmp[tmp.price <= 0.8 * tmp.pred_price]
			# s_df = s_df.append(tmp_1[[ 'block_timestamp','timestamp','collection','token_id','price','mn_20' ]])
			# tmp_2 = tmp[tmp.price <= 0.6 * tmp.pred_price]
			# tmp_2 = s_df.append(tmp_2[[ 'block_timestamp','timestamp','collection','token_id','price','mn_20' ]])


	###########################
	#     Calculate Floor     #
	###########################
	coefsdf = pd.DataFrame()
	salesdf = pd.DataFrame()
	attributes = pd.DataFrame()
	pred_price = pd.DataFrame()
	feature_values = pd.DataFrame()
	# non-binary in model: collection_rank, temperature, weight
	# non-binary in model; exclude from rarity: pct, rank, score
	# exclude from model: lucky_number, shower
	# exclude from model and rarity %: meteor_id, attribute_count, cracking_date
	ALL_NUMERIC_COLS = ['rank','score','pct']
	ALL_NUMERIC_COLS = ['nft_rank','adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2']
	MODEL_EXCLUDE_COLS = {
		# 'Levana Dragon Eggs': ['collection_rank','meteor_id','shower','lucky_number','cracking_date','attribute_count','weight','temperature']
		'Levana Dragon Eggs': ['meteor_id','shower','lucky_number','cracking_date','attribute_count','rarity_score_rank','rarity_score','weight']
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
	collection = 'Solana Monkey Business'
	# for collection in s_df.collection.unique():
	for collection in [ 'Solana Monkey Business' ]:
		print('Working on collection {}'.format(collection))
		sales = s_df[ s_df.collection == collection ]
		metadata = m_df[ m_df.collection == collection ]
		metadata.groupby(['feature_name','feature_value']).token_id.count().reset_index().to_csv('~/Downloads/tmp.csv', index=False)
		metadata[metadata.token_id == '1']
		metadata['feature_name'] = metadata.feature_name.apply(lambda x: x.strip() )
		metadata[metadata.token_id == '1']
		metadata[metadata.feature_name == 'rank']
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
		cat_metadata = calculate_percentages( cat_metadata, cat_features )
		dummies = pd.get_dummies(cat_metadata[cat_features])
		dummies.head(1).to_csv('~/Downloads/tmp2.csv', index=False)
		if collection == 'Solana Monkey Business':
			dummies['matching_cop'] = ((dummies['Clothes_Cop Vest'] == 1) & (dummies['Hat_Cop Hat'] == 1)).astype(int)
			dummies['matching_white'] = ((dummies['Clothes_Beige Smoking'] == 1) & ((dummies['Hat_White Fedora 1'] + dummies['Hat_White Fedora 2']) == 1)).astype(int)
			dummies['matching_black'] = ((dummies['Clothes_Black Smoking'] == 1) & ((dummies['Hat_Black Fedora 1'] + dummies['Hat_Black Fedora 2'] + dummies['Hat_Black Top Hat']) == 1)).astype(int)
			dummies['matching_top'] = ((dummies['matching_black'] == 1) | (dummies['matching_white']== 1)).astype(int)
			# dummies['matching_green'] = ((dummies['Clothes_Green Smoking'] == 1) & ((dummies['Hat_Green Top Hat']) == 1)).astype(int)
			# dummies['naked_1_att'] = ((dummies['Attribute Count_1'] == 1) & (dummies['Clothes_None'] == 1)).astype(int)
			# dummies['naked_1_att_hat'] = ((dummies['Attribute Count_1'] == 1) & (dummies['Hat_None'] == 0)).astype(int)
			dummies['fedora'] = (dummies['Hat_Black Fedora 1'] + dummies['Hat_Black Fedora 2'] + dummies['Hat_White Fedora 1'] + dummies['Hat_White Fedora 2'] + dummies['Hat_White Fedora 2'] >= 1 ).astype(int)
			dummies['backwards_cap'] = (dummies['Hat_Black Backwards Cap'] + dummies['Hat_Blue Backwards Cap'] + dummies['Hat_Green Backwards Cap'] + dummies['Hat_Orange Backwards Cap'] + dummies['Hat_Purple Backwards Cap'] + dummies['Hat_Solana Backwards Cap'] >= 1 ).astype(int)
			del dummies['matching_white']
			del dummies['matching_black']
		cat_metadata = pd.concat([ cat_metadata.reset_index(drop=True), dummies.reset_index(drop=True) ], axis=1)
		del cat_metadata['pct']

		for c in model_exclude:
			if c in dummies.columns:
				del dummies[c]
		pred_cols = num_features + list(dummies.columns)

		# create training df
		df = merge(sales, num_metadata, ['collection','token_id'], ensure=False)
		df = merge(df, cat_metadata, ['collection','token_id'])
		df[df.adj_nft_rank_0 == 'None']
		df[df.adj_nft_rank_0 == 'None'][['collection','token_id','nft_rank','adj_nft_rank_0']]
		df.adj_nft_rank_0.unique()
		for c in num_features:
			df[c].unique()
			df[df.nft_rank == 'None']
			df[df[c] == 'None'][[ 'nft_rank' ]]
			df[c] = df[c].apply(lambda x: just_float(x))
		df.sort_values('price', ascending=0)[['price']].head(20)
		# df.groupby(['rarity','weight']).price.mean()

		# create target cols
		target_col = 'adj_price'
		df[target_col] = df.apply(lambda x: max(0.7 * (x['mn_20'] - 0.2), x['price']), 1 )
		df = df[df[target_col].notnull()]
		df['log_price'] = df[target_col].apply(lambda x: np.log(x) )
		df['rel_price_0'] = df[target_col] - df.mn_20
		df['rel_price_1'] = df[target_col] / df.mn_20
		df = df[df.mn_20 > 0]
		df['log_mn_20'] = np.log(df.mn_20)
		print('Training on {} sales'.format(len(df)))
		df = standardize_df(df, pred_cols)

		std_pred_cols_0 = [ 'std_{}'.format(c) for c in pred_cols ]
		std_pred_cols = [ 'std_{}'.format(c) for c in pred_cols ]

		#########################
		#     Run the Model     #
		#########################
		tmp = df[std_pred_cols].count().reset_index()
		tmp.columns = ['a','b']
		tmp.sort_values('b').head(20)
		rem = list(tmp[tmp.b==0].a.values)
		std_pred_cols = [ c for c in std_pred_cols if not c in rem ]
		if collection == 'Levana Dragon Eggs':
			std_pred_cols = [ 'std_essence_Dark','std_collection_rank_group_0','std_rarity_Legendary','std_rarity_Rare','std_rarity_Ancient','std_collection_rank','std_transformed_collection_rank' ]
		mn = df.timestamp.min()
		mx = df.timestamp.max()
		df['wt'] = df.timestamp.apply(lambda x: 3.0 ** ((x - mn) / (mx - mn)) )
		if collection == 'Levana Dragon Eggs':
			df['wt'] = 1
		#     df['wt'] = df.price.apply(lambda x: 1.0 / (x ** 0.9) )
		#     df.sort_values('price', ascending=0)[['price','wt']].head(20)
		# std_pred_cols = [ 'std_Hat_Crown','std_adj_nft_rank_0','std_Hat_None','std_Eyes_None','std_Clothes_None','std_Attribute Count_4','std_Mouth_None','std_adj_nft_rank_1','std_Type_Dark','std_Ears_None','std_Background_Light purple','std_Hat_Black Fedora 2','std_Hat_White Fedora 2','std_Attribute Count_0','std_Type_Skeleton','std_Attribute Count_2','std_Attribute Count_1','std_Hat_Protagonist Black Hat','std_Clothes_Sailor Vest','std_Mouth_Pipe','std_Hat_Protagonist White Hat','std_Clothes_Pirate Vest','std_Hat_Roman Helmet','std_Type_Solana','std_Clothes_Beige Smoking','std_Hat_Military Helmet','std_Hat_White Fedora 1','std_naked_1_att','std_Type_Zombie','std_Clothes_Roman Armor','std_Eyes_3D Glasses','std_Clothes_Orange Kimono','std_Hat_Green Punk Hair','std_Hat_Sombrero','std_Clothes_Military Vest','std_Hat_Space Warrior Hair','std_Hat_Blue Punk Hair','std_Clothes_Orange Jacket','std_Ears_Earing Silver','std_Eyes_Laser Eyes','std_Eyes_Vipers','std_Type_Alien','std_Type_Red','std_Hat_Admiral Hat' ]
		cur_std_pred_cols = [ 'std_adj_nft_rank_0','std_Hat_Crown','std_adj_nft_rank_1','std_Type_Skeleton','std_Type_Alien','std_Clothes_None','std_Eyes_Vipers','std_Hat_Space Warrior Hair','std_Type_Zombie','std_Clothes_Pirate Vest','std_Clothes_Orange Kimono','std_Eyes_Laser Eyes','std_Type_Solana','std_Hat_Ninja Bandana','std_Hat_Solana Backwards Cap','std_Eyes_Solana Vipers','std_Attribute Count_0','std_Attribute Count_1','std_Attribute Count_2','std_Attribute Count_3','std_Attribute Count_5','std_Hat_Strawhat','std_Hat_Admiral Hat','std_matching_top','std_Hat_Sombrero','std_matching_cop','std_Hat_Cowboy Hat','std_Hat_None' ]
		cur_std_pred_cols = deepcopy(std_pred_cols)
		g = df[std_pred_cols].sum().reset_index()
		g.columns = [ 'col','cnt' ]
		g = g.sort_values('cnt')
		g.head(20)
		if collection == 'Solana Monkey Busines':
			df.loc[ df.token_id == '903', 'nft_rank' ] = 18
			df[df.token_id=='903']
			df[df.token_id==903]
		df = df.reset_index(drop=True)
		X = df[cur_std_pred_cols].values
		y_0 = df.rel_price_0.values
		y_1 = df.rel_price_1.values
		folds = ku.get_folds(len(X), 5)
		for target_col in [ 'rel_price_0', 'rel_price_1' ]:
			print('target_col = {}'.format(target_col))
			y_val = df[target_col].values
			cur_err = 0
			for model in ['las','ridge','rfr','gbr']:
				df, bst_p, bst_r = ku.get_bst_params( model, df, X, y_val, target_col, 'y_pred_{}_{}'.format(model, target_col[-1]), verbose = True, wt_col='wt'  )
		# df['tmp'] = df.collection_rank.apply(lambda x: int((8888 - x)/1000) )
		# g = df.groupby('tmp').rel_price_0.mean().reset_index()
		# g['g'] = g.tmp.apply(lambda x: (((1.42**(x**1.42)) - 1) / 20) + 0.13 )
		# g['g'] = g.tmp.apply(lambda x: 2**x )
		# g

		# run the linear model
		# clf_lin = Lasso(alpha=1.0) if collection in [ 'Levana Dragon Eggs' ] else RidgeCV(alphas=[1.5**x for x in range(20)])

		# clf_lin = Ridge(alpha=1000)
		# clf_lin = Ridge(alpha=100)
		# clf_lin.fit(X, y_0, df.wt.values)
		# clf_las = Lasso(alpha=1.5)
		# clf_las.fit(X, y_0, df.wt.values)
		# clf_rfr = RandomForestRegressor()
		# clf_rfr.fit(X, y_0)
		# clf_rfr.feature_importances_
		# imp = []
		# for a, b, c, d in zip(cur_std_pred_cols, clf_rfr.feature_importances_, clf_lin.coef_, clf_las.coef_):
		#     imp += [[a, b, abs(c), abs(d)]]
		# imp = pd.DataFrame(imp, columns=['col','imp','lin','las']).sort_values('imp', ascending=0)
		# imp['imp_rk'] = imp.imp.rank(ascending=0)
		# imp['lin_rk'] = imp.lin.rank(ascending=0)
		# imp['las_rk'] = imp.las.rank(ascending=0)
		# imp['include'] = 0
		# imp.to_csv('~/Downloads/coef.csv', index=False)
		# imp.head(50).tail(20)
		# imp.head(40).tail(10)
		# imp.head(50).tail(10)
		# nft_rank should be negative
		# adj_nft_rank_0 should be positive
		# adj_nft_rank_1 should be positive
		clf_lin = RidgeCV(alphas=[1.5**x for x in range(1, 20)])
		clf_lin = Ridge(alpha=30, fit_intercept=True)
		clf_lin = Lasso(alpha=.225)
		def get_coefs(cols, coef):
			coefs = []
			for a, b in zip(cols, coef):
				coefs += [[a,b]]
			coefs = pd.DataFrame(coefs, columns=['col','coef']).sort_values('coef', ascending=0)
			# coefs.to_csv('~/Downloads/{}_lin_coefs.csv'.format(collection), index=False)
			coefs['tmp'] = coefs.col.apply(lambda x: 'nft_rank' in x )
			# coefs['mult'] = coefs.col.apply(lambda x: -1 if x == 'std_nft_rank' else 1 )
			coefs['mult'] = coefs.apply(lambda x: -1 if x['col'] == 'std_nft_rank' else 1 if x['coef'] >= 0 else -1 , 1 )
			coefs['val'] = coefs.mult * coefs.coef
			coefs = coefs.sort_values('val', ascending=0)
			return(coefs)

		mn = -1
		print('Starting with {} cols'.format(len(cur_std_pred_cols)))
		while mn < 0 or len(cur_std_pred_cols) > 140:
			X = df[cur_std_pred_cols].values
			clf_lin.fit(X, y_0, df.wt.values)
			coefs = get_coefs(cur_std_pred_cols, clf_lin.coef_)
			tmp = coefs[coefs.tmp == True]
			mn = min(coefs.val) if len(coefs) else 0
			if mn < 0:
				cur_std_pred_cols.remove(coefs.col.values[-1])
			else:
				cur_std_pred_cols.remove(coefs.col.values[-1])
				coefs.to_csv('~/Downloads/{}_lin_coefs.csv'.format(collection), index=False)
				len(coefs[coefs.coef !=0])
		# print(coefs[coefs.coef !=0])
		# print(len(coefs[coefs.coef !=0]))
		INCLUDE_COLS = MODEL_INCLUDE_COLS[collection] if collection in MODEL_INCLUDE_COLS.keys() else []

		# clf_lin = RidgeCV(alphas=[1.5**x for x in range(1, 20)])

		cur_std_pred_cols = list(coefs[coefs.coef !=0].col.unique())
		for c in INCLUDE_COLS:
			if not c in cur_std_pred_cols:
				cur_std_pred_cols.append(c)
		lin_std_pred_cols = cur_std_pred_cols
		X = df[cur_std_pred_cols].values
		# clf_lin = RidgeCV(alphas=[1.5**x for x in range(1, 20)])
		# clf_lin = Lasso(alpha=0.1)
		clf_lin = Lasso(alpha=.1)
		clf_lin.fit(X, y_0, df.wt.values)
		coefs = get_coefs(cur_std_pred_cols, clf_lin.coef_)
		print(coefs[coefs.coef !=0])
		print(len(coefs[coefs.coef !=0]))
		print(coefs[coefs.col.isin(INCLUDE_COLS)])
		coefs[coefs.coef !=0].to_csv('./data/coefs/{}_lin_coefs.csv'.format(collection), index=False)
		df[df['std_Attribute Count_0']!=0]
		df['std_Attribute Count_0'].unique()
		coefs[coefs.col.isin(INCLUDE_COLS)]
		df['pred'] = clf_lin.predict(X)
		df['err'] = df.pred - df.rel_price_0
		df[df['std_Hat_Space Warrior Hair'] == 1][['pred',target_col]].mean()
		df[df['std_Hat_Space Warrior Hair'] == 1].err.median()
		tmp = []
		for c in std_pred_cols:
			if len(df[df[c] == 1]):
				mu = round(df[df[c] == 1].err.mean())
				md = round(df[df[c] == 1].err.median())
				n = len(df[df[c] == 1])
				tmp += [[ c, int(c in cur_std_pred_cols ), n, mu, md ]]
				# print('{}: {}, {}, {}'.format(c, mu, md, n))
		tmp = pd.DataFrame(tmp, columns=['c','i','n','mu','md']).sort_values('mu')
		tmp.to_csv('~/Downloads/tmp4.csv', index=False)
		tmp[tmp.i == 0].head(8)
		tmp[tmp.i == 0].tail(8)
		'std_Hat_Crown','std_Attribute Count_0','std_Hat_Space Warrior Hair','std_Eyes_Laser Eyes','std_Type_Solana',''
		df[df['std_Hat_Space Warrior Hair'] == 1].err.mean()
		df[df['std_Hat_Strawhat'] == 1][['pred','rel_price_0']].mean()

		df['pred_lin'] = clf_lin.predict(X)
		df['pred_lin'] = df.pred_lin.apply(lambda x: max(0, x)) + df.mn_20
		df['err_lin'] = abs(((df.pred_lin - df[target_col]) / df[target_col]) )
		# df[df.genus_Titan==1][['rarity']]
		# df[(df.rarity=='Legendary') | (df.genus=='Titan')][['genus','rarity']]

		# run the log model
		# clf_log = Lasso(1.0) if collection in [ 'Levana Dragon Eggs' ] else RidgeCV(alphas=[1.5**x for x in range(20)])
		clf_log = RidgeCV(alphas=[1.5**x for x in range(1, 20)])
		clf_log = Ridge(alpha=30)
		clf_log = Lasso(0.003)
		# clf_lin = RidgeCV(alphas=[1.5**x for x in range(1, 20)])

		mn = -1
		cur_std_pred_cols = deepcopy(std_pred_cols)
		while mn < 0 or len(cur_std_pred_cols) > 140:
			X = df[cur_std_pred_cols].values
			clf_log.fit(X, y_1, df.wt.values)
			coefs = get_coefs(cur_std_pred_cols, clf_log.coef_)
			tmp = coefs[coefs.tmp == True]
			mn = min(tmp.coef) if len(tmp) else 0
			if mn < 0:
				cur_std_pred_cols.remove(tmp.col.values[-1])
			else:
				cur_std_pred_cols.remove(coefs.col.values[-1])
		coefs = get_coefs(cur_std_pred_cols, clf_log.coef_)
		coefs[coefs.coef !=0].to_csv('./data/coefs/{}_log_coefs.csv'.format(collection), index=False)
		# print(coefs[coefs.coef !=0])
		len(coefs[coefs.coef !=0])
		# cur_std_pred_cols = list(coefs[coefs.coef !=0].col.unique())
		for c in INCLUDE_COLS:
			if not c in cur_std_pred_cols:
				cur_std_pred_cols.append(c)
		log_std_pred_cols = cur_std_pred_cols
		X = df[cur_std_pred_cols].values
		clf_log = Lasso(0.001)
		clf_log.fit(X, y_1, df.wt.values)
		coefs = get_coefs(cur_std_pred_cols, clf_log.coef_)
		print(coefs[coefs.coef !=0])
		print(len(coefs[coefs.coef !=0]))
		print(coefs[coefs.col.isin(INCLUDE_COLS)])
		# clf_log.fit(X, y_1, df.wt.values)
		# if collection == 'Levana Dragon Eggs':
		#     coefs = []
		#     for a, b in zip(std_pred_cols, clf_lin.coef_):
		#         coefs += [[a,b]]
		#     coefs = pd.DataFrame(coefs, columns=['col','coef']).sort_values('coef', ascending=0)
		#     coefs.to_csv('~/Downloads/levana_log_coefs.csv', index=False)
		df['pred_log'] = clf_log.predict(X)
		df['pred_log'] = df.pred_log.apply(lambda x: max(1, x)) * df.mn_20
		df['err_log'] = abs(((df.pred_log - df[target_col]) / df[target_col]) )
		df[[ target_col,'pred_log','err_log','mn_20' ]].sort_values('err_log').tail(50)
		df['err'] = df.err_lin * df.err_log


		# combine the models
		clf = LinearRegression(fit_intercept=False)
		clf.fit( df[['pred_lin','pred_log']].values, df[target_col].values, df.wt.values )
		df[['pred_lin','pred_log',target_col]].mean()
		print('Price = {} * lin + {} * log'.format( round(clf.coef_[0], 2), round(clf.coef_[1], 2) ))
		l = df.sort_values('block_timestamp', ascending=0).mn_20.values[0]
		tmp = pd.DataFrame([[collection, clf.coef_[0], clf.coef_[1], l]], columns=['collection','lin_coef','log_coef','floor_price'])
		if clf.coef_[0] < 0:
			print('Only using log')
			df['pred'] = df.pred_log
			tmp['lin_coef'] = 0
			tmp['log_coef'] = 1
		elif clf.coef_[1] < 0:
			print('Only using lin')
			df['pred'] = df.pred_lin
			tmp['lin_coef'] = 1
			tmp['log_coef'] = 0
		else:
			print('Only using BOTH!')
			df['pred'] = clf.predict( df[['pred_lin','pred_log']].values )
		coefsdf = coefsdf.append(tmp)
		df['err'] = (df.pred / df[target_col]).apply(lambda x: abs(x-1) )

		# print out some summary stats
		df['err'] = df[target_col] - df.pred
		df['q'] = (df.pred.rank() ** 1.5 * .2) / len(df)
		df['q'] = df.q.apply(lambda x: int(round(x)) )
		df['pct_err'] = (df[target_col] / df.pred) - 1
		pe_mu = df.pct_err.mean()
		pe_sd = df[ (df.pct_err > -.9) & (df.pct_err < 0.9) ].pct_err.std()
		pe_sd = df[ (df.pct_err > -.9) & (df.pct_err < 0.9) & (df.days_ago<=50) ].pct_err.std()
		df['pred_price'] = df.pred#.apply(lambda x: x*(1+pe_mu) )
		df['pred_sd'] = df.pred * pe_sd
		# print(df.groupby('q')[['err','pred',target_col]].mean())
		print(df[df.wt >= df.wt.median()].groupby('q')[['err','pred',target_col]].mean())
		print(df.groupby('q')[['err','pred',target_col]].mean())
		# df.err.mean()
		# df[df.weight >= 3.5].err.mean()
		df[df.pred < 200].err.mean()
		df['collection'] = collection
		print('Avg err last 100: {}'.format(round(df.sort_values('block_timestamp').head(100).err.mean(), 2)))
		salesdf = salesdf.append( df.merge(s_df[s_df.sim == 0][['collection','token_id','block_timestamp','price']] )[[ 'collection','token_id','block_timestamp','price','pred','mn_20','nft_rank' ]].sort_values('block_timestamp', ascending=0) )


		############################################################
		#     Create Predictions for Each NFT in The Collection    #
		############################################################
		test = merge(num_metadata, cat_metadata, ['collection','token_id'])
		for c in num_features:
			test[c] = test[c].apply(lambda x: just_float(x) )
		tail = df.sort_values('timestamp').tail(1)
		test.loc[ test.token_id == '903', 'nft_rank' ] = 18
		test[test.token_id=='903']
		for c in [ 'std_timestamp','mn_20','log_mn_20' ]:
			if c in tail.columns:
				test[c] = tail[c].values[0]
		test = standardize_df(test, pred_cols, df)

		test['pred_lin'] = clf_lin.predict(test[lin_std_pred_cols].values)
		test['pred_lin'] = test.pred_lin.apply(lambda x: max(0, x) + l)
		test['pred_log'] = clf_log.predict(test[log_std_pred_cols].values)
		test['pred_log'] = test.pred_log.apply(lambda x: max(1, x)) * l

		test['pred_price'] = clf.predict( test[[ 'pred_lin','pred_log' ]].values )
		if not check_exclude:
			test['pred_price'] = test.pred_price.apply(lambda x: (x*0.985) )
		test['pred_sd'] = test.pred_price * pe_sd
		test = test.sort_values(['collection','token_id'])
		test['rk'] = test.pred_price.rank(ascending=0, method='first')
		test['collection'] = collection
		pred_price = pred_price.append( test[[ 'collection','token_id','nft_rank','rk','pred_price','pred_sd' ]].sort_values('pred_price') )

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

	attributes['feature_name'] = attributes.feature_name.apply(lambda x: re.sub('_', ' ', x).title() )
	sorted(attributes['feature_name'].unique())
	if len(feature_values):
		feature_values['feature_name'] = feature_values.feature_name.apply(lambda x: re.sub('_', ' ', x).title() )

	coefsdf.to_csv('./data/coefsdf.csv', index=False)
	salesdf.to_csv('./data/model_sales.csv', index=False)
	old = pd.read_csv('./data/pred_price copy.csv')
	old['token_id'] = old.token_id.astype(str)
	old = pred_price.merge(old, on=['collection','token_id'])
	old['ratio'] = old.pred_price_x / old.pred_price_y
	old = old.sort_values('ratio')
	old.columns = [ 'collection', 'token_id', 'nft_rank', 'rk_new', 'pred_price_new', 'pred_sd_x', 'rank', 'rk_old', 'pred_price_old', 'pred_sd_y', 'clean_token_id', 'ratio' ]
	m = m_df[(m_df.collection.isin(pred_price.collection.unique())) & (-(m_df.feature_name.isin(['nft_rank','adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2'])))]
	m_p = m.pivot(['collection','token_id'], ['feature_name'], ['feature_value']).reset_index()
	m_p.columns = [ 'collection','token_id' ] + sorted(m.feature_name.unique())
	m_p.head()
	old = old.merge(m_p, on=['collection','token_id'])
	old = old[[ 'token_id', 'nft_rank', 'rk_old', 'rk_new', 'pred_price_old', 'pred_price_new', 'ratio' ] + [c for c in m_p.columns if not c in ['token_id','collection']]]
	old.to_csv('~/Downloads/tmp1.csv', index=False)
	pred_price.head()
	old[old.token_id == '4857']
	old.head()
	old.tail()

	pred_price.to_csv('./data/pred_price.csv', index=False)
	attributes.to_csv('./data/attributes.csv', index=False)
	attributes[attributes.rarity.isnull()]
	feature_values.to_csv('./data/feature_values.csv', index=False)

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


	feature_values.to_csv('./data/feature_values.csv', index=False)

	if check_exclude:
		salesdf['rat'] = salesdf.price / salesdf.pred
		salesdf['dff'] = salesdf.price - salesdf.pred
		salesdf['exclude_1'] = (((salesdf.dff >= 20) & (salesdf.rat > 4)) | ((salesdf.dff >= 40) & (salesdf.rat > 3)) | ((salesdf.dff >= 60) & (salesdf.rat > 2)) | ((salesdf.dff >= 80) & (salesdf.rat > 2))).astype(int)
		salesdf['rat'] = salesdf.pred / salesdf.price
		salesdf['dff'] = salesdf.pred - salesdf.price
		salesdf['exclude_2'] = (((salesdf.dff >= 20) & (salesdf.rat > 4)) | ((salesdf.dff >= 40) & (salesdf.rat > 3)) | ((salesdf.dff >= 60) & (salesdf.rat > 2)) | ((salesdf.dff >= 80) & (salesdf.rat > 2))).astype(int)
		salesdf['exclude'] = (salesdf.exclude_1 + salesdf.exclude_2).apply(lambda x: int(x>0))
		print(salesdf.exclude_1.mean())
		print(salesdf.exclude_2.mean())
		print(salesdf.exclude.mean())
		salesdf[salesdf.token_id == '2239'][['collection','price','exclude']]
		salesdf[salesdf.exclude == 1][[ 'collection','token_id','price','exclude' ]].to_csv('./data/exclude.csv', index=False)

train_model(True, False)
train_model(False, True)