import collections
from copy import deepcopy
import re
import os
import json
import math
# from statistics import LinearRegression
import requests
import datetime
import pandas as pd
import urllib.request
import snowflake.connector
from bs4 import BeautifulSoup
from time import sleep
from sklearn.linear_model import LinearRegression, RidgeCV, Lasso, Ridge
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier


os.chdir('/Users/kellenblumberg/git/nft-deal-score')

from solana_model import get_coefs, just_float, standardize_df
from utils import clean_name, clean_token_id, format_num, merge
from load_data import clean_colnames
import kutils as ku

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

INCLUDE = [ 'okay bears','degods','shadowy super coder dao','cets on creck','degen apes','portals','catalina whale mixer','blocksmith labs','stoned ape crew','tombstoned high society','astrals','space runners','taiyo robotics','aurory','famous fox federation','quantum traders','baby ape social club','solgods','dazedducks metagalactic club','the suites','monkelabs','defi pirates','lux real estate','og atadians','bohemia','monkeyleague gen zero','monkey kingdom','galactic geckos','nuked apes','degen dojo','grim syndicate','best buds','degen trash pandas','thugbirdz','lifinity flares','balloonsville 2.0','mindfolk','pesky penguins','solsteads surreal estate','zaysan raptors','boryoku dragonz','turtles','nyan heroes','meerkat millionaires country club' ]

query = '''
	WITH mints AS (
		SELECT DISTINCT LOWER(project_name) AS collection
		, mint
		, token_id
		FROM solana.dim_nft_metadata
		WHERE token_id IS NOT NULL
	), base AS (
	SELECT m.collection
	, m.token_id
	, s.tx_id
	, s.block_timestamp
	, s.mint
	, s.sales_amount AS price
	FROM solana.fact_nft_sales s
	JOIN mints m ON LOWER(m.mint) = LOWER(s.mint)
	WHERE succeeded = TRUE
	)
	SELECT *
	FROM base
'''
sales = ctx.cursor().execute(query)
sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
sales = clean_colnames(sales)
len(sales)

sales = sales.sort_values(['collection','block_timestamp'])
sales['mn_20'] = sales.groupby('collection').price.shift(1)
sales = sales.sort_values(['collection','block_timestamp'])
sales['md_20'] = sales.groupby('collection')['mn_20'].rolling(20).quantile(.01).reset_index(0,drop=True)

# exclude sales that are far below the existing floor
sales['tmp'] = (sales.price >= sales.md_20 * 0.80).astype(int)
sales[sales.tmp == 0]
sales.tmp.mean()
sales = sales[ (sales.price) >= (sales.md_20 * 0.80) ]

# 10%ile of last 20 sales
sales = sales.sort_values(['collection','block_timestamp'])
sales['mn_20'] = sales.groupby('collection').price.shift(1)
sales = sales.sort_values(['collection','block_timestamp'])

# sales['mn_20'] = sales.groupby('collection')['mn_20'].rolling(20).quantile(.0525).reset_index(0,drop=True)
sales['mn_20'] = sales.groupby('collection')['mn_20'].rolling(20).quantile(.0525).reset_index(0,drop=True)

pred_price = pd.read_csv('./data/pred_price.csv')
coefs = pd.read_csv('./data/coefsdf.csv')

sales['token_id'] = sales.token_id.astype(int)
pred_price['token_id'] = pred_price.token_id.astype(int)
sales['collection'] = sales.collection.apply(lambda x: clean_name(x))

sales = sales[-sales.collection.isin(['Astrals','Baby Ape Social Club','Lightning Og','Taiyo Robotics'])]
sorted(sales.collection.unique())

df = merge(sales, pred_price, ensure=True, on=['collection','token_id'])
df = merge(df, coefs, ensure=True, on=['collection'])
df['fair_market_price']
df['abs_chg'] = (df.mn_20 - df.floor_price) * df.lin_coef
df['pct_chg'] = (df.mn_20 - df.floor_price) * df.log_coef
df['fair_market_price'] = df.pred_price + df.abs_chg + (df.pct_chg * df.pred_price / df.floor_price)
df['vs_fair_market_price'] = df.price - df.fair_market_price
df['date'] = df.block_timestamp.apply(lambda x: ku.tt(str(x)[:10]) )
df['n_sales'] = 1

a = df.groupby(['collection','date']).tx_id.count().reset_index()
b = df.groupby(['collection','date'])[['tx_id']].count().reset_index()


query = '''
	WITH mints AS (
		SELECT DISTINCT LOWER(project_name) AS project_name
		, address AS mint
		FROM crosschain.address_labels
		WHERE blockchain = 'solana'
		AND label_subtype = 'nf_token_contract'
		UNION 
		SELECT DISTINCT LOWER(project_name) AS project_name
		, mint
		FROM solana.dim_nft_metadata
	), rem AS (
		SELECT pre_token_balances[0]:mint::string AS mint
		, m.project_name
		, t.tx_id AS remove_tx
		, t.block_timestamp AS remove_time
		, CASE WHEN s.tx_id IS NULL THEN 0 ELSE 1 END AS is_sale
		, s.sales_amount
		, ROW_NUMBER() OVER (PARTITION BY m.mint ORDER BY t.block_timestamp DESC) AS rn
		FROM solana.fact_transactions t
		JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
		LEFT JOIN solana.fact_nft_sales s ON s.tx_id = t.tx_id
		WHERE t.block_timestamp >= '2021-12-01'
		AND (
			(
				LEFT(instructions[0]:data::string, 4) IN ('ENwH','3GyW')
				AND instructions[0]:programId = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
			)
			OR 
			(
				LEFT(instructions[0]:data::string, 4) IN ('TE6a','3UjL')
				AND instructions[0]:programId = 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8'
			)
		)
		AND t.succeeded = TRUE
	), add AS (
		SELECT pre_token_balances[0]:mint::string AS mint
		, m.project_name
		, t.tx_id AS listing_tx
		, block_timestamp AS listing_time
		, ROW_NUMBER() OVER (PARTITION BY mint ORDER BY block_timestamp DESC) AS rn
		FROM solana.fact_transactions t
		JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
		WHERE t.block_timestamp >= '2021-12-01'
		AND LEFT(instructions[0]:data::string, 4) IN ('2B3v','2RD3')
		AND instructions[0]:programId IN ('M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K','MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8')
		AND t.succeeded = TRUE
	), base AS (
		SELECT a.*
		, r.remove_tx
		, r.remove_time
		, date_trunc('day', a.listing_time)::date AS listing_day
		, date_trunc('day', r.remove_time)::date AS remove_day
		, r.is_sale
		, r.sales_amount
		, CASE WHEN r.remove_time IS NULL OR a.listing_time > r.remove_time THEN 1 ELSE 0 END AS is_listed
		, ROW_NUMBER() OVER (PARTITION BY a.listing_tx ORDER BY r.remove_time ASC) AS rn2
		FROM add a
		LEFT JOIN rem r ON r.mint = a.mint AND a.listing_time <= r.remove_time
	), b2 AS (
		SELECT *
		, ROW_NUMBER() OVER (PARTITION BY remove_tx ORDER BY listing_time DESC) AS rn3
		FROM base 
		WHERE rn2 = 1
	), b3 AS (
		SELECT *
		FROM b2
		WHERE rn3 = 1
	), days AS (
		SELECT DISTINCT listing_day AS day
		FROM b3
		UNION
		SELECT DISTINCT remove_day AS day
		FROM b3
	), filter AS (
		SELECT project_name
		, SUM(sales_amount) AS amt
		FROM base
		WHERE remove_day >= '2021-12-01'
		AND sales_amount > 0
		GROUP BY 1
		ORDER BY 2 DESC
		LIMIT 200
	)
	SELECT h.day AS date
	, b.project_name AS collection
	, MIN(sales_amount) AS floor
	FROM days h
	JOIN b3 b ON b.listing_day <= h.day AND COALESCE(b.remove_day, h.day) >= h.day
	JOIN filter f ON f.project_name = b.project_name
	WHERE COALESCE(b.sales_amount, 0) > 0
	GROUP BY 1, 2
	ORDER BY 1
'''
floors = ctx.cursor().execute(query)
floors = pd.DataFrame.from_records(iter(floors), columns=[x[0] for x in floors.description])
floors = clean_colnames(floors)
sorted(floors.date.unique())
floors.to_csv('./data/floors.csv', index=False)
floors[floors.collection == '3d flowers'].sort_values('date').head(22)
floors.date.unique()[:11]
floors = pd.read_csv('./data/floors.csv')
floors['date'] = floors.date.apply(lambda x: datetime.datetime.strptime(str(x)[:10], '%Y-%m-%d').date() )
# floors = floors.groupby(['date','collection']).floor.min().reset_index()
# tmp = floors.groupby(['date','collection']).floor.count().reset_index().sort_values('floor', ascending=0)
# floors.merge(tmp[['collection','date']]).sort_values(['collection','date'])
# rem = tmp[tmp.floor > 1].collection.unique()
# floors = floors[-floors.collection.isin(rem)]
# len(floors.collection.unique())
# tmp.head()

def grp_f(x):
	cutoffs = [ 
		-0.5
		, -0.25
		, -0.15
		, -0.05
		, 0.05
		, 0.15
		, 0.25
		, 0.50
	]
	i = 0
	for i in range(len(cutoffs)):
		if x < cutoffs[i]:
			return('grp_{}'.format(i))
	return('grp_{}'.format(len(cutoffs)))

for i in [1, 3, 7, 14, 30]:
	print(i)
	for c in [ 'n_{}_floor'.format(i), 'p_{}_floor'.format(i), 'n_{}_chg'.format(i), 'p_{}_chg'.format(i) ]:
		if c in floors.columns:
			del floors[c]
	cur = floors[['date','collection','floor']].copy()
	cur['date'] = cur.date.apply(lambda x: x - datetime.timedelta(days=i) )
	cur = cur.rename(columns={'floor':'n_{}_floor'.format(i)})
	floors = merge(floors, cur, how='left', ensure=True, on=['collection','date'])
	floors['n_{}_chg'.format(i)] = (floors['floor'] / floors['n_{}_floor'.format(i)]) - 1
	floors['n_{}_grp'.format(i)] = floors['n_{}_chg'.format(i)].apply(lambda x: grp_f(x) )

	cur = floors[['date','collection','floor']].copy()
	cur['date'] = cur.date.apply(lambda x: x + datetime.timedelta(days=i) )
	cur = cur.rename(columns={'floor':'p_{}_floor'.format(i)})
	floors = merge(floors, cur, how='left', ensure=True, on=['collection','date'])
	floors['p_{}_chg'.format(i)] = (floors['floor'] / floors['p_{}_floor'.format(i)]) - 1
	floors['p_{}_chg_abs'.format(i)] = abs(floors['p_{}_chg'.format(i)])
	floors = floors.sort_values('date')
for i in [ 1, 3, 7, 30 ]:
	floors['p_chg_avg_{}'.format(i)] = floors.groupby('collection').p_1_chg_abs.rolling(i).mean().reset_index(0,drop=True)


floors[floors.collection == 'solana monkey business'].sort_values('date').head(35)[['date','floor','p_1_floor','p_1_chg_abs','p_chg_avg_1','p_chg_avg_3','p_chg_avg_7']]
floors[floors.collection == 'degen apes'].sort_values('date').head(35)[['date','floor','p_1_floor','p_1_chg_abs','p_chg_avg_1','p_chg_avg_3','p_chg_avg_7']]

sorted(floors.collection.unique())

floors.groupby('n_7_grp').date.count()
floors.groupby('n_7_grp').n_7_chg.min()

pred_cols = ['p_1_chg','p_7_chg','p_30_chg']
target_col = 'n_7_grp'
train = floors[[ 'p_1_chg','p_7_chg','p_30_chg','n_7_chg','n_7_grp' ]].dropna().reset_index(drop=True)
# y = train.get_dummies
dummies = pd.get_dummies(train[target_col])
target_cols = dummies.columns
train = pd.concat([ train.reset_index(drop=True), dummies.reset_index(drop=True) ], axis=1)

X = train[pred_cols].values
y = train[target_cols].values

clf = RandomForestClassifier()
clf.fit(X, y)

clf.feature_importances_

model = 'rfc'
col = 'pred'
params = []
wt_col = None
df, bst_p, bst_r = ku.get_bst_params( model, train, X, y, target_col, col, verbose = True, wt_col=wt_col, params = params )



floors[floors.n_14_chg.notnull()].sort_values('n_14_chg')
len(train[train.n_1_chg < -.5])
len(floors[floors.n_1_chg < -.5])

len(floors[floors.n_7_chg < -.5])
len(floors[floors.n_7_chg < -.3])
len(floors[floors.n_7_chg < -.2])
len(floors[floors.n_7_chg < -.15])
len(floors[floors.n_7_chg < -.1])
len(floors[floors.n_7_chg < -.05])

len(floors[floors.n_3_chg < -.5])
len(train)
train.to_csv('~/Down')
clf = LinearRegression()
clf = RandomForestClassifier()
clf.fit( train[['p_1_chg']].values, train.n_1_chg.values )


clf.fit()
floors[floors.collection == 'solana monkey business'].sort_values('date')

# days since creation
mn = floors.groupby('collection').date.min().reset_index().rename(columns={'date':'mn_date'})
df = merge(floors, mn, how='left', ensure=True)
df['days_since_creation'] = (df.date - df.mn_date).apply(lambda x: x.days)
df['target'] = (df.n_3_chg < -0.25).astype(int)
df[df.target.isnull()]

clf = Lasso()
clf = LinearRegression()
clf = RidgeCV(alphas=[0.1, 1, 3, 10])
pred_cols = [ 'days_since_creation' ]+[ x for x in df.columns if x[:2]=='p_' and x[-4:]!='_abs' and x[-6:]!='_floor' ]
std_pred_cols = [ 'std_'+c for c in pred_cols ]
train = df[ (df.date >= ku.tt('2022-01-01')) & (df.n_3_chg.notnull()) ][['date','collection']+pred_cols+['target']].dropna().reset_index(drop=True)
print('Training on {} data points'.format(len(train)))

clf = LinearRegression()
df[df.collection.isin(INCLUDE)].sort_values(['collection','date']).to_csv('~/Downloads/tmp.csv', index=False)
train = df[df.collection.isin(INCLUDE)][['p_1_chg','n_1_chg']].dropna()
clf.fit( train[['p_1_chg']].values, train.n_1_chg.values )
clf.coef_


train = standardize_df(train, pred_cols)
X = train[std_pred_cols].values
y = train.target.values
clf.fit(X, y)
coefs = []
for a, b in zip(pred_cols, clf.coef_):
	coefs += [[ a, b ]]
coefs = pd.DataFrame(coefs, columns=['col','coef']).sort_values('coef')
coefs

model = 'ridge'
cur_std_pred_cols = deepcopy(std_pred_cols)
print(model)
# y = y_val_rar_adj if model in ['rfr'] else y_val
col = 'y_pred_{}'.format(model)
target_col = 'target'
# params = [saved_params[collection][col]] if col in saved_params[collection].keys() and use_saved_params else []
params = []
train, bst_p, bst_r = ku.get_bst_params( model, train, X, y, target_col, col, verbose = True, wt_col=None, params = params )
# saved_params[collection][col] = bst_p



def get_coefs(cols, coef):
	coefs = []
	for a, b in zip(cols, coef):
		coefs += [[a,b]]
	coefs = pd.DataFrame(coefs, columns=['col','coef']).sort_values('coef', ascending=0)
	# coefs.to_csv('~/Downloads/{}_lin_coefs.csv'.format(collection), index=False)
	# coefs['tmp'] = coefs.col.apply(lambda x: 'nft_rank' in x )
	# coefs['mult'] = coefs.col.apply(lambda x: -1 if x == 'std_nft_rank' else 1 )
	def f(x):
		if x['col'] in ['days_since_creation']:
			return(-1)
		if '_chg' in x['col']:
			return(-1)
		return(1)
	coefs['mult'] = coefs.apply(lambda x: f(x), 1 )
	coefs['val'] = coefs.mult * coefs.coef
	coefs = coefs.sort_values('val', ascending=0)
	return(coefs)

clf = ku.get_model(model, bst_p)
clf.fit(X, y)
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



# average purchase price of holders
# % are minters
# % of collection held <= 30d, 14d, 7d, 3d, 1d
# average age of holder (capped at 50)
# number of premium sales recently
# number of current listings within 30% of floor
# recent listings within 20% of the floor
# of sales in the last x days
# sales volume in the last x days
# recent sales vs deal score prediction


# get daily floors
# get avg variance over past 1d, 3d, 7d, 14d, 30d
# get variance over past 1d, 3d, 7d, 14d, 30d