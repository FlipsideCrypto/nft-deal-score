import os
import re
import json
import warnings
import requests
import numpy as np
import pandas as pd
import urllib.request
import tensorflow as tf
import snowflake.connector
from datetime import datetime
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, RidgeCV, Lasso
from sklearn.model_selection import train_test_split, KFold, GridSearchCV, RandomizedSearchCV

warnings.filterwarnings('ignore')

os.chdir('/Users/kellenblumberg/git/nft-deal-score')

CHECK_EXCLUDE = False
# CHECK_EXCLUDE = True

# Using sales from howrare.is - the last sale that was under 300 was when the floor was at 72. Filtering for when the floor is >100, the lowest sale was 400

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
            df['std_{}'.format(c)] = df[c].apply(lambda x: (x*2) - 1 )
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

exclude = [
    # (collection, token_id, price)
    ( 'aurory', 2239, 3500 )
    # ( 'aurory', 856, 150 )
    # ( 'aurory', 4715, 500 )
    # ( 'aurory', 5561, 298 )
    # ( 'aurory', 5900, 199 )
    # ( 'aurory', 3323, 138 )
]
s_df = pd.read_csv('./data/sales.csv').rename(columns={'sale_date':'block_timestamp'})
s_df = s_df[-s_df.collection.isin(['Levana Meteors','Levana Dust'])]
s_df = s_df[ -s_df.collection.isin(['boryokudragonz', 'Boryoku Dragonz']) ]
s_df = s_df[[ 'chain','collection','block_timestamp','token_id','price','tx_id' ]]
for e in exclude:
    s_df = s_df[-( (s_df.collection == e[0]) & (s_df.token_id == e[1]) & (s_df.price == e[2]) )]
s_df = s_df[ -((s_df.collection == 'smb') & (s_df.price < 1)) ]

# exclude wierd data points
if not CHECK_EXCLUDE:
    exclude = pd.read_csv('./data/exclude.csv')
    s_df = s_df.merge(exclude, how='left')
    s_df = s_df[s_df.exclude.isnull()]
    del s_df['exclude']

#########################
#     Load Metadata     #
#########################
m_df = pd.read_csv('./data/metadata.csv')
m_df['token_id'] = m_df.token_id.astype(str)
# remove ones that are not actually metadata
m_df = m_df[ -m_df.feature_name.isin([ 'price','last_sale','feature_name','feature_value' ]) ]
m_df['feature_value'] = m_df.feature_value.apply(lambda x: re.split("\(", re.sub("\"", "", x))[0] if type(x)==str else x )
m_df[(m_df.feature_name=='rank') & (m_df.collection == 'Levana Dragon Eggs')]


#####################################
#     Exclude Special LunaBulls     #
#####################################
tokens = pd.read_csv('./data/tokens.csv')
tokens.token_id.unique()
lunabullsrem = tokens[tokens.clean_token_id>=10000].token_id.unique()
m_df = m_df[ -((m_df.collection == 'LunaBulls') & (m_df.token_id.isin(lunabullsrem))) ]
s_df = s_df[ -((s_df.collection == 'LunaBulls') & (s_df.token_id.isin(lunabullsrem))) ]


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
MODEL_EXCLUDE_COLS = {
    # 'Levana Dragon Eggs': ['collection_rank','meteor_id','shower','lucky_number','cracking_date','attribute_count','weight','temperature']
    'Levana Dragon Eggs': ['meteor_id','shower','lucky_number','cracking_date','attribute_count']
}
RARITY_EXCLUDE_COLS = {
    # 'Levana Dragon Eggs': ['collection_rank','meteor_id','shower','lucky_number','cracking_date','attribute_count','weight','temperature']
    'Levana Dragon Eggs': ['meteor_id','attribute_count','collection_rank','transformed_collection_rank','rarity_score']
}
NUMERIC_COLS = {
    'Levana Dragon Eggs': ['collection_rank','weight','temperature','transformed_collection_rank','rarity_score']
}
ATT_EXCLUDE_COLS = {
    'Levana Dragon Eggs': ['attribute_count','transformed_collection_rank']
}
# for collection in [ 'Levana Dragon Eggs' ]:
for collection in s_df.collection.unique():
    print('Working on collection {}'.format(collection))
    sales = s_df[ s_df.collection == collection ]
    metadata = m_df[ m_df.collection == collection ]
    metadata[metadata.token_id == '1']
    metadata[metadata.feature_name == 'rank']
    metadata.feature_name.unique()

    # categorize columns
    all_names = sorted(metadata.feature_name.unique())
    model_exclude = MODEL_EXCLUDE_COLS[collection] if collection in MODEL_EXCLUDE_COLS.keys() else []
    num_features = sorted((NUMERIC_COLS[collection] if collection in NUMERIC_COLS.keys() else []) + ALL_NUMERIC_COLS)
    num_features = [ x for x in num_features if x in metadata.feature_name.unique() ]
    num_metadata = metadata[metadata.feature_name.isin(num_features)]
    num_metadata[num_metadata.feature_name == 'rank']
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
    cat_metadata = pd.concat([ cat_metadata.reset_index(drop=True), dummies.reset_index(drop=True) ], axis=1)
    del cat_metadata['pct']

    pred_cols = num_features + list(dummies.columns)

    # create training df
    df = merge(sales, num_metadata, ['collection','token_id'], ensure=False)
    df = merge(df, cat_metadata, ['collection','token_id'])
    for c in num_features:
        df[c] = df[c].apply(lambda x: just_float(x))

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
    # if collection == 'Levana Dragon Eggs':
    #     std_pred_cols = [ 'std_genus_Titan','std_score','std_weight','std_transformed_collection_rank','std_collection_rank','std_legendary_composition_None','std_ancient_composition_None' ]
    mn = df.timestamp.min()
    mx = df.timestamp.max()
    df['wt'] = df.timestamp.apply(lambda x: 2.5 ** ((x - mn) / (mx - mn)) )
    X = df[std_pred_cols].values
    y_0 = df.rel_price_0.values
    y_1 = df.rel_price_1.values


    # run the linear model
    clf_lin = Lasso() if collection in [ 'Levana Dragon Eggs' ] else RidgeCV(alphas=[1.5**x for x in range(20)])
    # clf_lin = RidgeCV(alphas=[1.5**x for x in range(20)])
    clf_lin.fit(X, y_0, df.wt.values)
    if collection == 'Levana Dragon Eggs':
        coefs = []
        for a, b in zip(std_pred_cols, clf_lin.coef_):
            coefs += [[a,b]]
        coefs = pd.DataFrame(coefs, columns=['col','coef']).sort_values('coef', ascending=0)
        coefs.to_csv('~/Downloads/levana_lin_coefs.csv', index=False)
    df['pred_lin'] = clf_lin.predict(X)
    df['pred_lin'] = df.pred_lin.apply(lambda x: max(0, x)) + df.mn_20
    df['err_lin'] = abs(((df.pred_lin - df[target_col]) / df[target_col]) )

    # run the log model
    clf_log = Lasso() if collection in [ 'Levana Dragon Eggs' ] else RidgeCV(alphas=[1.5**x for x in range(20)])
    # clf_log = RidgeCV(alphas=[1.5**x for x in range(20)])
    clf_log.fit(X, y_1, df.wt.values)
    if collection == 'Levana Dragon Eggs':
        coefs = []
        for a, b in zip(std_pred_cols, clf_lin.coef_):
            coefs += [[a,b]]
        coefs = pd.DataFrame(coefs, columns=['col','coef']).sort_values('coef', ascending=0)
        coefs.to_csv('~/Downloads/levana_log_coefs.csv', index=False)
    df['pred_log'] = clf_log.predict(X)
    df['pred_log'] = df.pred_log.apply(lambda x: max(1, x)) * df.mn_20
    df['err_log'] = abs(((df.pred_log - df[target_col]) / df[target_col]) )
    df[[ target_col,'pred_log','err_log','mn_20' ]].sort_values('err_log').tail(50)
    df['err'] = df.err_lin * df.err_log


    # combine the models
    clf = LinearRegression(fit_intercept=False)
    clf.fit( df[['pred_lin','pred_log']].values, df[target_col].values, df.wt.values )
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
    df['q'] = df.pred.rank() * 10 / len(df)
    df['q'] = df.q.apply(lambda x: int(round(x)) )
    df['pct_err'] = (df[target_col] / df.pred) - 1
    pe_mu = df.pct_err.mean()
    pe_sd = df[ (df.pct_err > -.9) & (df.pct_err < 0.9) ].pct_err.std()
    pe_sd = df[ (df.pct_err > -.9) & (df.pct_err < 0.9) & (df.days_ago<=50) ].pct_err.std()
    df['pred_price'] = df.pred#.apply(lambda x: x*(1+pe_mu) )
    df['pred_sd'] = df.pred * pe_sd
    print(df.groupby('q')[['err','pred',target_col]].mean())
    print(df[df.wt >= df.wt.median()].groupby('q')[['err','pred',target_col]].mean())
    # df.err.mean()
    # df[df.weight >= 3.5].err.mean()
    df['collection'] = collection
    print('Avg err last 100: {}'.format(round(df.sort_values('block_timestamp').head(100).err.mean(), 2)))
    salesdf = salesdf.append( df[[ 'collection','token_id','block_timestamp','price','pred','mn_20','rank' ]].sort_values('block_timestamp', ascending=0) )


    ############################################################
    #     Create Predictions for Each NFT in The Collection    #
    ############################################################
    test = merge(num_metadata, cat_metadata, ['collection','token_id'])
    for c in num_features:
        test[c] = test[c].apply(lambda x: just_float(x) )
    tail = df.sort_values('timestamp').tail(1)
    for c in [ 'std_timestamp','mn_20','log_mn_20' ]:
        if c in tail.columns:
            test[c] = tail[c].values[0]
    test = standardize_df(test, pred_cols, df)

    test['pred_lin'] = clf_lin.predict(test[std_pred_cols].values)
    test['pred_lin'] = test.pred_lin.apply(lambda x: max(0, x) + l)
    test['pred_log'] = clf_log.predict(test[std_pred_cols].values)
    test['pred_log'] = test.pred_log.apply(lambda x: max(1, x)) * l

    test['pred_price'] = clf.predict( test[[ 'pred_lin','pred_log' ]].values )
    if not CHECK_EXCLUDE:
        test['pred_price'] = test.pred_price.apply(lambda x: (x*0.985) )
    test['pred_sd'] = test.pred_price * pe_sd
    test['rk'] = test.pred_price.rank(ascending=0, method='first')
    test['collection'] = collection
    pred_price = pred_price.append( test[[ 'collection','token_id','rank','rk','pred_price','pred_sd' ]].sort_values('pred_price') )

    cols = metadata.feature_name.unique()
    cols = [ x for x in cols if not x in (ATT_EXCLUDE_COLS[collection] if collection in ATT_EXCLUDE_COLS.keys() else []) + ALL_NUMERIC_COLS ]
    exclude = RARITY_EXCLUDE_COLS[collection] if collection in RARITY_EXCLUDE_COLS.keys() else []
    for c in cols:
        cur = metadata[metadata.feature_name == c][['collection','token_id','feature_name','feature_value']]
        if c in exclude:
            cur['rarity'] = None
        else:
            g = cur.groupby('feature_value').token_id.count().reset_index()
            g['rarity'] = g.token_id / len(cur.token_id.unique())
            cur = merge(cur, g[['feature_value','rarity']])
        attributes = attributes.append(cur)

attributes['feature_name'] = attributes.feature_name.apply(lambda x: re.sub('_', ' ', x).title() )
sorted(attributes['feature_name'].unique())
if len(feature_values):
    feature_values['feature_name'] = feature_values.feature_name.apply(lambda x: re.sub('_', ' ', x).title() )

coefsdf.to_csv('./data/coefsdf.csv', index=False)
salesdf.to_csv('./data/model_sales.csv', index=False)
pred_price.to_csv('./data/pred_price.csv', index=False)
attributes.to_csv('./data/attributes.csv', index=False)
feature_values.to_csv('./data/feature_values.csv', index=False)

if CHECK_EXCLUDE:
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
