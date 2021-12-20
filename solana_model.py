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
from sklearn.linear_model import LinearRegression, RidgeCV
from sklearn.model_selection import train_test_split, KFold, GridSearchCV, RandomizedSearchCV

warnings.filterwarnings('ignore')

os.chdir('/Users/kellenblumberg/git/nft-deal-score')

CHECK_EXCLUDE = False
# CHECK_EXCLUDE = True

# Using sales from howrare.is - the last sale that was under 300 was when the floor was at 72. Filtering for when the floor is >100, the lowest sale was 400

###################################
#     Define Helper Functions     #
###################################
def standardize_df(df, cols, usedf=None):
    for c in cols:
        if type(usedf) != type(pd.DataFrame()):
            usedf = df
        mu = usedf[c].mean()
        sd = usedf[c].std()
        # print(c)
        if len(df[c].unique()) == 2 and df[c].max() == 1 and df[c].min() == 0:
            df['std_{}'.format(c)] = df[c].apply(lambda x: (x*2) - 1 )
        else:
            df['std_{}'.format(c)] = (df[c] - mu) / sd
    return(df)

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
s_df.collection.unique()
s_df = s_df[-s_df.collection.isin(['Levana Dragons'])]
s_df = s_df[[ 'chain','collection','block_timestamp','token_id','price','tx_id' ]]
s_df = s_df[ -s_df.collection.isin(['boryokudragonz', 'Boryoku Dragonz']) ]
for e in exclude:
    s_df = s_df[-( (s_df.collection == e[0]) & (s_df.token_id == e[1]) & (s_df.price == e[2]) )]
s_df = s_df[ -((s_df.collection == 'smb') & (s_df.price < 1)) ]

# exclude wierd data points
if not CHECK_EXCLUDE:
    exclude = pd.read_csv('./data/exclude.csv')
    s_df = s_df.merge(exclude, how='left')
    s_df = s_df[s_df.exclude.isnull()]
    del s_df['exclude']

m_df = pd.read_csv('./data/metadata.csv')
m_df.head()
# s_df['block_timestamp'] = s_df.block_timestamp.apply(lambda x: datetime.strptime(x[:10], '%Y-%m-%d %H:%M:%S') )
s_df['block_timestamp'] = s_df.block_timestamp.apply(lambda x: datetime.strptime(str(x)[:19], '%Y-%m-%d %H:%M:%S') if len(x) > 10 else datetime.strptime(x[:10], '%Y-%m-%d') )
s_df['timestamp'] = s_df.block_timestamp.astype(int)
# del metadata['price']
# del metadata['last_sale']
s_df = s_df.sort_values(['collection','block_timestamp'])
s_df['mn_20'] = s_df.groupby('collection').price.shift(1)
s_df = s_df.sort_values(['collection','block_timestamp'])
s_df['days_ago'] = s_df.block_timestamp.apply(lambda x: (datetime.today() - x).days ).astype(int)
s_df[[ 'block_timestamp','days_ago' ]].drop_duplicates(subset=['days_ago'])

s_df['av_20'] = s_df.groupby('collection')['mn_20'].rolling(20).mean().reset_index(0,drop=True)
s_df = s_df.sort_values(['collection','block_timestamp'])
s_df['md_20'] = s_df.groupby('collection')['mn_20'].rolling(20).median().reset_index(0,drop=True)
s_df = s_df[ (s_df.price) >= (s_df.av_20 * 0.2) ]
s_df['mn_20'] = s_df.groupby('collection').price.shift(1)
# s_df['mn_20'] = s_df.groupby('collection')['mn_20'].rolling(20).min().reset_index(0,drop=True)
s_df['mn_20'] = s_df.groupby('collection')['mn_20'].rolling(20).quantile(.1).reset_index(0,drop=True)
s_df[['price','mn_20','block_timestamp']].head(40).tail(40)
s_df['tmp'] = s_df.mn_20 / s_df.md_20

tmp = s_df[s_df.collection=='smb'][['mn_20','block_timestamp']]
tmp['date'] = tmp.block_timestamp.apply(lambda x: str(x)[:10] )
tmp = tmp.groupby('date').mn_20.median().reset_index()
tmp.to_csv('~/Downloads/tmp.csv', index=False)

s_df['tmp'] = s_df.price / s_df.mn_20
s_df[s_df.collection == 'smb'].sort_values('block_timestamp')[['token_id','price','mn_20']]
s_df[s_df.collection == 'smb'].sort_values('tmp').head(20)[['collection','token_id','price','mn_20','tmp']]
s_df.groupby('collection').tmp.median()
s_df.groupby('collection').tmp.mean()

s_df.sort_values('tmp').head()
s_df['tmp'] = s_df.price / s_df.mn_20
s_df[['collection','token_id','block_timestamp','price','mn_20','md_20','av_20','tmp']].to_csv('~/Downloads/tmp.csv', index=False)
s_df.groupby('collection').tmp.median()
s_df.groupby('collection').tmp.mean()
s_df.sort_values('tmp', ascending=0).head()
s_df.head(21)
m_df = m_df[ -m_df.feature_name.isin([ 'price','last_sale','feature_name','feature_value' ]) ]
# m_df['feature_value'] = m_df.feature_value.apply(lambda x: x.strip() )
# m_df.feature_value.unique()
pred_cols = {}
metadata = {}
sales = {}
collection_features = {}
m_df[(m_df.collection == 'Galactic Punks') & (m_df.feature_name == 'pct')].sort_values('token_id')
c = 'Galactic Punks'
for c in s_df.collection.unique():
    print('Building {} model'.format(c))
    sales[c] = s_df[ s_df.collection == c ]
    pred_cols[c] = sorted(m_df[ m_df.collection == c ].feature_name.unique())
    collection_features[c] = [ c for c in pred_cols[c] if not c in ['score','rank','pct'] ]
    metadata[c] = m_df[ m_df.collection == c ]

    # tmp = pd.pivot_table( metadata[c], ['collection','token_id'], columns=['feature_name'], values=['feature_value'] )
    metadata[c] = metadata[c].pivot( ['collection','token_id'], ['feature_name'], ['feature_value'] ).reset_index()
    metadata[c].columns = [ 'collection','token_id' ] + pred_cols[c]

    features = collection_features[c]
    cur = metadata[c]
    cur = cur.dropna(subset=features)
    for f in features:
        if type(cur[f].values[0] == str):
            cur[f] = cur[f].apply(lambda x: re.sub("\"", "", str(x) ) )
            cur[f] = cur[f].apply(lambda x: re.split("\(", x )[0].strip())
    cur = cur.replace('', 'Default')
    # if not 'pct' in cur.columns:
    cur = calculate_percentages( cur, features )
    dummies = pd.get_dummies(cur[features])
    feature_cols = dummies.columns
    cur = pd.concat([ cur.reset_index(drop=True), dummies.reset_index(drop=True) ], axis=1)
    metadata[c] = cur
    # pred_cols[c] = ['rank','score','timestamp','mn_20','log_mn_20'] + list(dummies.columns)
    pred_cols[c] = ['rank','score'] + list(dummies.columns)

# collection_features = {
#     'Hashmasks': [ 'character','eyecolor','item','mask','skincolor' ]
#     , 'Galactic Punks': [ 'backgrounds','hair','species','suits','jewelry','headware','glasses' ]
#     , 'Solana Monkey Business': [ 'attribute_count','type','clothes','ears','mouth','eyes','hat','background' ]
#     , 'Aurory': [ 'attribute_count','type','clothes','ears','mouth','eyes','hat','background' ]
#     # , 'Thugbirdz': [ 'attribute_count','type','clothes','ears','mouth','eyes','hat','background' ]
# }

sorted(metadata['aurory'].columns)

excludedf = pd.DataFrame()
coefsdf = pd.DataFrame()
salesdf = pd.DataFrame()
attributes = pd.DataFrame()
pred_price = pd.DataFrame()
feature_values = pd.DataFrame()
collections = sorted(metadata.keys())
collection = 'Galactic Punks'
tokens = pd.read_csv('./data/tokens.csv')
for collection in s_df.collection.unique():
    # collection = 'LunaBulls'
    # collection = 'smb'
    # collection = 'aurory'
    # collection = 'meerkatmillionaires'
    print('Working on collection {}'.format(collection))
    p_metadata = metadata[collection]
    p_metadata['attribute_count'] = p_metadata.attribute_count.astype(float).astype(int)
    
    p_sales = sales[collection]
    # specify the predictive features
    p_pred_cols = pred_cols[collection]
    p_features = collection_features[collection]
    p_sales['token_id'] = p_sales.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
    p_metadata['token_id'] = p_metadata.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
    for c in [ 'rank','score' ]:
        p_metadata[c] = p_metadata[c].astype(float)
    # p_sales['contract_address'] = p_sales.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
    # p_metadata['contract_address'] = p_metadata.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
    p_sales['contract_address'] = ''
    p_metadata['contract_address'] = ''

    # remove 1 columns for each group (since they are colinear)
    exclude = []
    for f in p_features:
        e = [ c for c in p_pred_cols if c[:len(f)] == f ][-1]
        exclude.append(e)

    df = p_sales.merge(p_metadata, on=['token_id','contract_address'])
    df = df[df.mn_20.notnull()]
    target_col = 'adj_price'
    df[target_col] = df.apply(lambda x: max(0.7 * (x['mn_20'] - 0.2), x['price']), 1 )
    # df['mn_20'] = df.apply(lambda x: min(x[target_col], x['mn_20']), 1 )
    # tmp = df[['block_timestamp','mn_20']].copy()
    # tmp['tmp'] = tmp.block_timestamp.apply(lambda x: str(x)[:10] )
    # tmp = tmp.groupby('tmp').mn_20.median().reset_index()
    # tmp.sort_values('tmp').to_csv('~/Downloads/tmp.csv', index=False)
    # df['timestamp'] = df.block_timestamp.astype(int)
    df = df[df[target_col].notnull()]
    df = df.reset_index(drop=True)
    df['rel_price_0'] = df[target_col] - df.mn_20
    df['rel_price_1'] = df[target_col] / df.mn_20
    df = df[df.mn_20 > 0]
    df['log_mn_20'] = np.log(df.mn_20)
    print('Training on {} sales'.format(len(df)))
    # df['price_median'] = df.groupby('token_id').price.median()

    # standardize columns to mean 0 sd 1
    df = standardize_df(df, p_pred_cols)
    std_pred_cols_0 = [ 'std_{}'.format(c) for c in p_pred_cols ]
    # p_pred_cols = [ c for c in p_pred_cols if not c in exclude ]
    std_pred_cols = [ 'std_{}'.format(c) for c in p_pred_cols ]
    df['log_price'] = df[target_col].apply(lambda x: np.log(x) )
    # df.sort_values('block_timestamp').head(10)[['price','tx_id']]
    # df.sort_values('block_timestamp').head(10)[['price','tx_id']].tx_id.values
    # df = df[df.price >= 1]

    #########################
    #     Run the Model     #
    #########################
    len(df)
    len(df.dropna(subset=std_pred_cols))
    tmp = df[std_pred_cols].count().reset_index()
    tmp.columns = ['a','b']
    tmp.sort_values('b').head(20)
    rem = list(tmp[tmp.b==0].a.values)
    std_pred_cols = [ c for c in std_pred_cols if not c in rem ]
    mn = df.timestamp.min()
    mx = df.timestamp.max()
    df['weight'] = df.timestamp.apply(lambda x: 2.5 ** ((x - mn) / (mx - mn)) )
    X = df[std_pred_cols].values
    mu = df.log_price.mean()
    sd = df.log_price.std()
    df['std_log_price'] = (df.log_price - mu) / sd
    # y = df.std_log_price.values
    # y = df[target_col].values
    # y = df.rel_price_1.values
    y_0 = df.rel_price_0.values
    y_1 = df.rel_price_1.values
    # y_log = df.log_price.values

    # max_depth, min_samples_leaf, max_leaf_nodes
    # d = {}
    # tracker = []
    # for max_depth in [ 2, 4, 6 ]:
    #     for min_samples_leaf in [5, 25, 100]:
    #         for max_leaf_nodes in [25, 100, 500]:
    #             x_train, x_test, y_train, y_test = train_test_split(X, y, test_size = .2, random_state = 123)

    #             clf = RandomForestRegressor(max_depth=max_depth, min_samples_leaf=min_samples_leaf, max_leaf_nodes=max_leaf_nodes)
    #             kf = KFold(n_splits=4)
    #             errs = []
    #             pred = np.zeros(len(X))
    #             for train_index, test_index in kf.split(X):
    #                 X_train, X_test = X[train_index], X[test_index]
    #                 y_train, y_test = y[train_index], y[test_index]
    #                 clf.fit(X_train, y_train)
    #                 y_pred = clf.predict(X_test)
    #                 pred[test_index] = y_pred
    #                 err = np.mean([ abs((y_p - y_a) - 0) for y_p, y_a in zip(y_pred, y_test) ])
    #                 # err = np.mean([ ((y_p - y_a) - 0) for y_p, y_a in zip(y_pred, y_test) ])
    #                 errs.append(err)
    #             tracker += [[ max_depth, min_samples_leaf, max_leaf_nodes, np.mean(errs) ]]
    # tracker = pd.DataFrame(tracker, columns=['max_depth', 'min_samples_leaf', 'max_leaf_nodes', 'avg_err']).sort_values('avg_err', ascending=0)
    # tracker
    # df['pred'] = pred
    # df[['pred','rel_price_1','token_id','mn_20']].head()



    # # run neural net
    # model = tf.keras.models.Sequential([
    #     tf.keras.layers.Dense(10, activation='relu')
    #     # , tf.keras.layers.Dense(10, activation='linear')
    #     , tf.keras.layers.Dense(5, activation='relu')
    #     # , tf.keras.layers.Dense(3, activation='linear')
    #     , tf.keras.layers.Dense(1, activation='linear')
    # ])
    # # model.compile(loss=tf.keras.losses.MeanAbsoluteError(), optimizer='adam')
    # model.compile(loss=tf.keras.losses.MeanSquaredError(), optimizer='sgd')
    # # model.compile(loss='mse', optimizer='sgd')
    # # model.compile(loss='mse', optimizer=tf.keras.optimizers.SGD(learning_rate=0.001))
    # # model.compile(loss='mae', optimizer=tf.keras.optimizers.SGD(learning_rate=0.0025))
    # # model.compile(loss='mae', optimizer=tf.keras.optimizers.SGD())
    # model.fit(X, y, epochs=200, validation_split=0.25)

    # print(np.mean(y))
    # print(np.mean(model.predict(X)))



    # d = {}
    # d[collection] = df
    # ret = models.fit_mlp( d, collection, std_pred_cols, None, .2, classifier = False, useUSD = False )

    clf_lin = RidgeCV(alphas=[1.5**x for x in range(20)])
    clf_lin.fit(X, y_0, df.weight.values)
    df['pred_lin'] = clf_lin.predict(X)
    df['pred_lin'] = df.pred_lin.apply(lambda x: max(0, x)) + df.mn_20
    df['err_lin'] = abs(((df.pred_lin - df[target_col]) / df[target_col]) )
    # df['err_lin'] = abs(df.pred_lin - df.price )
    # df[[ 'price','pred_lin','err_lin','mn_20' ]].sort_values('err_lin').tail(50)
    df.head()
    clf_log = RidgeCV(alphas=[1.5**x for x in range(20)])
    clf_log.fit(X, y_1, df.weight.values)
    df['pred_log'] = clf_log.predict(X)
    df['pred_log'] = df.pred_log.apply(lambda x: max(1, x)) * df.mn_20
    df['err_log'] = abs(((df.pred_log - df[target_col]) / df[target_col]) )
    df[[ target_col,'pred_log','err_log','mn_20' ]].sort_values('err_log').tail(50)

    df['err'] = df.err_lin * df.err_log

    df[[ target_col,'pred_log','err_log','err_lin','err','mn_20' ]].sort_values('err').tail(50)
    df['collection'] = collection
    excludedf = excludedf.append(df[df.err > 2][['collection','token_id','price']])
    # df = df[df.err < 2]

    print(round(len(df[df.err > 2]) * 100.0 / len(df), 2))

    df[(df.err_log > 1) & (df.err_lin >= 5)]

    clf_log = RidgeCV(alphas=[1.5**x for x in range(20)])
    clf_log.fit(X, y_1, df.weight.values)

    clf_log = RidgeCV(alphas=[1.5**x for x in range(20)])
    clf_log.fit(X, y_1, df.weight.values)
    df['pred_lin'] = clf_lin.predict(X)
    df['pred_lin'] = df.pred_lin.apply(lambda x: max(0, x)) + df.mn_20
    # df['pred_log'] = np.exp(clf_log.predict(X))
    df['pred_log'] = clf_log.predict(X)
    df['pred_log'] = df.pred_log.apply(lambda x: max(1, x)) * df.mn_20
    clf = LinearRegression(fit_intercept=False)
    clf.fit( df[['pred_lin','pred_log']].values, df[target_col].values, df.weight.values )
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
    df[df.block_timestamp>='2021-10-01'].sort_values('err', ascending=0).head(10)[[ 'pred',target_col,'token_id','block_timestamp','err','mn_20' ]]
    # df[df.block_timestamp>='2021-10-01'].err.mean()
    df.merge(tokens[['collection','token_id','clean_token_id']]).sort_values('err', ascending=0).head(10)[[ 'pred',target_col,'clean_token_id','rank','block_timestamp','err','mn_20','tx_id' ]]
    df.sort_values('price', ascending=0).head(20)[[ 'price','pred',target_col,'token_id','block_timestamp','err','mn_20','tx_id' ]]
    df.sort_values('price', ascending=0).tail(40)[[ 'price','pred',target_col,'token_id','block_timestamp','err','mn_20','tx_id' ]]
    df.sort_values('price', ascending=0).head(20).tx_id.values

    # print(np.mean(y))
    # print(np.mean(clf.predict(X)))

    # # run neural net
    # model = tf.keras.models.Sequential([
    #     tf.keras.layers.Dense(9, activation='relu')
    #     , tf.keras.layers.Dropout(.2)
    #     , tf.keras.layers.Dense(3, activation='relu')
    #     , tf.keras.layers.Dropout(.2)
    #     , tf.keras.layers.Dense(1, activation='linear')
    # ])
    # model.compile(loss='mae', optimizer=tf.keras.optimizers.SGD(learning_rate=0.0025))
    # model.fit(X, y, epochs=500, validation_split=0.3)

    # df['pred'] = np.exp( (sd * model.predict(df[std_pred_cols].values)) + mu)
    # df['pred'] = model.predict(df[std_pred_cols].values)
    # ratio = df.price.mean() / df.pred.mean()
    # print("Manually increasing predictions by {}%".format(round((ratio-1) * 100, 1)))

    # checking errors
    # df['pred'] = df.pred * ratio
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
    print(df[df.weight >= df.weight.median()].groupby('q')[['err','pred',target_col]].mean())
    # df.err.mean()
    # df[df.weight >= 3.5].err.mean()
    df['collection'] = collection
    print('Avg err last 100: {}'.format(round(df.sort_values('block_timestamp').head(100).err.mean(), 2)))
    salesdf = salesdf.append( df[[ 'collection','contract_address','token_id','block_timestamp','price','pred','mn_20','rank','score' ]].sort_values('block_timestamp', ascending=0) )

    # create the attributes dataframe
    for f in p_features:
        cur = p_metadata[[ 'token_id', f, '{}_pct'.format(f) ]]
        cur.columns = [ 'token_id', 'value','rarity' ]
        cur['feature'] = f
        cur['collection'] = collection
        attributes = attributes.append(cur)

    # create predictions for each NFT in the collection
    test = p_metadata.copy()
    tail = df.sort_values('timestamp').tail(1)
    for c in [ 'std_timestamp','mn_20','log_mn_20' ]:
        if c in tail.columns:
            test[c] = tail[c].values[0]
    test = standardize_df(test, [c for c in p_pred_cols if not c in ['timestamp'] ], df)
    # test['pred_lin'] = clf_lin.predict( test[std_pred_cols].values )
    # test['pred_log'] = np.exp(clf_log.predict( test[std_pred_cols].values ))

    test['pred_lin'] = clf_lin.predict(test[std_pred_cols].values)
    test['pred_lin'] = test.pred_lin.apply(lambda x: max(0, x) + l)
    # test['pred_lin'] = df.pred_lin + df.mn_20
    # df['pred_log'] = np.exp(clf_log.predict(X))
    test['pred_log'] = clf_log.predict(test[std_pred_cols].values)
    test['pred_log'] = test.pred_log.apply(lambda x: max(1, x)) * l

    test['pred'] = clf.predict( test[[ 'pred_lin','pred_log' ]].values )
    # test['pred'] = np.exp( (sd * model.predict(test[std_pred_cols].values)) + mu) * ratio
    test['pred_price'] = test.pred#.apply(lambda x: x*(1+pe_mu) )
    if not CHECK_EXCLUDE:
        test['pred_price'] = test.pred.apply(lambda x: (x*0.985) )
    test['pred_sd'] = test.pred * pe_sd
    test['rk'] = test.pred.rank(ascending=0, method='first')
    test['collection'] = collection
    pred_price = pred_price.append( test[[ 'collection', 'contract_address','token_id','rank','rk','pred_price','pred_sd' ] + p_features].rename(columns={'rank':'hri_rank'}).sort_values('pred_price') )
    # print(test[[ 'contract_address','token_id','pred_price','pred_sd' ]].sort_values('pred_price'))


    ##############################
    #     Feature Importance     #
    ##############################
    coefs = []
    for a, b, c in zip(p_pred_cols, clf_lin.coef_, clf_log.coef_):
        coefs += [[ collection, a, b, c ]]
    coefs = pd.DataFrame(coefs, columns=['collection','col','lin_coef','log_coef'])
    # coefs['feature'] = coefs.col.apply(lambda x: ' '.join(re.split('_', x)[:-1]).title() )
    # coefs['feature'] = coefs.col.apply(lambda x: '_'.join(re.split('_', x)[:-1]) )
    # coefs['value'] = coefs.col.apply(lambda x: re.split('_', x)[-1] )
    # mn = coefs.groupby('feature')[[ 'lin_coef','log_coef' ]].min().reset_index()
    # mn.columns = [ 'feature','mn_lin_coef','mn_log_coef' ]
    # coefs = coefs.merge(mn)
    # coefs['lin_coef'] = coefs.lin_coef - coefs.mn_lin_coef
    # coefs['log_coef'] = coefs.log_coef - coefs.mn_log_coef
    # coefs
    # g = attributes[ attributes.collection == collection ][[ 'feature','value','rarity' ]].drop_duplicates()
    # g['value'] = g.value.astype(str)
    # len(coefs)
    # g = coefs.merge(g, how='left')
    # g[g.rarity.isnull()]
    # len(g)
    # coefs = coefs.merge( m_df[ m_df.collection == collection ][[ 'feature_name','' ]] )
    # coefs.sort_values('lin_coef').tail(20)

    # TODO: pick the most common one and have that be the baseline
    most_common = attributes[(attributes.collection == collection)].sort_values('rarity', ascending=0).groupby('feature').head(1)
    most_common['col'] = most_common.apply(lambda x: 'std_{}_{}'.format( re.sub(' ', '_', x['feature'].lower()), x['value'] ), 1 )
    mc = most_common.col.unique()
    data = []
    for c0 in std_pred_cols_0:
        if c0 in ['std_rank','std_score','std_pct','std_timestamp','std_mn_20','std_log_mn_20']:
            continue
        f = '_'.join(re.split('_', c0)[1:-1])
        v = re.split('_', c0)[-1]
        rarity = p_metadata[p_metadata['{}_{}'.format(f, v)]==1]['{}_pct'.format(f)].values[0]
        # avg = p_metadata['{}_pct'.format(f)].mean()
        # avg_pct = df.pct.mean()
        # pct_std = ((avg_pct * r / avg) - avg_pct) / df.pct.std()
        r = df[df['{}_{}'.format(f, v)]==1].std_rank.mean()
        s = df[df['{}_{}'.format(f, v)]==1].std_score.mean()
        if r == r and s == s:
            datum = [ c0, rarity ]
            for c1 in std_pred_cols:
                datum.append(1 if c1 == c0 else r if c1 == 'std_rank' else s if c1 == 'std_score' else 1 if c1 in mc else 0 )
            data += [ datum ]

    importance = pd.DataFrame(data, columns=['feature','rarity']+std_pred_cols)
    sorted(importance.feature.unique())
    importance[importance.feature == 'std_fur_/_skin_Leopard']
    if 'std_timestamp' in df.columns:
        importance['std_timestamp'] = df.std_timestamp.max()
    # importance['pred_lin'] = clf_lin.predict( importance[std_pred_cols].values )
    # importance['pred_log'] = np.exp(clf_log.predict( importance[std_pred_cols].values ))

    importance['pred_lin'] = clf_lin.predict(importance[std_pred_cols].values)
    importance['pred_lin'] = importance.pred_lin.apply(lambda x: max(0, x) + l)
    # importance['pred_lin'] = importance.pred_lin.apply(lambda x: x + l)
    importance['pred_log'] = clf_log.predict(importance[std_pred_cols].values)
    importance['pred_log'] = importance.pred_log.apply(lambda x: max(1, x)) * l
    # importance['pred_log'] = importance.pred_log.apply(lambda x: x) * l

    importance['pred'] = clf.predict( importance[[ 'pred_lin','pred_log' ]].values )
    # importance['pred'] = np.exp( (sd * model.predict(importance[std_pred_cols].values)) + mu)
    importance = importance.sort_values('pred', ascending=0)
    importance.head()[['feature','pred']]
    importance[importance.feature == 'std_fur_/_skin_Leopard']
    importance['feature'] = importance.feature.apply(lambda x: re.sub('std_', '', x))
    importance['value'] = importance.feature.apply(lambda x: re.split('_', x)[-1])
    importance['feature'] = importance.feature.apply(lambda x: '_'.join(re.split('_', x)[:-1]))
    mn = importance.groupby('feature').pred.min().reset_index().rename(columns={'pred':'baseline'})
    importance = importance.merge(mn)
    importance['pred_vs_baseline'] = importance.pred - importance.baseline
    importance['pct_vs_baseline'] = (importance.pred / importance.baseline) - 1
    importance[(importance.feature == 'fur_/_skin')].sort_values('pred')[['value','rarity','pred','pred_lin','pred_log','std_rank','std_score']].sort_values('rarity')
    importance['collection'] = collection
    importance.sort_values('pct_vs_baseline')[['feature','value','pct_vs_baseline']]
    tmp = importance[std_pred_cols].mean().reset_index()
    tmp.columns = [ 'a', 'b' ]
    tmp = tmp.sort_values('b')
    feature_values = feature_values.append(importance[['collection','feature','value','pred','pred_vs_baseline','pct_vs_baseline','rarity']])

attributes['feature'] = attributes.feature.apply(lambda x: re.sub('_', ' ', x).title() )
feature_values['feature'] = feature_values.feature.apply(lambda x: re.sub('_', ' ', x).title() )

pred_price = pred_price[[ 'collection', 'contract_address', 'token_id', 'hri_rank', 'rk', 'pred_price', 'pred_sd' ]]

coefsdf.to_csv('./data/coefsdf.csv', index=False)
salesdf.to_csv('./data/model_sales.csv', index=False)
pred_price.to_csv('./data/pred_price.csv', index=False)
attributes.to_csv('./data/attributes.csv', index=False)
feature_values.to_csv('./data/feature_values.csv', index=False)
# excludedf.to_csv('./data/excludedf.csv', index=False)
# listings = pd.read_csv('./data/listings.csv')
# listings['token_id'] = listings.token_id.astype(int)

# tmp = salesdf.merge(attributes[ (attributes.collection == 'thugbirdz') & (attributes.feature == 'Position In Gang') & (attributes.value == 'Underboss') ])
# tmp = pred_price.merge(attributes[ (attributes.collection == 'thugbirdz') & (attributes.feature == 'Position In Gang') & (attributes.value == 'Underboss') ])
# tmp['token_id'] = tmp.token_id.astype(int)
# tmp = tmp.merge(listings[['collection','token_id','price']])
# tmp.sort_values('pred_price', ascending=0)

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

attributes[ (attributes.collection == 'thugbirdz') & (attributes.token_id == '1869') ]
feature_values[ (feature_values.collection == 'thugbirdz') & (feature_values.feature == 'position_in_gang') ]
sorted(feature_values[ (feature_values.collection == 'thugbirdz') ].feature.unique())

pred_price[pred_price.collection == 'peskypenguinclub'].head()