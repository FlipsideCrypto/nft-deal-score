import os
import re
import json
import requests
import numpy as np
import pandas as pd
import urllib.request
import tensorflow as tf
import snowflake.connector
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, RidgeCV

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

def standardize_df(df, cols, usedf=None):
    for c in cols:
        # print(c)
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
    if not len(cols):
        cols = df.columns
    df['pct'] = 1
    for c in cols:
        g = df[c].value_counts().reset_index()
        g.columns = [ c, 'N' ]
        col = '{}_pct'.format(c)
        g[col] = g.N / g.N.sum()
        df = df.merge( g[[ c, col ]] )
        df['pct'] = df.pct * df[col]
    return(df)

CONTRACT_ADDRESSES = [ '0xc2c747e0f7004f9e8817db2ca4997657a7746928' ]
contract_address_query = "'{}'".format( "','".join(CONTRACT_ADDRESSES) )
using_art_blocks = len(set(['0x059edd72cd353df5106d2b9cc5ab83a52287ac3a','0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270']).intersection(set(CONTRACT_ADDRESSES))) > 0


#########################
#     Load Metadata     #
#########################
def load_metadata_data():
    query = '''
    SELECT
    token_id
    , contract_address
    , token_metadata:collection_name as collection_name
    , token_metadata:character as character
    , token_metadata:eyeColor as eyeColor
    , token_metadata:ipfs_hash as ipfs_hash
    , token_metadata:item as item
    , token_metadata:mask as mask
    , token_metadata:skinColor as skinColor
    FROM ethereum.nft_metadata
    WHERE contract_address in ({})
    '''.format( contract_address_query )
    metadata_0 = ctx.cursor().execute(query)
    metadata_0 = pd.DataFrame.from_records(iter(metadata_0), columns=[x[0] for x in metadata_0.description])
    metadata_0 = clean_colnames(metadata_0)
    metadata_0.head()

    query = '''
    SELECT
    token_id
    , contract_address
    , token_metadata:traits:backgrounds as backgrounds
    , token_metadata:traits:hair as hair
    , token_metadata:traits:species as species
    , token_metadata:traits:suits as suits
    , token_metadata:traits:jewelry as jewelry
    , token_metadata:traits:headware as headware
    , token_metadata:traits:glasses as glasses
    FROM terra.nft_metadata
    WHERE contract_address in ('terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k')
    '''
    metadata_1 = ctx.cursor().execute(query)
    metadata_1 = pd.DataFrame.from_records(iter(metadata_1), columns=[x[0] for x in metadata_1.description])
    metadata_1 = clean_colnames(metadata_1)
    metadata_1.head()

    metadata = {
        'Hashmasks': metadata_0
        , 'Galactic Punks': metadata_1
    }

    return(metadata)

metadata = load_metadata_data()

######################
#     Load Sales     #
######################
def load_sales_data():
    query = '''
    WITH mints AS (
        SELECT DISTINCT tx_id
        FROM ethereum.nft_events
        WHERE contract_address in ({})
        AND event_type = 'mint'
    )
    SELECT
    e.tx_id
    , token_id
    , block_timestamp
    , contract_address
    , price
    , price_usd
    FROM ethereum.nft_events e
    LEFT JOIN mints m ON m.tx_id = e.tx_id
    WHERE contract_address in ({})
    AND event_type = 'sale'
    AND m.tx_id IS NULL
    AND price_usd > 0
    '''.format( contract_address_query, contract_address_query )
    sales_0 = ctx.cursor().execute(query)
    sales_0 = pd.DataFrame.from_records(iter(sales_0), columns=[x[0] for x in sales_0.description])
    sales_0 = clean_colnames(sales_0)
    sales_0.head()
    sales_0.tx_id.values[0]
    sales_0['collection'] = 'Hashmasks'
    sales_0 = sales_0.sort_values('block_timestamp')
    sales_0['mn_20'] = sales_0.groupby('collection').price_usd.shift(1).rolling(20).min()

    query = '''
    SELECT 
    tx_id
    , msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:token_id AS token_id
    , block_timestamp
    , msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:contract_addr AS contract_address
    , NULL AS price
    , msg_value:execute_msg:execute_order:order:order:taker_asset:amount::decimal/pow(10,6) AS price_usd
    FROM terra.msgs 
    WHERE (
        msg_value:contract::string = 'terra1eek0ymmhyzja60830xhzm7k7jkrk99a60q2z2t' 
        AND	msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:contract_addr::string = 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k'
        AND	tx_status = 'SUCCEEDED'
        AND	msg_value:execute_msg:execute_order IS NOT NULL
        AND price_usd > 0
    )
    '''
    sales_1 = ctx.cursor().execute(query)
    sales_1 = pd.DataFrame.from_records(iter(sales_1), columns=[x[0] for x in sales_1.description])
    sales_1 = clean_colnames(sales_1)
    sales_1['collection'] = 'Galactic Punks'
    sales_1.head()
    sales_1 = sales_1.sort_values('block_timestamp')
    sales_1['mn_20'] = sales_1.groupby('collection').price_usd.shift(1).rolling(20).min()

    sales = {
        'Hashmasks': sales_0
        , 'Galactic Punks': sales_1
    }
    return(sales)
sales = load_sales_data()


#############################################
#     Select Collection With Most Sales     #
#############################################
if False and using_art_blocks:
    df = sales.merge(metadata[[ 'token_id','contract_address','collection_name' ]])
    df['num_sales'] = 1
    df = df.groupby( ['collection_name','contract_address'] )[['price','num_sales']].sum().reset_index().sort_values('num_sales', ascending=0)
    collection_name = df.collection_name.values[0]
    print('Collection with most sales is {}'.format(collection_name))


######################################################
#     Create Dataframe with Requisite Collection     #
######################################################
pred_cols = {}
if False and using_art_blocks:
    metadata = metadata[metadata.collection_name == collection_name]
    metadata = metadata[metadata.feature.notnull()]
    features = [json.loads(x) for x in metadata.feature]
    clean_features = []
    for row in features:
        clean_feature_dict = {}
        for feature in row:
            k = re.split(':', feature)
            clean_feature_dict[k[0]] = k[1].strip()
        clean_features += [ clean_feature_dict ]
    clean_features_df = pd.DataFrame(clean_features)
    clean_features_df = clean_features_df.fillna('default')
    clean_features_df = calculate_percentages(clean_features_df)
    metadata = pd.concat( [metadata.reset_index(drop=True), clean_features_df.reset_index(drop=True)], axis=1 )
    metadata[metadata.pct.isnull()]
else:
    # hashmasks
    collection_features = {
        'Hashmasks': [ 'character','eyecolor','item','mask','skincolor' ]
        , 'Galactic Punks': [ 'backgrounds','hair','species','suits','jewelry','headware','glasses' ]
    }
    for p in metadata.keys():
        features = collection_features[p]
        cur = metadata[p]
        cur = cur.dropna(subset=features)
        for f in features:
            cur[f] = cur[f].apply(lambda x: re.sub("\"", "", x ))
        cur = calculate_percentages( cur, features )
        dummies = pd.get_dummies(cur[features])
        feature_cols = dummies.columns
        cur = pd.concat([ cur.reset_index(drop=True), dummies.reset_index(drop=True) ], axis=1)
        metadata[p] = cur
        pred_cols[p] = ['pct','timestamp','mn_20','log_mn_20'] + list(dummies.columns)


#####################################
#     Create Training DataFrame     #
#####################################
salesdf = pd.DataFrame()
attributes = pd.DataFrame()
pred_price = pd.DataFrame()
feature_values = pd.DataFrame()
collections = sorted(metadata.keys())
for collection in collections:
    print('Working on collection {}'.format(collection))
    p_metadata = metadata[collection]
    p_sales = sales[collection]
    # specify the predictive features
    p_pred_cols = pred_cols[collection]
    p_features = collection_features[collection]
    p_sales['token_id'] = p_sales.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
    p_metadata['token_id'] = p_metadata.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
    p_sales['contract_address'] = p_sales.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
    p_metadata['contract_address'] = p_metadata.token_id.apply(lambda x: re.sub("\"", "", str(x)) )

    # remove 1 columns for each group (since they are colinear)
    exclude = []
    for f in p_features:
        e = [ c for c in p_pred_cols if c[:len(f)] == f ][-1]
        exclude.append(e)
    p_pred_cols = [ c for c in p_pred_cols if not c in exclude ]

    df = p_sales.merge(p_metadata, on=['token_id','contract_address'])
    df = df[df.mn_20.notnull()]
    df['timestamp'] = df.block_timestamp.astype(int)
    df = df[df.price_usd.notnull()]
    df = df.reset_index(drop=True)
    df['log_mn_20'] = np.log(df.mn_20)
    print('Training on {} sales'.format(len(df)))

    # standardize columns to mean 0 sd 1
    std_pred_cols = [ 'std_{}'.format(c) for c in p_pred_cols ]
    df = standardize_df(df, p_pred_cols)
    df['log_price_usd'] = df.price_usd.apply(lambda x: np.log(x) )
    df = df[df.price_usd >= 1]

    #########################
    #     Run the Model     #
    #########################
    mn = df.timestamp.min()
    mx = df.timestamp.max()
    df['weight'] = df.timestamp.apply(lambda x: 2 ** (2 * ((x - mn) / (mx - mn))) )
    X = df[std_pred_cols].values
    mu = df.log_price_usd.mean()
    sd = df.log_price_usd.std()
    df['std_log_price_usd'] = (df.log_price_usd - mu) / sd
    y = df.std_log_price_usd.values
    y = df.price_usd.values
    y_log = df.log_price_usd.values

    clf_lin = RidgeCV(alphas=[1.5**x for x in range(20)])
    clf_lin.fit(X, y, df.weight.values)
    clf_log = RidgeCV(alphas=[1.5**x for x in range(20)])
    clf_log.fit(X, y_log, df.weight.values)
    df['pred_lin'] = clf_lin.predict(X)
    df['pred_log'] = np.exp(clf_log.predict(X))
    clf = RidgeCV(alphas=[1.5**x for x in range(20)])
    clf.fit( df[['pred_lin','pred_log']].values, df.price_usd.values, df.weight.values )
    print('Price = {} * lin + {} * log'.format( round(clf.coef_[0], 2), round(clf.coef_[1], 2) ))
    if clf.coef_[0] < 0:
        print('Only using log')
        df['pred'] = df.pred_log
    elif clf.coef_[1] < 0:
        print('Only using lin')
        df['pred'] = df.pred_lin
    else:
        print('Only using BOTH!')
        df['pred'] = clf.predict( df[['pred_lin','pred_log']].values )

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
    # ratio = df.price_usd.mean() / df.pred.mean()
    # print("Manually increasing predictions by {}%".format(round((ratio-1) * 100, 1)))

    # checking errors
    # df['pred'] = df.pred * ratio
    df['err'] = df.price_usd - df.pred
    df['q'] = df.pred.rank() * 10 / len(df)
    df['q'] = df.q.apply(lambda x: int(round(x)) )
    df['pct_err'] = (df.price_usd / df.pred) - 1
    pe_mu = df.pct_err.mean()
    pe_sd = df[ (df.pct_err > -.9) & (df.pct_err < 0.9) ].pct_err.std()
    df['pred_price'] = df.pred#.apply(lambda x: x*(1+pe_mu) )
    df['pred_sd'] = df.pred * pe_sd
    print(df.groupby('q')[['err','pred','price_usd']].mean())
    print(df[df.weight >= 3.5].groupby('q')[['err','pred','price_usd']].mean())
    # df.err.mean()
    # df[df.weight >= 3.5].err.mean()
    df['collection'] = collection
    salesdf = salesdf.append( df[[ 'collection','contract_address','token_id','block_timestamp','price_usd','pred' ] + p_features].sort_values('block_timestamp', ascending=0) )

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
        test[c] = tail[c].values[0]
    test = standardize_df(test, [c for c in p_pred_cols if not c in ['timestamp'] ], df)
    test['pred_lin'] = clf_lin.predict( test[std_pred_cols].values )
    test['pred_log'] = np.exp(clf_log.predict( test[std_pred_cols].values ))
    test['pred'] = clf.predict( test[[ 'pred_lin','pred_log' ]].values )
    # test['pred'] = np.exp( (sd * model.predict(test[std_pred_cols].values)) + mu) * ratio
    test['pred_price'] = test.pred#.apply(lambda x: x*(1+pe_mu) )
    test['pred_sd'] = test.pred * pe_sd
    test['rk'] = test.pred.rank(ascending=0)
    test['collection'] = collection
    pred_price = pred_price.append( test[[ 'collection', 'contract_address','token_id','rk','pred_price','pred_sd' ] + p_features].sort_values('pred_price') )
    # print(test[[ 'contract_address','token_id','pred_price','pred_sd' ]].sort_values('pred_price'))


    ##############################
    #     Feature Importance     #
    ##############################
    coefs = []
    for a, b, c in zip(p_pred_cols, clf_lin.coef_, clf_log.coef_):
        coefs += [[ collection, a, b, c ]]
    coefs = pd.DataFrame(coefs, columns=['collection','col','lin_coef','log_coef'])

    data = []
    for c0 in std_pred_cols:
        if c0 in ['std_pct','std_timestamp','std_mn_20','std_log_mn_20']:
            continue
        f = re.split('_', c0)[1]
        v = re.split('_', c0)[2]
        r = p_metadata[p_metadata['{}_{}'.format(f, v)]==1]['{}_pct'.format(f)].values[0]
        avg = p_metadata['{}_pct'.format(f)].mean()
        avg_pct = df.pct.mean()
        pct_std = ((avg_pct * r / avg) - avg_pct) / df.pct.std()
        datum = [ c0, r ]
        for c1 in std_pred_cols:
            datum.append(1 if c1 == c0 else pct_std if c1 == 'std_pct' else 0)
        data += [ datum ]

    importance = pd.DataFrame(data, columns=['feature','rarity']+std_pred_cols)
    importance['std_timestamp'] = df.std_timestamp.max()
    importance['pred_lin'] = clf_lin.predict( importance[std_pred_cols].values )
    importance['pred_log'] = np.exp(clf_log.predict( importance[std_pred_cols].values ))
    importance['pred'] = clf.predict( importance[[ 'pred_lin','pred_log' ]].values )
    # importance['pred'] = np.exp( (sd * model.predict(importance[std_pred_cols].values)) + mu)
    importance = importance.sort_values('pred', ascending=0)
    importance.head()[['feature','pred']]
    importance['feature'] = importance.feature.apply(lambda x: re.sub('std_', '', x))
    importance['value'] = importance.feature.apply(lambda x: re.split('_', x)[1])
    importance['feature'] = importance.feature.apply(lambda x: re.split('_', x)[0])
    mn = importance.groupby('feature').pred.min().reset_index().rename(columns={'pred':'baseline'})
    importance = importance.merge(mn)
    importance['pred_vs_baseline'] = importance.pred - importance.baseline
    importance['pct_vs_baseline'] = (importance.pred / importance.baseline) - 1
    importance['collection'] = collection
    feature_values = feature_values.append(importance[['collection','feature','value','pred','pred_vs_baseline','pct_vs_baseline','rarity']])

salesdf.to_csv('./data/sales.csv', index=False)
pred_price.to_csv('./data/pred_price.csv', index=False)
attributes.to_csv('./data/attributes.csv', index=False)
feature_values.to_csv('./data/feature_values.csv', index=False)

pred_price.collection.unique()

pred_price.collection
attributes.collection
feature_values.collection