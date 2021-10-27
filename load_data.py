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
metadata = ctx.cursor().execute(query)
metadata = pd.DataFrame.from_records(iter(metadata), columns=[x[0] for x in metadata.description])
metadata = clean_colnames(metadata)
metadata.head()


######################
#     Load Sales     #
######################
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
'''.format( contract_address_query, contract_address_query )
sales = ctx.cursor().execute(query)
sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
sales = clean_colnames(sales)
sales.head()
sales.tx_id.values[0]

#############################################
#     Select Collection With Most Sales     #
#############################################
if using_art_blocks:
    df = sales.merge(metadata[[ 'token_id','contract_address','collection_name' ]])
    df['num_sales'] = 1
    df = df.groupby( ['collection_name','contract_address'] )[['price','num_sales']].sum().reset_index().sort_values('num_sales', ascending=0)
    collection_name = df.collection_name.values[0]
    print('Collection with most sales is {}'.format(collection_name))


######################################################
#     Create Dataframe with Requisite Collection     #
######################################################
if using_art_blocks:
    metadata = metadata[metadata.collection_name == collection_name]
    metadata = metadata[metadata.feature.notnull()]
    features = [json.loads(x) for x in data.feature]
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
    features = [ 'character','eyecolor','item','mask','skincolor' ]
    for f in features:
        metadata[f] = metadata[f].apply(lambda x: re.sub("\"", "", x ))
    metadata = calculate_percentages( metadata, features )
    dummies = pd.get_dummies(metadata[features])
    feature_cols = dummies.columns
    metadata = pd.concat([ metadata.reset_index(drop=True), dummies.reset_index(drop=True) ], axis=1)


#####################################
#     Create Training DataFrame     #
#####################################
# specify the predictive features
pred_cols = ['pct','timestamp'] + list(dummies.columns)

# remove 1 columns for each group (since they are colinear)
exclude = []
for f in features:
    e = [ c for c in pred_cols if c[:len(f)] == f ][-1]
    exclude.append(e)
pred_cols = [ c for c in pred_cols if not c in exclude ]

df = sales.merge(metadata)
df['timestamp'] = df.block_timestamp.astype(int)
df = df[df.price_usd.notnull()]
df = df.reset_index(drop=True)
print('Training on {} sales'.format(len(df)))

# standardize columns to mean 0 sd 1
std_pred_cols = [ 'std_{}'.format(c) for c in pred_cols ]
df = standardize_df(df, pred_cols)
df['log_price_usd'] = df.price_usd.apply(lambda x: np.log(x) )
df = df[df.price_usd >= 100]

#########################
#     Run the Model     #
#########################
X = df[std_pred_cols].values
mu = df.log_price_usd.mean()
sd = df.log_price_usd.std()
df['std_log_price_usd'] = (df.log_price_usd - mu) / sd
y = df.std_log_price_usd.values

# run neural net
model = tf.keras.models.Sequential([
    tf.keras.layers.Dense(20, activation='linear')
    , tf.keras.layers.Dense(10, activation='sigmoid')
    # , tf.keras.layers.Dense(10, activation='linear')
    , tf.keras.layers.Dense(5, activation='sigmoid')
    # , tf.keras.layers.Dense(3, activation='linear')
    , tf.keras.layers.Dense(1, activation='linear')
])
# model.compile(loss=tf.keras.losses.MeanAbsoluteError(), optimizer='adam')
model.compile(loss=tf.keras.losses.MeanSquaredError(), optimizer='sgd')
# model.compile(loss='mse', optimizer='sgd')
# model.compile(loss='mse', optimizer=tf.keras.optimizers.SGD(learning_rate=0.001))
# model.compile(loss='mae', optimizer=tf.keras.optimizers.SGD(learning_rate=0.0025))
# model.compile(loss='mae', optimizer=tf.keras.optimizers.SGD())
model.fit(X, y, epochs=200, validation_split=0.3)

# some sanity checking
print(np.mean(y))
print(np.mean(model.predict(X)))

# checking errors
df['pred'] = np.exp( (sd * model.predict(df[std_pred_cols].values)) + mu)
ratio = df.price_usd.mean() / df.pred.mean()
print("Manually increasing predictions by {}%".format(round((ratio-1) * 100, 1)))
df['pred'] = df.pred * ratio
df['err'] = df.std_log_price_usd - df.pred
df['err'] = df.price_usd - df.pred
df['q'] = df.pred.rank() * 10 / len(df)
df['q'] = df.q.apply(lambda x: int(round(x)) )
df['pct_err'] = (df.price_usd / df.pred) - 1
df['pct_err'] = df.pct_err.apply(lambda x: min(1, x) )
len(df[(df.pct_err > -.9) & (df.pct_err < 1.0)])
df[[ 'pred','price_usd','err','pct_err' ]].sort_values('pct_err')
pe_mu = df.pct_err.mean()
pe_sd = df[ (df.pct_err > -.9) & (df.pct_err < 0.9) ].pct_err.std()
df['pred_price'] = df.pred.apply(lambda x: x*(1+pe_mu) )
df['pred_sd'] = df.pred * pe_sd
print(df.groupby('q')[['err','pred','pred_price','price_usd','std_log_price_usd']].mean())
df.err.mean()

# create an attributes dataframe
attributes = pd.DataFrame()
for f in features:
    cur = metadata[[ 'token_id', f, '{}_pct'.format(f) ]]
    cur.columns = [ 'token_id', 'value','rarity' ]
    cur['feature'] = f
    attributes = attributes.append(cur)
attributes.to_csv('./data/attributes.csv', index=False)

# create predictions for each NFT in the collection
test = metadata.copy()
test['std_timestamp'] = df.std_timestamp.max()
test = standardize_df(test, [c for c in pred_cols if not c in ['timestamp'] ], df)
test['pred'] = np.exp( (sd * model.predict(test[std_pred_cols].values)) + mu) * ratio
test['pred_price'] = test.pred.apply(lambda x: x*(1+pe_mu) )
test['pred_sd'] = test.pred * pe_sd
test['rk'] = test.pred_price.rank(ascending=0)
df[[ 'contract_address','token_id','block_timestamp','price_usd' ] + features].sort_values('block_timestamp', ascending=0).to_csv('data/sales.csv', index=False)
test[[ 'contract_address','token_id','rk','pred_price','pred_sd' ] + features].sort_values('pred_price').to_csv('data/pred_price.csv', index=False)
print(test[[ 'contract_address','token_id','pred_price','pred_sd' ]].sort_values('pred_price'))


##############################
#     Feature Importance     #
##############################
data = []
for c0 in std_pred_cols:
    if c0 in ['std_pct','std_timestamp']:
        continue
    f = re.split('_', c0)[1]
    v = re.split('_', c0)[2]
    r = metadata[metadata['{}_{}'.format(f, v)]==1]['{}_pct'.format(f)].values[0]
    avg = metadata['{}_pct'.format(f)].mean()
    avg_pct = df.pct.mean()
    pct_std = ((avg_pct * r / avg) - avg_pct) / df.pct.std()
    datum = [ c0, r ]
    for c1 in std_pred_cols:
        datum.append(1 if c1 == c0 else pct_std if c1 == 'std_pct' else 0)
    data += [ datum ]
importance = pd.DataFrame(data, columns=['feature','rarity']+std_pred_cols)
importance['std_timestamp'] = df.std_timestamp.max()
importance['pred'] = np.exp( (sd * model.predict(importance[std_pred_cols].values)) + mu)
importance = importance.sort_values('pred', ascending=0)
importance.head()[['feature','pred']]
importance['feature'] = importance.feature.apply(lambda x: re.sub('std_', '', x))
importance['value'] = importance.feature.apply(lambda x: re.split('_', x)[1])
importance['feature'] = importance.feature.apply(lambda x: re.split('_', x)[0])
mn = importance.groupby('feature').pred.min().reset_index().rename(columns={'pred':'baseline'})
importance = importance.merge(mn)
importance['pred_vs_baseline'] = importance.pred - importance.baseline
importance['pct_vs_baseline'] = (importance.pred / importance.baseline) - 1
importance[['feature','value','pred','pred_vs_baseline','pct_vs_baseline','rarity']].to_csv('./data/feature_values.csv', index=False)


###########################
#     Save NFT Images     #
###########################
def save_images():
    l = 1
    offset = 0
    data = []
    it = 0
    while l:
        if offset % 1000 == 0:
            print("#{}/{}".format(offset, 15000))
        contract_address = '0xc2c747e0f7004f9e8817db2ca4997657a7746928'
        r = requests.get('https://api.opensea.io/api/v1/assets?asset_contract_address={}&order_direction=asc&offset={}&limit=50'.format(contract_address, offset))
        assets = r.json()['assets']
        l = len(assets)
        for a in assets:
            data += [[ contract_address, a['token_id'], a['image_url'] ]]
        offset += 50
    opensea_data = pd.DataFrame(data, columns=['contract_address','token_id','image_url']).drop_duplicates()
    len(opensea_data)

    it = 0
    max_it = 9458
    for row in opensea_data.iterrows():
        it += 1
        if it % 100 == 0:
            print('#{}/{}'.format(it, len(opensea_data)))
        if it < max_it:
            continue
        row = row[1]
        urllib.request.urlretrieve(row['image_url'], './viz/www/img/{}.png'.format(row['token_id']))


