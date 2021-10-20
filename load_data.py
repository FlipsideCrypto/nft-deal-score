import re
import json
import snowflake.connector
import numpy as np
import tensorflow as tf
from sklearn.ensemble import RandomForestRegressor
ctx = snowflake.connector.connect(
    user='USER',
    password='PASSWORD',
    account='ACCOUNT'
)
import pandas as pd

def clean_colnames(df):
	names = [ x.lower() for x in df.columns ]
	df.columns = names
	return(df)

def standardize_df(df, cols):
    for c in cols:
        mu = df[c].mean()
        sd = df[c].std()
        df['std_{}'.format(c)] = (df[c] - mu) / sd
    return(df)

def calculate_percentages(df):
    df = clean_features_df
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


#########################
#     Load Metadata     #
#########################
query = '''
SELECT
token_id
, contract_address
, token_metadata:collection_name as collection_name
, token_metadata:features as feature
FROM ethereum.nft_metadata
WHERE contract_address in ('0x059edd72cd353df5106d2b9cc5ab83a52287ac3a','0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270')
'''
metadata = ctx.cursor().execute(query)
metadata = pd.DataFrame.from_records(iter(metadata), columns=[x[0] for x in metadata.description])
metadata = clean_colnames(metadata)
metadata.head()


######################
#     Load Sales     #
######################
query = '''
SELECT
tx_id
, token_id
, block_timestamp
, contract_address
, price
, price_usd
FROM ethereum.nft_events
WHERE contract_address in ('0x059edd72cd353df5106d2b9cc5ab83a52287ac3a','0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270')
AND event_type = 'sale'
'''
sales = ctx.cursor().execute(query)
sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
sales = clean_colnames(sales)
sales.head()


#############################################
#     Select Collection With Most Sales     #
#############################################
df = sales.merge(metadata[[ 'token_id','contract_address','collection_name' ]])
df['num_sales'] = 1
df = df.groupby( ['collection_name','contract_address'] )[['price','num_sales']].sum().reset_index().sort_values('num_sales', ascending=0)
collection_name = df.collection_name.values[0]
print('Collection with most sales is {}'.format(collection_name))


######################################################
#     Create Dataframe with Requisite Collection     #
######################################################
data = metadata[metadata.collection_name == collection_name]
data = data[data.feature.notnull()]
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
data = pd.concat( [data.reset_index(drop=True), clean_features_df.reset_index(drop=True)], axis=1 )
data[data.pct.isnull()]

df = sales.merge(data)
# df = df.head(1)
# x = df.block_timestamp.values[0]
df['timestamp'] = df.block_timestamp.astype(int)
df = df[df.price_usd.notnull()]
df = df.reset_index(drop=True)
print('Training on {} sales'.format(len(df)))
df.sort_values('timestamp', ascending=0)[['tx_id','price_usd','pct']].head(20).values
df.sort_values('price', ascending=0).to_csv('~/Downloads/tmp3.csv', index=False)




pred_cols = [ 'pct', 'timestamp' ]
std_pred_cols = [ 'std_{}'.format(c) for c in pred_cols ]
df = standardize_df(df, pred_cols)
X = df[std_pred_cols].values
y = df.price_usd.values

model = tf.keras.models.Sequential([
    tf.keras.layers.Dense(10, activation='relu')
    , tf.keras.layers.Dense(5, activation='relu')
    , tf.keras.layers.Dense(1, activation='linear')
])
model.compile(loss=tf.keras.losses.MeanAbsoluteError(), optimizer='adam')
model.fit(X, y, epochs=100, validation_split=0.2)

df['pred'] = model.predict(df[std_pred_cols].values)
df['err'] = df.price_usd - df.pred
df['q'] = df.pct.rank() * 10 / len(df)
df['q'] = df.q.apply(lambda x: int(round(x)) )
df.groupby('q')[['err','pred','price_usd']].mean()

test = df[['pct','std_pct']].drop_duplicates()
test['std_timestamp'] = df.std_timestamp.max()
test['pred'] = model.predict(test[std_pred_cols].values)
test.sort_values('pct')
test.sort_values('pct').to_csv('~/Downloads/tmp.csv', index=False)




clf = RandomForestRegressor(min_samples_leaf=5)
df[df.pct.isnull()]
df[df.timestamp.isnull()]
pred_cols = [ 'pct', 'timestamp' ]
X = df[pred_cols].values
y = df.price_usd.values
clf.fit(X, y)
df['pred'] = clf.predict(X)
df['q'] = df.pred.rank() * 5 / len(df)
df['q'] = df.q.apply(lambda x: int(round(x)) )
df.groupby('q')[['price_usd','pred']].mean()
test = df[['pct']].drop_duplicates()
test['timestamp'] = df.timestamp.max()
test['pred'] = clf.predict(test[pred_cols].values)

test.sort_values('pct').to_csv('~/Downloads/tmp.csv', index=False)
t = df.timestamp.quantile(.9)
df[df.timestamp>=t].price_usd.quantile(.8)
df[df.timestamp>=t][['pred','price_usd']].mean()
df[df.timestamp>=t].to_csv('~/downloads/tmp2.csv', index=False)

df['q'] = df.timestamp.rank() * 100 / len(df)
df['q'] = df.q.apply(lambda x: int(round(x)) )
df.groupby('q').price.mean()

