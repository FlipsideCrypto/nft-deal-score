import os
import re
import json
import requests
import numpy as np
import pandas as pd
import urllib.request
#import tensorflow as tf
#import snowflake.connector
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, RidgeCV



#########################
#     Connect to DB     #
#########################
# =============================================================================
# with open('snowflake.pwd', 'r') as f:
#     pwd = f.readlines()[0].strip()
# with open('snowflake.usr', 'r') as f:
#     usr = f.readlines()[0].strip()
# 
# ctx = snowflake.connector.connect(
#     user=usr,
#     password=pwd,
#     account='vna27887.us-east-1'
# )
# 
# =============================================================================
###################################
#     Define Helper Functions     #
###################################
def clean_colnames(df):
	names = [ x.lower() for x in df.columns ]
	df.columns = names
	return(df)

def standardize_df(df, cols, usedf=None):
    for c in cols:
        print(c)
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



#########################
#     Load Metadata     #
#########################

def load_metadata():
    # query = '''
    # SELECT
    # token_id
    # , contract_address
    # , token_metadata:collection_name as collection_name
    # , token_metadata:character as character
    # , token_metadata:eyeColor as eyeColor
    # , token_metadata:ipfs_hash as ipfs_hash
    # , token_metadata:item as item
    # , token_metadata:mask as mask
    # , token_metadata:skinColor as skinColor
    # FROM ethereum.nft_metadata
    # WHERE contract_address in ({})
    # '''.format( contract_address_query )
    # metadata_0 = ctx.cursor().execute(query)
    # metadata_0 = 
    # metadata_0 = pd.DataFrame.from_records(iter(metadata_0), columns=[x[0] for x in metadata_0.description])
    # metadata_0 = clean_colnames(metadata_0)
    # metadata_0.head()
    api_0 = 'https://api.flipsidecrypto.com/api/v2/queries/543deca7-3a3e-4f25-bb51-99e5ba5536ec/data/latest'
    response = requests.get(api_0)
    metadata_0 = pd.DataFrame(response.json())
    metadata_0 = clean_colnames(metadata_0)

    # query = '''
    # SELECT
    # token_id
    # , contract_address
    # , token_metadata:traits:backgrounds as backgrounds
    # , token_metadata:traits:hair as hair
    # , token_metadata:traits:species as species
    # , token_metadata:traits:suits as suits
    # , token_metadata:traits:jewelry as jewelry
    # , token_metadata:traits:headware as headware
    # , token_metadata:traits:glasses as glasses
    # FROM terra.nft_metadata
    # WHERE contract_address in ('terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k')
    # '''
    # metadata_1 = ctx.cursor().execute(query)
    # metadata_1 = pd.DataFrame.from_records(iter(metadata_1), columns=[x[0] for x in metadata_1.description])
    # metadata_1 = clean_colnames(metadata_1)
    # metadata_1.head()
    api_1 = 'https://api.flipsidecrypto.com/api/v2/queries/e02cb537-f451-49d4-a32c-f7b082a54c79/data/latest'
    response = requests.get(api_1)
    metadata_1 = pd.DataFrame(response.json())
    metadata_1 = clean_colnames(metadata_1)


    
    metadata = {
        'Hashmasks': metadata_0
        , 'Galactic Punks': metadata_1
    }

    return(metadata)



######################
#     Load Sales     #
######################
def load_sales_data():
    # query = '''
    # WITH mints AS (
    #     SELECT DISTINCT tx_id
    #     FROM ethereum.nft_events
    #     WHERE contract_address in ({})
    #     AND event_type = 'mint'
    # )
    # SELECT
    # e.tx_id
    # , token_id
    # , block_timestamp
    # , contract_address
    # , price
    # , price_usd
    # FROM ethereum.nft_events e
    # LEFT JOIN mints m ON m.tx_id = e.tx_id
    # WHERE contract_address in ({})
    # AND event_type = 'sale'
    # AND m.tx_id IS NULL
    # AND price_usd > 0
    # '''.format( contract_address_query, contract_address_query )
    # sales_0 = ctx.cursor().execute(query)
    # sales_0 = pd.DataFrame.from_records(iter(sales_0), columns=[x[0] for x in sales_0.description])
    # sales_0 = clean_colnames(sales_0)
    # sales_0.head()
    
    api_0 = 'https://api.flipsidecrypto.com/api/v2/queries/53438a14-5f4e-420b-a0d6-39e157557da0/data/latest'
    response = requests.get(api_0)
    sales_0 = pd.DataFrame(response.json())
    sales_0 = clean_colnames(sales_0)
    
    sales_0.tx_id.values[0]
    sales_0['collection'] = 'Hashmasks'
    sales_0 = sales_0.sort_values('block_timestamp')
    #sales_0['mn_20'] = sales_0.groupby('collection').price_usd.shift(1).rolling(20).min()


    # query = '''
    # SELECT 
    # tx_id
    # , msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:token_id AS token_id
    # , block_timestamp
    # , msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:contract_addr AS contract_address
    # , NULL AS price
    # , msg_value:execute_msg:execute_order:order:order:taker_asset:amount::decimal/pow(10,6) AS price_usd
    # FROM terra.msgs 
    # WHERE (
    #     msg_value:contract::string = 'terra1eek0ymmhyzja60830xhzm7k7jkrk99a60q2z2t' 
    #     AND	msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:contract_addr::string = 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k'
    #     AND	tx_status = 'SUCCEEDED'
    #     AND	msg_value:execute_msg:execute_order IS NOT NULL
    #     AND price_usd > 0
    # )
    # '''
    # sales_1 = ctx.cursor().execute(query)
    # sales_1 = pd.DataFrame.from_records(iter(sales_1), columns=[x[0] for x in sales_1.description])
    # sales_1 = clean_colnames(sales_1)
    
    api_1 = 'https://api.flipsidecrypto.com/api/v2/queries/4463197b-002c-48cc-bba6-634e717c7b62/data/latest'
    response = requests.get(api_1)
    sales_1 = pd.DataFrame(response.json())
    sales_1 = clean_colnames(sales_1)
    
    sales_1['collection'] = 'Galactic Punks'
    sales_1.head()
    sales_1 = sales_1.sort_values('block_timestamp')
    #sales_1['mn_20'] = sales_1.groupby('collection').price_usd.shift(1).rolling(20).min()

    sales = {
        'Hashmasks': sales_0
        , 'Galactic Punks': sales_1
    }
    return(sales)

#############################################
#     Select Collection With Most Sales     #
#############################################
def pick_most_sales(using_art_blocks, sales, metadata):
    if False and using_art_blocks:
        df = sales.merge(metadata[[ 'token_id','contract_address','collection_name' ]])
        df['num_sales'] = 1
        df = df.groupby( ['collection_name','contract_address'] )[['price','num_sales']].sum().reset_index().sort_values('num_sales', ascending=0)
        collection_name = df.collection_name.values[0]
        print('Collection with most sales is {}'.format(collection_name))

############################
## Prepare model features ##
############################
def model_data(using_art_blocks, metadata, sales, collection=None):
    # pred_cols = {}
    if False and using_art_blocks:
        metadata = metadata[metadata.collection_name == collection]
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
        collection_features = {
        'Hashmasks': [ 'character','eyecolor','item','mask','skincolor' ]
        , 'Galactic Punks': [ 'backgrounds','hair','species','suits','jewelry','headware','glasses' ]
        }
        
        alldata = {}
        dummy_features = {}
        for p in metadata.keys():
            features = collection_features[p]
            cur = metadata[p]
            cur = cur.dropna(subset=features)
            for f in features:
                cur[f] = cur[f].apply(lambda x: re.sub("\"", "", x ))
            cur = calculate_percentages( cur, features )
            dummies = pd.get_dummies(cur[features])
            #feature_cols = dummies.columns ## not used anywhere
            cur = pd.concat([ cur.reset_index(drop=True), dummies.reset_index(drop=True) ], axis=1)
            metadata[p] = cur
            # pred_cols[p] = ['pct','timestamp','mn_20','log_mn_20'] + list(dummies.columns)
            
            p_metadata = metadata[p]    
            p_sales = sales[p]

            p_sales['token_id'] = p_sales.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
            p_metadata['token_id'] = p_metadata.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
            p_sales['contract_address'] = p_sales.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
            p_metadata['contract_address'] = p_metadata.token_id.apply(lambda x: re.sub("\"", "", str(x)) )
            p_metadata = p_metadata.sort_values(by=['token_id'], ascending=True)

            ## Create token level unique sales data
            p_sales_unique = pd.DataFrame({'sale_count': p_sales.groupby(['token_id']).size()})
            p_sales_unique['price_usd_median'] = p_sales.groupby('token_id').price_usd.median()
            p_sales_unique['price_usd_min'] = p_sales.groupby('token_id').price_usd.min()
            p_sales_unique['price_usd_max'] = p_sales.groupby('token_id').price_usd.max()
            
            if p_sales['price'].isnull().all():
                print("*** " + p + " prices are all None! Use price USD! ***")
            else:
                p_sales_unique['price_median'] = p_sales.groupby('token_id').price.median()
                p_sales_unique['price_min'] = p_sales.groupby('token_id').price.min()
                p_sales_unique['price_max'] = p_sales.groupby('token_id').price.max()
            
            alldata[p] = p_metadata.merge(p_sales_unique, on='token_id', how='left')
            alldata[p].set_index('token_id', inplace = True)
            
            dummy_features[p] = dummies.columns

    
    return alldata, dummy_features


    
    



