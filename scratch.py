import os
import pandas as pd

os.chdir('/Users/kellenblumberg/git/nft-deal-score')

COLLECTION = 'Stoned Ape Crew'

sales = pd.read_csv('./data/sales.csv')
sales = pd.read_csv('./data/model_sales.csv')
sorted(sales.collection.unique())
# sales = sales[sales.exclude == 0]
metadata = pd.read_csv('./data/metadata.csv')
metadata = metadata[metadata.collection == COLLECTION]
metadata[metadata.feature_name=='Role'].groupby('feature_value').token_id.count()
sorted(metadata.feature_name.unique())
sales = sales[sales.collection == COLLECTION]
features = sorted(metadata.feature_name.unique())
metadata = metadata.pivot( ['collection','token_id'], ['feature_name'], ['feature_value'] ).reset_index()
metadata.columns = [ 'collection','token_id' ] + features
sales['token_id'] = sales.token_id.astype(int)
metadata['token_id'] = metadata.token_id.astype(int)
df = sales.merge(metadata)
# df.sort_values('sale_date', ascending=0).head()
df = df.fillna('None')
df['id'] = len(df)
df['rel_price_0'] = (df.price - df.mn_20).apply(lambda x: max(0, x))
df['rel_price_1'] = (df.price / df.mn_20).apply(lambda x: max(0, x-1))
df[ (df.rel_price_0.notnull()) ].to_csv('./data/tableau_data.csv', index=False)

