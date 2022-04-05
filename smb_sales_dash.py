import os
from time import sleep
import urllib.request
import pandas as pd
import snowflake.connector

os.chdir('/Users/kellenblumberg/git/nft-deal-score')

from utils import merge
from load_data import clean_colnames

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

def load_smb_images():
	tokens = pd.read_csv('./data/tokens.csv')
	tokens = tokens[tokens.collection == 'Solana Monkey Business']
	tokens['token_id'] = tokens.token_id.astype(int)
	tokens = tokens.sort_values('token_id')
	seen = []
	for fname in os.listdir('/Users/kellenblumberg/Documents/My Tableau Repository/Shapes/smb/'):
		token_id = int(fname[:-4])
		seen.append(token_id)
	seen = sorted(seen)
	# for row in tokens.iterrows():
	for row in tokens[tokens.token_id>=2500].iterrows():
		row = row[1]
		token_id = int(row['token_id'])
		print(token_id)
		if token_id in seen:
			continue
		# urllib.request.urlretrieve(row['image_url'], '/Users/kellenblumberg/Documents/My\ Tableau\ Repository/Shapes/smb/{}.png'.format(row['token_id']))
		# urllib.request.urlretrieve(row['image_url'], '~/Downloads/{}.png'.format(row['token_id']))
		# urllib.request.urlretrieve(row['image_url'], '{}.png'.format(row['token_id']))
		try:
			urllib.request.urlretrieve(row['image_url'], '/Users/kellenblumberg/Documents/My Tableau Repository/Shapes/smb/{}.png'.format(token_id))
		except:
			sleep(10)
			print('Re-trying once...')
			try:
				urllib.request.urlretrieve(row['image_url'], '/Users/kellenblumberg/Documents/My Tableau Repository/Shapes/smb/{}.png'.format(token_id))
			except:
				sleep(50)
				print('Re-trying once...')
				urllib.request.urlretrieve(row['image_url'], '/Users/kellenblumberg/Documents/My Tableau Repository/Shapes/smb/{}.png'.format(token_id))
	for i in range(1, 5001):
		src = '/Users/kellenblumberg/Documents/My Tableau Repository/Shapes/smb/{}.png'.format(i)
		dst = '/Users/kellenblumberg/Documents/My Tableau Repository/Shapes/smb/{}.png'.format(str(i).zfill(4))
		os.rename(src, dst)



mints = pd.read_csv('./data/solana_rarities.csv')
mints = mints[mints.collection == 'smb']
# mints = pd.read_csv('./data/solana_mints.csv')
sorted(mints.collection.unique())
metadata = pd.read_csv('./data/metadata.csv')
pred_price = pd.read_csv('./data/pred_price.csv')
collection = 'Solana Monkey Business'
metadata = metadata[metadata.collection == collection]
metadata['token_id'] = metadata.token_id.astype(int)
mints['token_id'] = mints.token_id.astype(int)
pred_price = pred_price[pred_price.collection == collection]

p = metadata.pivot( ['collection','token_id'], ['feature_name'], ['feature_value'] ).reset_index()
p.columns = [ 'collection','token_id' ] + sorted(metadata.feature_name.unique())
cols = ['collection', 'token_id', 'Attribute Count', 'Background', 'Clothes', 'Ears', 'Eyes', 'Hat', 'Mouth', 'Type', 'nft_rank']
p = p[cols]

with open('./sql/smb_sales.sql', 'r') as f:
	query = f.read()

sales = ctx.cursor().execute(query)
sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
sales = clean_colnames(sales)
sales.head()
sales[sales.mint != 'So11111111111111111111111111111111111111112']
sales = sales[sales.mint != 'So11111111111111111111111111111111111111112']
sales = sales[sales.mint.notnull()]
sales['marketplace'] = sales.program_id.apply(lambda x: 'SMB Marketplace' if x == 'J7RagMKwSD5zJSbRQZU56ypHUtux8LRDkUpAPSKH4WPp' else 'Magic Eden' )
sales.head()
sales['date'] = sales.block_timestamp.apply(lambda x: str(x)[:10] )

sales = sales.sort_values('block_timestamp')
sales['floor'] = sales.price.rolling(10, 5).quantile(.05)
sales['floor'] = sales.floor.rolling(5, 1, True).mean()
# sales['tmp'] = sales.price.rolling(5, 5).quantile(0).reset_index(0, drop=True)
sales.head(20)[['block_timestamp','price','floor']]
sales.tail(20)[['block_timestamp','price','floor']]
sales.groupby('date').tail(1)[['date','floor']].tail(20)
# sales.tail(6)[['block_timestamp','price','floor','tmp']]
# sales['mn_20'] = sales.mn_20.rolling(5).min().reset_index(0, drop=True)

df = merge( sales, mints[['mint_address','token_id']].rename(columns={'mint_address':'mint'}), ensure = True )
df = merge( df, p, ensure = True )
a = p[-(p.token_id.isin(df.token_id.unique()))]
df = df.append(a)

df = merge( df, pred_price[[ 'collection','token_id','rk' ]].rename(columns={'rk':'nds_rank'}), ensure = True )
len(df.token_id.unique())

df = df.sort_values('date')
df['price'] = df.price.apply(lambda x: round(x, 1) )
df['floor'] = df.apply(lambda x: round(min(x['floor'], x['price']), 1), 1 )
df['vs_floor'] = df.price - df.floor
df['token_id'] = df.token_id.astype(int)
df['date'] = df.date.apply(lambda x: str(x)[:10])
df.sort_values('block_timestamp', ascending=0).head()[['block_timestamp','token_id','price']]
df.to_csv('./tableau/data/smb_dash_sales.csv', index=False)

