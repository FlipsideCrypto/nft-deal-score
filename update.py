import os
from selenium import webdriver

os.chdir('/Users/kellenblumberg/git/nft-deal-score')
os.environ['PATH'] += os.pathsep + '/Users/kellenblumberg/shared/'

import scrape_sol_nfts as ssn
import load_data as ld
import solana_model as sm

browser = webdriver.Chrome()

# sales = pd.read_csv('./data/sales.csv')
# pred_price = pd.read_csv('./data/pred_price.csv').sort_values('token_id')
# pred_price['rank'] = pred_price.groupby('collection').pred_price.rank(ascending=0)
# sales = sales.merge(pred_price[['collection','token_id','rank']])
# sales = sales[ sales.collection.isin(['Solana Monkey Business','Degen Apes','Aurory','Pesky PenguinsPesky Penguins','Thugbirdz']) ]
# sales = sales[sales['rank']<=10].sort_values('price', ascending=0)
# # sales = sales.sort_values('price', ascending=0).groupby('collection').head(3)[['collection','sale_date','token_id','price','rank']].sort_values('collection')
# d = {
#     'Solana Monkey Business':	140,
#     'Aurory':	18.5,
#     'Degen Apes':	34,
#     'Thugbirdz':	40,
# }
# sales['current_floor'] = sales.collection.apply(lambda x: d[x] )
# sales['floor_ratio'] = sales.price / sales.current_floor
# sales.to_csv('~/Downloads/tmp.csv', index=False)

# update sales
ssn.scrape_recent_smb_sales(browser)
ssn.scrape_recent_sales()
ld.add_terra_sales()

# update listings
ssn.scrape_randomearth(browser)
ssn.scrape_listings(browser)

# update model
ssn.convert_collection_names()
sm.train_model(True, False)
sm.train_model(False, False)
# sm.train_model(False, True)
