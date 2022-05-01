import re
import json
import requests
import pandas as pd

from time import sleep

def scrape_opensea():
	headers = {
		'Content-Type': 'application/json'
		, 'X-API-KEY': '2b7cbb0ebecb468bba431aefb8dbbebe'
	}
	collections = {
		'BAYC': '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'
	}
	for collection, contract_address in collections.items():
		data = []
		cursor = ''
		for i in range(0, 10000, 30):
			# print(i)
			sleep(1)
			token_ids = '&token_ids='.join([ str(x) for x in range(i, i + 30)])
			url = 'https://api.opensea.io/wyvern/v1/orders?asset_contract_address={}&bundled=false&include_bundled=false&token_ids={}&limit=50&side=1&order_by=created_date&order_direction=desc'.format(contract_address, token_ids)
			r = requests.get(url, headers=headers)
			j = r.json()
			for o in j['orders']:
				if 'current_price' in o.keys() and  'asset' in o.keys() and 'token_id' in o['asset'].keys():
					token_id = int(o['asset']['token_id'])
					price = float(o['current_price']) / (10**18)
					data += [[ collection, token_id, price ]]
	df = pd.DataFrame(data, columns=['collection','token_id','price'])
	df = df.sort_values(['collection','price'])
	q = df.groupby('collection').price.quantile(.1).rename(columns={'price':'q'})
	df = df[-((df.collection == 'BAYC') & (df.price < 50))]
	df.head(20)
	return(df)