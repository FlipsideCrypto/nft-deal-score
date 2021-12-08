import os
import requests
import pandas as pd
import urllib.request

os.chdir('/Users/kellenblumberg/git/nft-deal-score')

######################################
#     Grab Data From OpenSea API     #
######################################
def load_api_data():
    data = []
    traits_data = []
    contract_address = '0x60e4d786628fea6478f785a6d7e704777c86a7c6'
    for o in [ 'asc', 'desc' ]:
        l = 1
        it = 0
        offset = 0
        while l and offset <= 10000:
            if offset % 1000 == 0:
                print("#{}/{}".format(offset, 20000))
            r = requests.get('https://api.opensea.io/api/v1/assets?asset_contract_address={}&order_by=pk&order_direction={}&offset={}&limit=50'.format(contract_address, o, offset))
            assets = r.json()['assets']
            l = len(assets)
            for a in assets:
                token_id = a['token_id']
                for t in a['traits']:
                    traits_data += [[ contract_address, token_id, t['trait_type'], t['value']  ]]
                data += [[ contract_address, token_id, a['image_url'] ]]
            offset += 50
    opensea_data = pd.DataFrame(data, columns=['contract_address','token_id','image_url']).drop_duplicates()
    len(opensea_data.token_id.unique())
    traits = pd.DataFrame(traits, columns=['contract_address','token_id','trait_type','trait_value']).drop_duplicates()
    a = set(range(opensea_data.token_id.min(), opensea_data.token_id.max()))
    b = set(opensea_data.token_id.unique())
    a.difference(b)
    len(opensea_data)
    sorted(traits.trait_type.unique())
    traits = traits[(traits.trait_type != 'Token ID')]
    traits['token_id'] = traits.token_id.astype(int)
    traits.to_csv('./data/bayc_traits.csv', index=False)
    len(traits.token_id.unique())
    opensea_data['token_id'] = opensea_data.token_id.astype(int)
    opensea_data.token_id.max()
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


