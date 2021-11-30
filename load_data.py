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
    traits = []
    l = 1
    it = 0
    offset = 0
    while l:
        if offset % 1000 == 0:
            print("#{}/{}".format(offset, 15000))
        contract_address = '0xc2c747e0f7004f9e8817db2ca4997657a7746928'
        r = requests.get('https://api.opensea.io/api/v1/assets?asset_contract_address={}&order_by=pk&order_direction=desc&offset={}&limit=50'.format(contract_address, offset))
        assets = r.json()['assets']
        l = len(assets)
        for a in assets:
            token_id = a['token_id']
            for t in a['traits']:
                traits += [[ contract_address, token_id, t['trait_type'], t['value']  ]]
            data += [[ contract_address, token_id, a['image_url'] ]]
        offset += 50
    opensea_data = pd.DataFrame(data, columns=['contract_address','token_id','image_url']).drop_duplicates()
    traits = pd.DataFrame(traits, columns=['contract_address','token_id','trait_type','trait_value']).drop_duplicates()
    a = set(range(opensea_data.token_id.min(), opensea_data.token_id.max()))
    b = set(opensea_data.token_id.unique())
    a.difference(b)
    len(opensea_data)
    traits = traits[traits.trait_type != 'Token ID']
    traits[traits.trait_type=='Set']
    traits['token_id'] = traits.token_id.astype(int)
    traits[traits.token_id==1]
    traits.groupby('token_id').contract_address.count()
    traits.to_csv('./data/hashmasks_traits.csv', index=False)
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


