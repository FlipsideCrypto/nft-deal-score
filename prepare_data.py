import os
import pandas as pd

os.chdir('/Users/kellenblumberg/git/nft-deal-score')

from solana_model import get_sales
from scrape_sol_nfts import clean_name

m_df = pd.read_csv('./data/metadata.csv')

solana_rarities = pd.read_csv('./data/solana_rarities.csv')
lp_ranks = pd.read_csv('./data/lp_ranks.csv')
gp_ranks = pd.read_csv('./data/gp_ranks.csv')

rarities = solana_rarities.append(lp_ranks).append(gp_ranks)
rarities = rarities[[ 'collection','token_id','nft_rank' ]]
rarities['collection'] = rarities.collection.apply(lambda x: clean_name(x) )
rarities[ (rarities.collection == 'Solana Monkey Business') & (rarities.token_id == 903) ]
rarities.loc[ (rarities.collection == 'Solana Monkey Business') & (rarities.token_id == 903) , 'nft_rank' ] = 18
rarities['adj_nft_rank_0'] = rarities.nft_rank.apply(lambda x: (x+1) ** -0.2 )
rarities['adj_nft_rank_1'] = rarities.nft_rank.apply(lambda x: (x+1) ** -0.9 )
rarities['adj_nft_rank_2'] = rarities.nft_rank.apply(lambda x: (x+1) ** -1.4 )

print(solana_rarities.groupby('collection').token_id.count())
print(rarities.groupby('collection').token_id.count())

# m_df[ m_df.feature_name.isin(['nft_rank','adj_nft_rank_0','adj_nft_rank_1']) ].groupby('collection').token_id.count()
m_df = m_df[ -m_df.feature_name.isin(['nft_rank','adj_nft_rank_0','adj_nft_rank_1','adj_nft_rank_2']) ]

m_df['token_id'] = m_df['token_id'].astype(str)
m_df = m_df.drop_duplicates()
m_df = m_df.fillna('None')
print(m_df[(m_df.token_id=='1') & (m_df.collection == 'Solana Monkey Business')])
print(m_df[(m_df.token_id=='10') & (m_df.collection == 'Aurory')])

sorted(m_df.feature_name.unique())

c = 'Solana Monkey Business'
for c in m_df.collection.unique():
    cur = m_df[m_df.collection == c]
    base = cur[[ 'collection','token_id','chain' ]].drop_duplicates()
    for f in cur.feature_name.unique():
        exists = set(cur[cur.feature_name==f].token_id.unique())
        missing = sorted(list(set(base.token_id).difference(exists)))
        a = base[ base.token_id.isin(missing) ]
        a['feature_name'] = f
        a['feature_value'] = 'None'
        m_df = m_df.append(a)

print(m_df[(m_df.token_id=='1') & (m_df.collection == 'Solana Monkey Business')])
print(m_df[(m_df.token_id=='10') & (m_df.collection == 'Aurory')])

for c in [ 'nft_rank','adj_nft_rank_0','adj_nft_rank_1' ]:
    cur = rarities[[ 'collection','token_id',c ]].rename(columns={c: 'feature_value'})
    cur['feature_name'] = c
    m_df = m_df[ m_df.feature_name != c ]
    m_df = m_df.append(cur)


m_df = m_df.drop_duplicates(subset=['collection','token_id','feature_name'], keep='last')

print(m_df[(m_df.token_id=='1') & (m_df.collection == 'Solana Monkey Business')])
print(m_df[(m_df.token_id=='10') & (m_df.collection == 'Aurory')])

m_df['feature_value'] = m_df.feature_value.apply(lambda x: x.strip() if type(x) == str else x )
m_df.to_csv('./data/metadata.csv', index=False)


g = m_df[['collection','token_id']].drop_duplicates().groupby('collection').token_id.count().reset_index()
a = m_df.groupby('collection').token_id.count().reset_index().rename(columns={'token_id':'atts'})
g = g.merge(a)
g['rat'] = g.atts / g.token_id
print(g)


m_df.collection.unique()
m_df['chain'] = m_df.collection.apply(lambda x: 'Terra' if x in ['LunaBulls','Galactic Punks','Levana Dragon Eggs'] else 'Solana' )

m_df.to_csv('./data/metadata.csv', index=False)

d = {
    'nft_rank': 'NFT Rank'
    , 'adj_nft_rank_0': 'NFT Rank'
}

s_df = get_sales()
s_df['token_id'] = s_df.token_id.astype(str)
rarities['token_id'] = rarities.token_id.astype(str)

df = s_df.merge( rarities )
print(df.groupby('collection').token_id.count())
print(s_df.groupby('collection').token_id.count())
s_df[s_df.collection]
print(s_df[['collection','token_id']].drop_duplicates().groupby('collection').token_id.count())
print(df[['collection','nft_rank']].drop_duplicates().groupby('collection').nft_rank.count())

df['rel_price_0'] = df.price - df.mn_20
df['rel_price_1'] = df.price / df.mn_20
df[[ 'collection','nft_rank','rel_price_0','rel_price_1' ]].dropna().to_csv('./data/tableau_sales_vs_rank.csv', index=False)