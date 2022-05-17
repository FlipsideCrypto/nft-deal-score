import re
# import os
# import json
# import math
# import requests
import pandas as pd
# import urllib.request
import snowflake.connector


# os.chdir('/Users/kellenblumberg/git/nft-deal-score')

# from solana_model import just_float
# from utils import clean_name, clean_token_id, format_num


clean_names = {
	'aurory': 'Aurory'
	,'thugbirdz': 'Thugbirdz'
	,'smb': 'Solana Monkey Business'
	,'degenapes': 'Degen Apes'
	,'peskypenguinclub': 'Pesky Penguins'
	,'meerkatmillionaires': 'Meerkat Millionaires'
	,'boryokudragonz': 'Boryoku Dragonz'
	,'degods': 'DeGods'
	,'lunabulls': 'LunaBulls'
	,'boredapekennelclub': 'BAKC'
	,'boredapeyachtclub': 'BAYC'
	,'mutantapeyachtclub': 'MAYC'
	,'bakc': 'BAKC'
	,'bayc': 'BAYC'
	,'mayc': 'MAYC'
	,'solgods': 'SOLGods'
	,'meerkatmillionairescc': 'Meerkat Millionaires'
	,'stonedapecrew': 'Stoned Ape Crew'
}

def clean_name(name):
	x = re.sub('-', '', name).lower()
	x = re.sub(' ', '', x).lower()
	if x in clean_names.keys():
		return(clean_names[x])
	name = name.title()
	name = re.sub('-', ' ', name)
	name = re.sub(' On ', ' on ', name)
	name = re.sub('Defi ', 'DeFi ', name)
	return(name)

#########################
#     Connect to DB     #
#########################
def get_ctx(usr, pwd):
	# with open('snowflake.pwd', 'r') as f:
	# 	pwd = f.readlines()[0].strip()
	# with open('snowflake.usr', 'r') as f:
	# 	usr = f.readlines()[0].strip()

	ctx = snowflake.connector.connect(
		user=usr,
		password=pwd,
		account='vna27887.us-east-1'
	)
	return(ctx)

def clean_colnames(df):
	names = [ x.lower() for x in df.columns ]
	df.columns = names
	return(df)

def clean_token_id(df, data_folder):
	tokens = pd.read_csv(data_folder+'nft_deal_score_tokens.csv')
	df['collection'] = df.collection.apply(lambda x: clean_name(x))
	df['token_id'] = df.token_id.apply(lambda x: re.sub('"', '', x) if type(x)==str else x )
	df['tmp'] = df.token_id.apply(lambda x: x[:10] )
	tokens['tmp'] = tokens.token_id.apply(lambda x: str(x)[:10] )
	df = df.merge(tokens[['collection','tmp','clean_token_id']], how='left', on=['collection','tmp'])
	df['token_id'] = df.clean_token_id.fillna(df.token_id)
	df['token_id'] = df.token_id.astype(int)
	del df['tmp']
	del df['clean_token_id']
	return(df)

def add_sales(query, usr, pwd, do_clean_token_id = False, data_folder = '/rstudio-data/'):
	fname = data_folder+'nft_deal_score_sales.csv'
	ctx = get_ctx(usr, pwd)
	sales = ctx.cursor().execute(query)
	sales = pd.DataFrame.from_records(iter(sales), columns=[x[0] for x in sales.description])
	print('Loaded {} sales'.format(len(sales)))
	sales = clean_colnames(sales)
	sales['collection'] = sales.collection.apply(lambda x: clean_name(x) )
	sales['sale_date'] = sales.sale_date.apply(lambda x: str(x)[:19] )
	sorted(sales.collection.unique())

	if do_clean_token_id:
		sales['token_id'] = sales.token_id.apply(lambda x: str(int(float(x))) )
		sales = clean_token_id(sales, data_folder)

	m = sales[[ 'tx_id','collection','token_id','sale_date','price' ]]
	old = pd.read_csv(fname)
	sorted(old.collection.unique())
	old['collection'] = old.collection.apply(lambda x: clean_name(x))
	old[old.tx_id.isnull()].groupby('collection').sale_date.count()
	old[old.token_id.isnull()].groupby('collection').sale_date.count()
	go = old.groupby('collection').token_id.count().reset_index().rename(columns={'token_id':'n_old'})
	l0 = len(old)
	app = old[old.collection.isin(m.collection.unique())].append(m)
	app = app[ app.price > 0 ]
	app['tmp'] = app.apply(lambda x: x['collection']+str(int(float(x['token_id'])))+x['sale_date'][:10], 1 )
	if len(app[app.tx_id.isnull()]):
		app['null_tx'] = app.tx_id.isnull().astype(int)
		app = app.sort_values('null_tx', ascending=1)
		app = app.drop_duplicates(subset=['tmp'], keep='first')
		app['tx_id'] = app.tx_id.fillna(app.tmp)
	else:
		app = app.drop_duplicates(subset=['tx_id'])
	old = old[-old.collection.isin(app.collection.unique())]
	old = old.append(app)

	old = old[[ 'collection','token_id','sale_date','price','tx_id' ]]

	# check changes
	l1 = len(old)
	gn = old.groupby('collection').token_id.count().reset_index().rename(columns={'token_id':'n_new'})
	g = gn.merge(go, how='outer', on=['collection']).fillna(0)
	g['dff'] = g.n_new - g.n_old
	g = g[g.dff != 0].sort_values('dff', ascending=0)
	print(g)
	print('Added {} sales'.format(l1 - l0))
	# old.sort_values('sale_date').tail(11)
	old.to_csv(fname, index=False)

def add_solana_sales(usr, pwd, data_folder = '/rstudio-data/'):
	print('Adding Solana sales...')

	query = '''
		SELECT tx_id
		, s.mint
		, m.project_name AS collection
		, s.block_timestamp AS sale_date
		, m.token_id
		, sales_amount AS price
		FROM solana.fact_nft_sales s
		JOIN solana.dim_nft_metadata m ON LOWER(m.mint) = LOWER(s.mint)
		WHERE block_timestamp >= CURRENT_DATE - 14
		AND m.project_name IN (
			'Astrals',
			'Aurory',
			'Cets On Creck',
			'Catalina Whale Mixer',
			'DeFi Pirates',
			'DeGods',
			'Degen Apes',
			'Meerkat Millionaires',
			'Okay Bears',
			'Pesky Penguins',
			'SOLGods',
			'Solana Monkey Business',
			'Stoned Ape Crew',
			'Thugbirdz'
		)
	'''
	add_sales(query, usr, pwd, False, data_folder)

def add_ethereum_sales(usr, pwd, data_folder = '/rstudio-data/'):
	print('Adding Ethereum sales...')

	query = '''
		SELECT project_name AS collection
		, token_id::int AS token_id
		, block_timestamp AS sale_date
		, price
		, tx_id
		FROM ethereum.nft_events
		WHERE project_name IN (
			'BoredApeYachtClub'
			, 'MutantApeYachtClub'
			, 'BoredApeKennelClub'
		)
		AND price IS NOT NULL
		AND block_timestamp >= CURRENT_DATE - 14
	'''
	add_sales(query, usr, pwd, False, data_folder)

def add_terra_sales(usr, pwd, data_folder = '/rstudio-data/'):
	print('Adding Terra sales')
	query = '''
		WITH 
		RE_events AS (
			SELECT
				block_timestamp,
				tx_id,
				event_attributes
			FROM
				terra.msg_events
			WHERE event_attributes:action = 'execute_orders'
				AND event_type = 'from_contract'
				AND tx_status = 'SUCCEEDED'
				AND block_timestamp >= CURRENT_DATE - 14
		),

		RE_takers AS (
			SELECT DISTINCT
				tx_id,
				msg_value:sender as taker
			FROM
				terra.msgs
			WHERE
				tx_id IN (SELECT DISTINCT tx_id FROM RE_events)
				AND block_timestamp >= CURRENT_DATE - 14
		),
		allSales AS 
		(
			SELECT 
			block_timestamp AS sale_date,
			tx_id,
			platform,
			nft_from,
			nft_to,
			nft_address,
			CASE nft_address
				WHEN 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2' THEN 'LunaBulls'
				WHEN 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k' THEN 'Galactic Punks'
				WHEN 'terra1vhuyuwwr4rkdpez5f5lmuqavut28h5dt29rpn6' THEN 'Levana Dragons'
				WHEN 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' THEN 'Levana Dust'
				WHEN 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' THEN 'Levana Eggs'
				WHEN 'terra14gfnxnwl0yz6njzet4n33erq5n70wt79nm24el' THEN 'Levana Loot'
				WHEN 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' THEN 'Levana Meteors'
				WHEN 'terra13nccm82km0ttah37hkygnvz67hnvkdass24yzv' THEN 'Galactic Angels'
				ELSE nft_address 
			END AS collection,
			amount AS price,
			denom,
			tokenid AS token_id
			FROM (
				SELECT
				block_timestamp,
				tx_id,
				'Random Earth' as platform,
				action,
				IFF(action = 'SELL', maker, taker) as nft_from,
				IFF(action = 'ACCEPT BID', maker, taker) as nft_to,
				nft_address,
				amount,
				denom,
				tokenid
				FROM (
					SELECT
						block_timestamp,
						e.tx_id,
						action,
						maker,
						taker,
						nft_address,
						amount,
						denom,
						tokenid
					FROM (
						SELECT 
							block_timestamp,
							tx_id,
							IFF(event_attributes:order:order:maker_asset:info:nft is not null, 'SELL', 'ACCEPT BID') as action,
							LISTAGG(CHR(F.VALUE)) WITHIN GROUP (ORDER BY F.INDEX) as maker,
							IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:maker_asset:info:nft:contract_addr, event_attributes:order:order:taker_asset:info:nft:contract_addr)::string as nft_address,
							IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:taker_asset:amount, event_attributes:order:order:maker_asset:amount) / 1e6 as amount,
							IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:taker_asset:info:native_token:denom, event_attributes:order:order:maker_asset:info:native_token:denom)::string as denom,
									IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:maker_asset:info:nft:token_id, event_attributes:order:order:taker_asset:info:nft:token_id) as tokenid
						FROM 
							RE_events e,
							LATERAL FLATTEN(input => event_attributes:order:order:maker) F
						GROUP BY
							block_timestamp,
							tx_id,
							nft_address,
							amount,
							denom,
							tokenid,
							action
					) e
					JOIN RE_takers t
					ON e.tx_id = t.tx_id
				)

				UNION 

				SELECT 
				block_timestamp,
				tx_id,
				'Knowhere' as platform,
				MAX(IFF(event_attributes:bid_amount is not null, 'SELL', 'AUCTION')) as action,
				MAX(IFF(event_type = 'coin_received', COALESCE(event_attributes:"2_receiver", event_attributes:"1_receiver"), '')) as nft_from,
				MAX(IFF(event_attributes:"0_action" = 'settle' AND event_attributes:"1_action" = 'transfer_nft', event_attributes:recipient, '')) as nft_to,
				MAX(IFF(event_attributes:"1_action" is not null, event_attributes:"1_contract_address", ''))::string as nft_address,
				MAX(IFF(event_type = 'coin_received', COALESCE(NVL(event_attributes:"0_amount"[0]:amount,0) + NVL(event_attributes:"1_amount"[0]:amount,0) + NVL(event_attributes:"2_amount"[0]:amount, 0), event_attributes:amount[0]:amount), 0)) / 1e6 as amount,
				MAX(IFF(event_type = 'coin_received', COALESCE(event_attributes:"0_amount"[0]:denom, event_attributes:amount[0]:denom), ''))::string as denom,
				MAX(IFF(event_type = 'wasm', event_attributes:token_id, 0)) as tokenid
				FROM 
				terra.msg_events
				WHERE 
					tx_status = 'SUCCEEDED'
					AND block_timestamp >= CURRENT_DATE - 14
					AND tx_id IN ( 
						SELECT DISTINCT 
							tx_id 
						FROM terra.msgs 
						WHERE 
							msg_value:execute_msg:settle:auction_id is not null 
							AND tx_status = 'SUCCEEDED' 
							AND msg_value:contract = 'terra12v8vrgntasf37xpj282szqpdyad7dgmkgnq60j'
							AND block_timestamp >= CURRENT_DATE - 14
					)
				GROUP BY
					block_timestamp,
					tx_id

				UNION

				SELECT 
				block_timestamp,
				tx_id,
				'Luart' as platform,
				UPPER(event_attributes:order_type) as action,
				event_attributes:order_creator as nft_from, -- for sells, no info about other types yet
				event_attributes:recipient as nft_to,
				event_attributes:nft_contract_address as nft_address,
				event_attributes:price / 1e6 as amount,
				event_attributes:denom::string as denom,
				event_attributes:"0_token_id" as tokenid
				FROM terra.msg_events
				WHERE 
				event_type = 'from_contract'
				AND event_attributes:action = 'transfer_nft'
				AND event_attributes:method = 'execute_order'
				AND event_attributes:"0_contract_address" = 'terra1fj44gmt0rtphu623zxge7u3t85qy0jg6p5ucnk'
				AND block_timestamp >= CURRENT_DATE - 14
			)
			WHERE nft_address IN (
				'terra13nccm82km0ttah37hkygnvz67hnvkdass24yzv',
				'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2',
				'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k',
				'terra1vhuyuwwr4rkdpez5f5lmuqavut28h5dt29rpn6',
				'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7',
				'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg',
				'terra14gfnxnwl0yz6njzet4n33erq5n70wt79nm24el',
				'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v'
			)
		)

		select * from allsales
	'''

	# query = '''
	# 	WITH 
	# 	RE_events AS (
	# 		SELECT
	# 			block_timestamp,
	# 			tx_id,
	# 			event_attributes
	# 		FROM
	# 			terra.msg_events
	# 		WHERE event_attributes:action = 'execute_orders'
	# 			AND event_type = 'from_contract'
	# 			AND tx_status = 'SUCCEEDED'
	# 	),

	# 	RE_takers AS (
	# 		SELECT DISTINCT
	# 			tx_id,
	# 			msg_value:sender as taker
	# 		FROM
	# 			terra.msgs
	# 		WHERE
	# 			tx_id IN (SELECT DISTINCT tx_id FROM RE_events)
	# 	),
	# 	allSales AS 
	# 	(
	# 		SELECT 
	# 		block_timestamp AS sale_date,
	# 		tx_id,
	# 		platform,
	# 		nft_from,
	# 		nft_to,
	# 		nft_address,
	# 		CASE nft_address
	# 			WHEN 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2' THEN 'LunaBulls'
	# 			WHEN 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k' THEN 'Galactic Punks'
	# 			WHEN 'terra1vhuyuwwr4rkdpez5f5lmuqavut28h5dt29rpn6' THEN 'Levana Dragons'
	# 			WHEN 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' THEN 'Levana Dust'
	# 			WHEN 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' THEN 'Levana Eggs'
	# 			WHEN 'terra14gfnxnwl0yz6njzet4n33erq5n70wt79nm24el' THEN 'Levana Loot'
	# 			WHEN 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' THEN 'Levana Meteors'
	# 			WHEN 'terra13nccm82km0ttah37hkygnvz67hnvkdass24yzv' THEN 'Galactic Angels'
	# 			ELSE nft_address 
	# 		END AS collection,
	# 		amount AS price,
	# 		denom,
	# 		tokenid AS token_id
	# 		FROM (
	# 			SELECT
	# 			block_timestamp,
	# 			tx_id,
	# 			'Random Earth' as platform,
	# 			action,
	# 			IFF(action = 'SELL', maker, taker) as nft_from,
	# 			IFF(action = 'ACCEPT BID', maker, taker) as nft_to,
	# 			nft_address,
	# 			amount,
	# 			denom,
	# 			tokenid
	# 			FROM (
	# 				SELECT
	# 					block_timestamp,
	# 					e.tx_id,
	# 					action,
	# 					maker,
	# 					taker,
	# 					nft_address,
	# 					amount,
	# 					denom,
	# 					tokenid
	# 				FROM (
	# 					SELECT 
	# 						block_timestamp,
	# 						tx_id,
	# 						IFF(event_attributes:order:order:maker_asset:info:nft is not null, 'SELL', 'ACCEPT BID') as action,
	# 						LISTAGG(CHR(F.VALUE)) WITHIN GROUP (ORDER BY F.INDEX) as maker,
	# 						IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:maker_asset:info:nft:contract_addr, event_attributes:order:order:taker_asset:info:nft:contract_addr)::string as nft_address,
	# 						IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:taker_asset:amount, event_attributes:order:order:maker_asset:amount) / 1e6 as amount,
	# 						IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:taker_asset:info:native_token:denom, event_attributes:order:order:maker_asset:info:native_token:denom)::string as denom,
	# 								IFF(event_attributes:order:order:maker_asset:info:nft is not null, event_attributes:order:order:maker_asset:info:nft:token_id, event_attributes:order:order:taker_asset:info:nft:token_id) as tokenid
	# 					FROM 
	# 						RE_events e,
	# 						LATERAL FLATTEN(input => event_attributes:order:order:maker) F
	# 					GROUP BY
	# 						block_timestamp,
	# 						tx_id,
	# 						nft_address,
	# 						amount,
	# 						denom,
	# 						tokenid,
	# 						action
	# 				) e
	# 				JOIN RE_takers t
	# 				ON e.tx_id = t.tx_id
	# 			)

	# 			UNION 

	# 			SELECT 
	# 			block_timestamp,
	# 			tx_id,
	# 			'Knowhere' as platform,
	# 			MAX(IFF(event_attributes:bid_amount is not null, 'SELL', 'AUCTION')) as action,
	# 			MAX(IFF(event_type = 'coin_received', COALESCE(event_attributes:"2_receiver", event_attributes:"1_receiver"), '')) as nft_from,
	# 			MAX(IFF(event_attributes:"0_action" = 'settle' AND event_attributes:"1_action" = 'transfer_nft', event_attributes:recipient, '')) as nft_to,
	# 			MAX(IFF(event_attributes:"1_action" is not null, event_attributes:"1_contract_address", ''))::string as nft_address,
	# 			MAX(IFF(event_type = 'coin_received', COALESCE(NVL(event_attributes:"0_amount"[0]:amount,0) + NVL(event_attributes:"1_amount"[0]:amount,0) + NVL(event_attributes:"2_amount"[0]:amount, 0), event_attributes:amount[0]:amount), 0)) / 1e6 as amount,
	# 			MAX(IFF(event_type = 'coin_received', COALESCE(event_attributes:"0_amount"[0]:denom, event_attributes:amount[0]:denom), ''))::string as denom,
	# 			MAX(IFF(event_type = 'wasm', event_attributes:token_id, 0)) as tokenid
	# 			FROM 
	# 			terra.msg_events
	# 			WHERE 
	# 				tx_status = 'SUCCEEDED'
	# 				AND tx_id IN ( 
	# 					SELECT DISTINCT 
	# 						tx_id 
	# 					FROM terra.msgs 
	# 					WHERE 
	# 						msg_value:execute_msg:settle:auction_id is not null 
	# 						AND tx_status = 'SUCCEEDED' 
	# 						AND msg_value:contract = 'terra12v8vrgntasf37xpj282szqpdyad7dgmkgnq60j'
	# 				)
	# 			GROUP BY
	# 				block_timestamp,
	# 				tx_id

	# 			UNION

	# 			SELECT 
	# 			block_timestamp,
	# 			tx_id,
	# 			'Luart' as platform,
	# 			UPPER(event_attributes:order_type) as action,
	# 			event_attributes:order_creator as nft_from, -- for sells, no info about other types yet
	# 			event_attributes:recipient as nft_to,
	# 			event_attributes:nft_contract_address as nft_address,
	# 			event_attributes:price / 1e6 as amount,
	# 			event_attributes:denom::string as denom,
	# 			event_attributes:"0_token_id" as tokenid
	# 			FROM terra.msg_events
	# 			WHERE 
	# 			event_type = 'from_contract'
	# 			AND event_attributes:action = 'transfer_nft'
	# 			AND event_attributes:method = 'execute_order'
	# 			AND event_attributes:"0_contract_address" = 'terra1fj44gmt0rtphu623zxge7u3t85qy0jg6p5ucnk'
	# 		)
	# 		WHERE nft_address IN (
	# 			'terra13nccm82km0ttah37hkygnvz67hnvkdass24yzv',
	# 			'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2',
	# 			'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k',
	# 			'terra1vhuyuwwr4rkdpez5f5lmuqavut28h5dt29rpn6',
	# 			'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7',
	# 			'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg',
	# 			'terra14gfnxnwl0yz6njzet4n33erq5n70wt79nm24el',
	# 			'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v'
	# 		)
	# 	)

	# 	select * from allsales
	# '''
	add_sales(query, usr, pwd, True, data_folder)


