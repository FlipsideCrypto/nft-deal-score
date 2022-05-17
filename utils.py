import re
import pandas as pd


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
	,'bayc': 'BAYC'
	,'bakc': 'BAKC'
	,'mayc': 'MAYC'
	,'solgods': 'SOLGods'
	,'meerkatmillionairescc': 'Meerkat Millionaires'
	# ,'stonedapecrew': 'Stoned Ape Crew'
}

def format_num(x):
	return('{:,}'.format(round(x, 2)))

def clean_token_id(df):
	tokens = pd.read_csv('./data/tokens.csv')
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

def clean_name(name):
	x = re.sub('-', '', name).lower()
	x = re.sub(' ', '', x).lower()
	if x in clean_names.keys():
		return(clean_names[x])
	name = name.title()
	name = re.sub('-', ' ', name)
	# name = re.sub(' On ', ' on ', name)
	name = re.sub('Defi ', 'DeFi ', name)
	return(name)


def merge(left, right, on=None, how='inner', ensure=True, verbose=True, message = ''):
	df = left.merge(right, on=on, how=how)
	if len(df) != len(left) and (ensure or verbose):
		if message:
			print(message)
		print('{} -> {}'.format(len(left), len(df)))
		cur = left.merge(right, on=on, how='left')
		cols = set(right.columns).difference(set(left.columns))
		# print(cols)
		if ensure:
			col = list(cols)[0]
			missing = cur[cur[col].isnull()]
			print(missing.head())
			assert(False)
	return(df)

