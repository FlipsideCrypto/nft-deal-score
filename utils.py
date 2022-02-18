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
}

def clean_name(name):
	if name.lower() in clean_names.keys():
		return(clean_names[name.lower()])
	name = re.sub('-', '', name.title())
	return(name)


def merge(left, right, on=None, how='inner', ensure=True, verbose=True, message = ''):
	df = left.merge(right, on=on, how=how)
	if len(df) != len(left) and (ensure or verbose):
		if message:
			print(message)
		print('{} -> {}'.format(len(left), len(df)))
		cur = left.merge(right, on=on, how='left')
		cols = set(right.columns).difference(set(left.columns))
		print(cols)
		if ensure:
			col = list(cols)[0]
			missing = cur[cur[col].isnull()]
			print(missing.head())
			assert(False)
	return(df)

