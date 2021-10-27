source("~/data_science/util/util_functions.R")
library('DescTools')
library('zoo')
library('tidyr')
library('lme4')
library('lubridate')
library('fmsb')
library('ggplot2')
library('randomForest')

setwd('~/data_science/viz/token_tracker')

#######################################
#     Map Symbol to Token Address     #
#######################################
query <- "WITH base AS (
	SELECT contract_address, symbol, COUNT(1) AS n
	FROM ethereum.udm_events
	WHERE block_timestamp >= CURRENT_DATE - 250
		AND symbol IS NOT NULL
	GROUP BY 1, 2
), r AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY n DESC) AS rn
	FROM base
) 
SELECT contract_address, symbol
FROM r WHERE rn = 1 "
symbol <- QuerySnowflake({query})


#####################
#     Contracts     #
#####################
query <- "SELECT address AS contract_address, name AS contract_name
FROM silver.ethereum_contracts "
contracts <- QuerySnowflake({query})
contracts[, is_contract := 1]
stopifnot( nrow(contracts) == length(unique(contracts$contract_address)) )
contracts[ contract_address == '0x73b1714fb3bfaefa12f3707befcba3205f9a1162' ]


######################################
#     Map Label to Token Address     #
######################################
query <- "SELECT address AS contract_address, project_name, address_name, l1_label, l2_label FROM flipside_dev_db.silver.ethereum_address_labels"
labels <- QuerySnowflake({query})
labels[, is_wrapped := substr(address_name, 1, 7) == 'Wrapped'  ]
address_labels <- labels[, list(contract_address)]
colnames(address_labels)[1] <- 'address'
address_labels[, is_project := 1]


#######################################################
#     Set the Baseline to be Valid for this Model     #
#######################################################
valid <- paste0("
WITH prices AS (
	SELECT token_address AS contract_address, MIN(hour) AS mn_date
	FROM ethereum.token_prices_hourly_v2
	GROUP BY 1
), balance AS (
	SELECT contract_address, MIN(block_timestamp) AS mn_date
	FROM silver.ethereum_balances
	GROUP BY 1
), recent AS (
	SELECT contract_address, symbol, MIN(block_timestamp) AS mn_date
	FROM ethereum.udm_events u
	WHERE amount > 0
	GROUP BY 1, 2
	HAVING COUNT(1) >= 100
		AND MIN(block_timestamp) >= CURRENT_DATE - 250
), pools AS (
	SELECT DISTINCT contract_address
	FROM ethereum.udm_events
	WHERE to_label_subtype = 'pool'
), valid AS (
	SELECT r.contract_address, r.symbol
	FROM recent r
	JOIN pools po ON po.contract_address = r.contract_address 
	JOIN prices p ON p.contract_address = r.contract_address 
		AND r.mn_date > p.mn_date
	JOIN balance b ON b.contract_address = r.contract_address 
		AND r.mn_date > b.mn_date
)")


########################
#     Total Supply     #
########################
ptm <- proc.time()
query <- paste0(valid,", base AS (
	SELECT
	b.address
	, b.contract_address
	, DATE_TRUNC(day, b.block_timestamp) AS date
	, balance
	, l.project_name
	, l.l1_label
	, l.l2_label
	, CASE 
		WHEN l.l2_label = 'treasury' THEN 'Treasury'
		WHEN l.l2_label = 'pool' THEN 'Pool'
		WHEN l.l1_label = 'cex' THEN 'CEX'
		WHEN l.address IS NOT NULL THEN 'Other Project Contract'
		ELSE 'Wallet'
	END AS grp
	, ROW_NUMBER() OVER (PARTITION BY b.address, b.contract_address, date ORDER BY block_timestamp DESC) AS rn
	FROM silver.ethereum_balances b 
	LEFT JOIN flipside_dev_db.silver.ethereum_address_labels l ON l.address = b.address
	JOIN valid v ON v.contract_address = b.contract_address
	WHERE b.address <> '0x000000000000000000000000000000000000dead'
		AND b.address <> '0x0000000000000000000000000000000000000000'
		AND date >= CURRENT_DATE - 250
)
SELECT * FROM base WHERE rn = 1
")
balance <- QuerySnowflake({query})
balance[, date := as.Date(date) ]
print(proc.time() - ptm)
print(paste0('Working with ',length(unique(balance$contract_address)),' tokens'))
print(paste0('Balances with ',nrow(balance),' rows'))

#############################
#     Define Base Query     #
#############################
base_query <- paste0(valid,", s AS (
		SELECT DISTINCT tx_id
		FROM ethereum.events_emitted
		WHERE (
			event_name ilike '%swap%' 
			AND tx_to_label_type = 'dex'
		)
		OR (
			event_name = 'Swap' AND (
				tx_to_label = 'furucombo' 
				OR tx_to_label = 'dxdao' 
				OR contract_name like '% UNI-V2 LP' 
				OR contract_name like '% UNI-V3 LP' 
				OR contract_name like '% SLP' 
				OR contract_name ilike 'uniswap%' 
				OR contract_name ilike 'sushiswap%'
			)
		)
		OR (
			event_name IN ('Deposit','Withdrawal')
			AND (
				tx_to_label = 'tokenlon' 
			)
		)
		OR (
			event_name = 'FeiExchange' 
			AND contract_address = '0xa08a721dfb595753fff335636674d76c455b275c'
		)
	), l AS (
		SELECT DISTINCT tx_id
		FROM ethereum.events_emitted
		WHERE event_name IN (
			'Deposit'
			, 'Withdrawal'
			, 'IncreaseLiquidity'
			, 'DecreaseLiquidity'
			, 'AddLiquidity'
			, 'RemoveLiquidityOne'
			, 'RemoveLiquidity'
			, 'LiquidityRemoved'
		)
	), cnt AS (
		SELECT tx_id, COUNT(1) AS n_events
		FROM ethereum.udm_events
		GROUP BY 1
	), base AS (
		SELECT 
		u.tx_id
		, CASE 
			WHEN to_address = '0x0000000000000000000000000000000000000000' THEN 'Burn'
			WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'Mint'
			WHEN from_label IS NOT NULL AND from_label = to_label THEN 'Intra-Protocol Transfer'
			WHEN from_label IS NOT NULL AND to_label IS NOT NULL THEN 'Inter-Protocol Transfer'
			WHEN origin_function_name = 'redeem' THEN 'Airdrop'
			WHEN s.tx_id IS NOT NULL 
				OR origin_function_name IN ( 'swap','swapETHForExactTokens','swapExactETHForTokens','swapExactTokensForETH','swapExactTokensForTokens','swapTokensForExactTokens' ) 
				THEN 'Swap'
			WHEN l.tx_id IS NOT NULL OR origin_function_name IN ('addLiquidity','removeLiquidity','removeLiquidityWithPermit','removeLiquidity','withdrawAll','withdraw','deposit') THEN 'LP'
			WHEN origin_function_name = 'purchase' THEN 'Purchase'
			WHEN origin_function_name IN ('borrow','repayBorrow') THEN 'Borrowing'
			WHEN COALESCE( to_label_type, from_label_type ) = 'cex' THEN 'CEX'
			WHEN l.tx_id IS NULL AND COALESCE( to_label_type, from_label_type ) = 'dex' THEN 'Swap'
			WHEN u.origin_function_signature = '0xb7e93121' THEN 'Burn'
			WHEN c.n_events = 1 AND origin_function_name = 'transfer' THEN 'Direct Transfer'
			ELSE 'Other'
		END AS tx_type
		, block_timestamp
		, DATE_TRUNC(day, block_timestamp) AS date
		, u.contract_address
		, to_address
		, to_label
		, to_label_type
		, to_label_subtype
		, from_address
		, from_label
		, from_label_type
		, from_label_subtype
		, amount
		, amount_usd
		FROM ethereum.udm_events u
		JOIN valid v ON v.contract_address = u.contract_address
		LEFT JOIN cnt c ON c.tx_id = u.tx_id
		LEFT JOIN s ON s.tx_id = u.tx_id
		LEFT JOIN l ON l.tx_id = u.tx_id
		WHERE amount > 0
			AND block_timestamp >= CURRENT_DATE - 250
	)
")

##############################
#     Time Between Swaps     #
##############################
ptm <- proc.time()
query <- paste0(base_query,", swap_to AS (
		SELECT tx_id, block_timestamp, date, contract_address, amount, to_address AS address
		FROM base 
		WHERE tx_type = 'Swap'
		AND to_label IS NULL
	), swap_fr AS (
		SELECT tx_id, block_timestamp, date, contract_address, amount, from_address AS address
		FROM base 
		WHERE tx_type = 'Swap'
		AND from_label IS NULL
	)
	SELECT t.tx_id
	, t.address
	, t.contract_address
	, t.block_timestamp AS to_time
	, MIN(f.block_timestamp) AS fr_time
	FROM swap_to t
	LEFT JOIN swap_fr f ON f.contract_address = t.contract_address 
		AND f.address = t.address 
		AND f.block_timestamp >= t.block_timestamp
	GROUP BY 1, 2, 3, 4
")
time.between.swaps <- QuerySnowflake({query})
time.between.swaps[, to_date:=as.Date(to_time)]
time.between.swaps[, fr_date:=as.Date(fr_time)]
print('Time Between Swaps')
print(proc.time() - ptm)


########################
#     Transactions     #
########################
ptm <- proc.time()
query <- paste0(valid,", s AS (
		SELECT tx_id, contract_address
		FROM ethereum.events_emitted
		WHERE (
			event_name ilike '%swap%' 
			AND tx_to_label_type = 'dex'
		)
		OR (
			event_name = 'Swap' AND (
				tx_to_label = 'furucombo' 
				OR tx_to_label = 'dxdao' 
				OR contract_name like '% UNI-V2 LP' 
				OR contract_name like '% UNI-V3 LP' 
				OR contract_name like '% SLP' 
				OR contract_name ilike 'uniswap%' 
				OR contract_name ilike 'sushiswap%'
			)
		)
		OR (
			event_name IN ('Deposit','Withdrawal')
			AND (
				tx_to_label = 'tokenlon' 
			)
		)
		OR (
			event_name = 'FeiExchange' 
			AND contract_address = '0xa08a721dfb595753fff335636674d76c455b275c'
		)
		GROUP BY 1, 2
	), l AS (
		SELECT tx_id, contract_address
		FROM ethereum.events_emitted
		WHERE event_name IN (
			'Deposit'
			, 'Withdrawal'
			, 'IncreaseLiquidity'
			, 'DecreaseLiquidity'
			, 'AddLiquidity'
			, 'RemoveLiquidityOne'
			, 'RemoveLiquidity'
			, 'LiquidityRemoved'
		)
		GROUP BY 1, 2
	), rewards AS (
		SELECT tx_id, contract_address
		FROM ethereum.events_emitted
		WHERE event_name IN (
			'Harvest'
			, 'TokensClaimed'
		)
		GROUP BY 1, 2
	), cnt AS (
		SELECT tx_id, COUNT(1) AS n_events
		FROM ethereum.udm_events
		GROUP BY 1
	)
	SELECT 
	u.tx_id
	, CASE 
		WHEN to_address = '0x0000000000000000000000000000000000000000' THEN 'Burn'
		WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'Mint'
		WHEN from_label IS NOT NULL AND from_label = to_label THEN 'Intra-Protocol Transfer'
		WHEN from_label IS NOT NULL AND to_label IS NOT NULL THEN 'Inter-Protocol Transfer'
		WHEN origin_function_name = 'redeem' THEN 'Airdrop'
		WHEN s.tx_id IS NOT NULL 
			OR origin_function_name IN ( 'swap','swapETHForExactTokens','swapExactETHForTokens','swapExactTokensForETH','swapExactTokensForTokens','swapTokensForExactTokens' ) 
			THEN 'Swap'
		WHEN l.tx_id IS NOT NULL OR origin_function_name IN ('addLiquidity','removeLiquidity','removeLiquidityWithPermit','removeLiquidity','withdrawAll','withdraw','deposit') THEN 'LP'
		WHEN origin_function_name = 'purchase' THEN 'Purchase'
		WHEN origin_function_name IN ('borrow','repayBorrow') THEN 'Borrowing'
		WHEN COALESCE( to_label_type, from_label_type ) = 'cex' THEN 'CEX'
		WHEN l.tx_id IS NULL AND COALESCE( to_label_type, from_label_type ) = 'dex' THEN 'Swap'
		WHEN u.origin_function_signature = '0xb7e93121' THEN 'Burn'
		WHEN c.n_events = 1 AND origin_function_name = 'transfer' THEN 'Direct Transfer'
		ELSE 'Other'
	END AS tx_type
	, block_timestamp
	, DATE_TRUNC(day, block_timestamp) AS date
	, u.contract_address
	, to_address
	, to_label
	, to_label_type
	, to_label_subtype
	, from_address
	, from_label
	, from_label_type
	, from_label_subtype
	, amount
	, amount_usd
	FROM ethereum.udm_events u
	JOIN valid v ON v.contract_address = u.contract_address
	LEFT JOIN cnt c ON c.tx_id = u.tx_id
	LEFT JOIN s ON s.tx_id = u.tx_id AND s.contract_address = u.contract_address
	LEFT JOIN l ON l.tx_id = u.tx_id AND l.contract_address = u.contract_address
	LEFT JOIN rewards r ON r.tx_id = u.tx_id AND r.contract_address = u.contract_address
	WHERE amount > 0
		AND block_timestamp >= CURRENT_DATE - 250
")
transaction_detail <- QuerySnowflake({query})
print(proc.time() - ptm)

# number of transactions by type
g <- transaction_detail %>% group_by(tx_type) %>% summarize(n=n()) %>% as.data.table()
g <- g[order(-n)]
print('# of Transactions by type')
print(g)

# merging in contracts
nrow(transaction_detail)
to_contracts <- contracts
colnames(to_contracts) <- c( 'to_address','to_contract_name','to_is_contract' )
transaction_detail <- merge(transaction_detail, to_contracts, all.x=T, by=c('to_address'))

from_contracts <- contracts
colnames(from_contracts) <- c( 'from_address','from_contract_name','from_is_contract' )
transaction_detail <- merge(transaction_detail, from_contracts, all.x=T, by=c('from_address'))
nrow(transaction_detail)

# printing out some info
n.0 <- nrow(transaction_detail[ tx_type == 'Swap' ])
n.1 <- nrow(transaction_detail[ tx_type == 'Swap' & is.na(to_label) & is.na(from_label) ])
print(paste0(n.1, " swaps with no label (",round(100 * n.1 / n.0, 1),"%)"))

n.2 <- nrow(transaction_detail[ tx_type == 'Swap' & is.na(to_label) & is.na(from_label) & is.na(to_contract_name) & is.na(from_contract_name) ])
print(paste0(n.2, " swaps with no label or contract (",round(100 * n.2 / n.0, 1),"%)"))

print("Swaps with no label but contract")
head(transaction_detail[ tx_type == 'Swap' & is.na(to_label) & is.na(from_label) & !is.na(to_contract_name) & !is.na(from_contract_name), list(tx_id, to_address, from_address, to_contract_name, from_contract_name) ])
g <- transaction_detail[ tx_type == 'Swap' & is.na(to_label) & is.na(from_label) & !is.na(to_contract_name) ] %>% group_by(to_contract_name) %>% summarize(n=n()) %>% as.data.table()
g <- g[order(-n)]
head(g, 20)

# update labels on a few contracts
transaction_detail[ tx_type == 'Swap' & is.na(to_label) & is.na(from_label) & to_contract_name %in% c('UniswapV2Pair','UniswapV3Pool'), to_label := 'uniswap' ]
transaction_detail[ tx_type == 'Swap' & is.na(to_label) & is.na(from_label) & from_contract_name %in% c('UniswapV2Pair','UniswapV3Pool'), from_label := 'uniswap' ]

# printing out some info
n.0 <- nrow(transaction_detail[ tx_type == 'LP' ])
n.1 <- nrow(transaction_detail[ tx_type == 'LP' & is.na(to_label) & is.na(from_label) ])
head(transaction_detail[ tx_type == 'LP' & is.na(to_label) & is.na(from_label), list(tx_id, from_address, to_address, from_contract_name, to_contract_name) ])
head(transaction_detail[ tx_id == '0xf9e539dde0a00b01c4bcfd3df625fb1d0741e293e87de5952f5b83b6f8c8c49e' ])
print(paste0(n.1, " LPs with no label (",round(100 * n.1 / n.0, 1),"%)"))

n.2 <- nrow(transaction_detail[ tx_type == 'LP' & is.na(to_label) & is.na(from_label) & is.na(to_contract_name) & is.na(from_contract_name) ])
print(paste0(n.2, " LPs with no label or contract (",round(100 * n.2 / n.0, 1),"%)"))


##############################
#     Price + Volatility     #
##############################
query <- paste0("WITH base AS (
	SELECT DATE_TRUNC(day, hour) AS date
	, price
	, ROW_NUMBER() OVER (PARTITION BY token_address, date ORDER BY hour DESC) AS rn
	FROM ethereum.token_prices_hourly_v2 p
	WHERE symbol = 'ETH' AND token_address IS NULL
)
SELECT date, price AS eth_price
, LAG(price, 1) OVER (ORDER BY date DESC) AS eth_price_in_1_days
, LAG(price, 7) OVER (ORDER BY date DESC) AS eth_price_in_7_days
FROM base
WHERE rn = 1
")
eth_price <- QuerySnowflake({query})
eth_price[, date := as.Date(date) ]

##############################
#     Price + Volatility     #
##############################
query <- paste0(valid,", base AS (
	SELECT token_address AS contract_address
	, DATE_TRUNC(day, hour) AS date
	, price
	, ROW_NUMBER() OVER (PARTITION BY token_address, date ORDER BY hour DESC) AS rn
	FROM ethereum.token_prices_hourly_v2 p
	JOIN valid v ON v.contract_address = p.token_address
)
SELECT contract_address, date, price
, LAG(price, 1) OVER (PARTITION BY contract_address ORDER BY date DESC) AS price_in_1_days
, LAG(price, 7) OVER (PARTITION BY contract_address ORDER BY date DESC) AS price_in_7_days
FROM base
WHERE rn = 1
")
price <- QuerySnowflake({query})
price[, date := as.Date(date) ]

transaction_detail$date <- as.Date(transaction_detail$date)
transaction_detail <- merge(transaction_detail, price, all.x=T)
transaction_detail[, cur_price:=na.locf(na.locf(price, na.rm = FALSE), na.rm = FALSE, fromLast = TRUE), c('contract_address')]
transaction_detail[ is.na(amount_usd), amount_usd := cur_price * amount ]


write.csv( transaction_detail[contract_address=='0x4b86e0295e7d32433ffa6411b82b4f4e56a581e1' & tx_type=='Swap'], '~/Downloads/tmp1.csv', row.names=F )

tx_to <- transaction_detail[, list(tx_id, tx_type, date, contract_address, amount, to_address, to_label, from_address, from_label)]
tx_fr <- transaction_detail[, list(tx_id, tx_type, date, contract_address, amount, from_address, from_label, to_address, to_label)]
n <- length(tx_to)
colnames(tx_to)[n-3] <- 'address'
colnames(tx_fr)[n-3] <- 'address'
colnames(tx_to)[n-2] <- 'label'
colnames(tx_fr)[n-2] <- 'label'
colnames(tx_to)[n-1] <- 'other_address'
colnames(tx_fr)[n-1] <- 'other_address'
colnames(tx_to)[n] <- 'other_label'
colnames(tx_fr)[n] <- 'other_label'
tx_to[, dir := 'to' ]
tx_fr[, dir := 'fr' ]
tx_to[, net_amt := amount ]
tx_fr[, net_amt := -amount ]

tx <- rbind(tx_to, tx_fr) %>% as.data.table()
tx <- tx[order(contract_address, address)]
keycols <- c("contract_address","address")
setkeyv(tx, keycols)

base <- unique(price[, list(contract_address)])
extra.metrics <- data.table()
for (cur_date in dates) {
	if (cur_date <= as.Date("2021-05-21")) {
		next
	}
	ptm <- proc.time()
	cur_date <- as.Date(cur_date)
	print(cur_date)

	t.all <- transaction_detail[ date <= eval(cur_date) ]
	t.30 <- t.all[ date > eval(cur_date - 30) ]
	t.7 <- t.30[ date > eval(cur_date - 7) ]
	t.1 <- t.7[ date == eval(cur_date) ]

	lp.30 <- t.30[ tx_type == 'LP' ] %>% group_by(contract_address) %>% summarize( lp.from.amount.30=sum(ifelse(is.na(from_label), amount, 0)), lp.to.amount.30=sum(ifelse(is.na(to_label), amount, 0)) ) %>% as.data.table()
	lp.7 <- t.7[ tx_type == 'LP' ] %>% group_by(contract_address) %>% summarize( lp.from.amount.7=sum(ifelse(is.na(from_label), amount, 0)), lp.to.amount.7=sum(ifelse(is.na(to_label), amount, 0)) ) %>% as.data.table()
	lp.1 <- t.1[ tx_type == 'LP' ] %>% group_by(contract_address) %>% summarize( lp.from.amount.1=sum(ifelse(is.na(from_label), amount, 0)), lp.to.amount.1=sum(ifelse(is.na(to_label), amount, 0)) ) %>% as.data.table()

	swap.30 <- t.30[ tx_type == 'Swap' ] %>% group_by(contract_address) %>% summarize( swap.from.amount.30=sum(ifelse(is.na(from_label), amount, 0)), swap.to.amount.30=sum(ifelse(is.na(to_label), amount, 0)) ) %>% as.data.table()
	swap.7 <- t.7[ tx_type == 'Swap' ] %>% group_by(contract_address) %>% summarize( swap.from.amount.7=sum(ifelse(is.na(from_label), amount, 0)), swap.to.amount.7=sum(ifelse(is.na(to_label), amount, 0)) ) %>% as.data.table()
	swap.1 <- t.1[ tx_type == 'Swap' ] %>% group_by(contract_address) %>% summarize( swap.from.amount.1=sum(ifelse(is.na(from_label), amount, 0)), swap.to.amount.1=sum(ifelse(is.na(to_label), amount, 0)) ) %>% as.data.table()

	cur <- base
	cur$date <- cur_date

	cur <- merge(cur, swap.30, all.x=T)
	cur <- merge(cur, swap.7, all.x=T)
	cur <- merge(cur, swap.1, all.x=T)

	cur <- merge(cur, lp.30, all.x=T)
	cur <- merge(cur, lp.7, all.x=T)
	cur <- merge(cur, lp.1, all.x=T)

	extra.metrics <- rbind( extra.metrics, cur )
	print(proc.time() - ptm)

}


##############################
#     Daily Calculations     #
##############################
# number of users swapping (1, 7, 30)
base <- unique(price[, list(contract_address)])
daily.metrics <- data.table()
balance <- balance[order(contract_address, address, -date)]
dates <- sort(unique(transaction_detail$date))
for (cur_date in dates) {
	if (cur_date <= as.Date("2021-08-03")) {
		next
	}
	ptm <- proc.time()
	cur_date <- as.Date(cur_date)
	print(cur_date)

	cur_balance <- balance[ date <= eval(cur_date), head(.SD, 1), by = list(contract_address, address)]
	cur_balance <- cur_balance[order(-balance)]
	# head(cur_balance[contract_address=='0xdbdb4d16eda451d0503b854cf79d55697f90c8df'])
	# head(cur_balance)
	tot_supply <- cur_balance %>% group_by(contract_address) %>% summarize(
		tot_supply=sum(balance)
		, wallet_balance=sum(ifelse(grp == 'Wallet', balance, 0))
		, treasury_balance=sum(ifelse(grp == 'Treasury', balance, 0))
		, pool_balance=sum(ifelse(grp == 'Pool', balance, 0))
		, cex_balance=sum(ifelse(grp == 'CEX', balance, 0))
		, contract_balance=sum(ifelse(grp == 'Other Project Contract', balance, 0))
	) %>% as.data.table()
	tot_supply[, pct_balance_wallet := wallet_balance / tot_supply ]
	tot_supply[, pct_balance_treasury := treasury_balance / tot_supply ]
	tot_supply[, pct_balance_pool := pool_balance / tot_supply ]
	tot_supply[, pct_balance_cex := cex_balance / tot_supply ]
	tot_supply[, pct_balance_contract := contract_balance / tot_supply ]


	t.all <- transaction_detail[ date <= eval(cur_date) ]
	t.30 <- t.all[ date > eval(cur_date - 30) ]
	t.7 <- t.30[ date > eval(cur_date - 7) ]
	t.1 <- t.7[ date == eval(cur_date) ]

	tx.all <- tx[ date <= eval(cur_date) ]
	tx.30 <- tx.all[ date > eval(cur_date - 30) ]
	tx.7 <- tx.30[ date > eval(cur_date - 7) ]
	tx.1 <- tx.7[ date == eval(cur_date) ]

	n.cex <- t.all[ (from_label_type == 'cex') ] %>% group_by(contract_address) %>% summarize( n.cex=n_distinct(from_label) )
	n.dex <- t.all[ (from_label_type %in% c('dex','defi')) & from_label_subtype %in% c('pool','swap_contract','rewards') ] %>% group_by(contract_address) %>% summarize( n.dex=n_distinct(from_label) )

	# cur_ownership <- tx.all[ tx_type != 'LP' ] %>% group_by( contract_address, address ) %>% summarize(net_amount=sum(net_amount))
	# ptm <- proc.time()
	cur_ownership <- tx[ date <= eval(cur_date) & tx_type != 'LP' ] %>% group_by( contract_address, address ) %>% summarize(net_amt=sum(net_amt)) %>% as.data.table()
	cur_ownership <- merge( cur_ownership, address_labels, all.x=T )
	cur_ownership <- cur_ownership[ is.na(is_project) ]
	# cur_ownership <- cur_ownership[ ! address %in% labels$contract_address ]
	# cur_ownership <- cur_ownership[order(-net_amt)]
	# head(cur_ownership)
	# head(cur_ownership[contract_address == '0x06f3c323f0238c72bf35011071f2b5b7f43a054c'])
	# print(proc.time() - ptm)
	gini <- cur_ownership[(net_amt > 0) & (address != '0x0000000000000000000000000000000000000000') & (address != '0x000000000000000000000000000000000000dead')] %>% group_by( contract_address ) %>% summarize(gini=Gini(net_amt))

	cex.30 <- t.30[ tx_type == 'CEX' ] %>% group_by(contract_address) %>% summarize( n.cex.30=n(), amount.cex.30=sum(amount), amount.usd.cex.30=sum(amount_usd), n.users.cex.to.30=n_distinct(to_address), n.users.cex.from.30=n_distinct(from_address), net.cex.amount.30=sum(ifelse(is.na(to_label), -amount, amount)), net.cex.amount.usd.30=sum(ifelse(is.na(to_label), amount_usd, -amount_usd)) )
	cex.7 <- t.7[ tx_type == 'CEX' ] %>% group_by(contract_address) %>% summarize( n.cex.7=n(), amount.cex.7=sum(amount), amount.usd.cex.7=sum(amount_usd), n.users.cex.to.7=n_distinct(to_address), n.users.cex.from.7=n_distinct(from_address), net.cex.amount.7=sum(ifelse(is.na(to_label), -amount, amount)), net.cex.amount.usd.7=sum(ifelse(is.na(to_label), amount_usd, -amount_usd)) )
	cex.1 <- t.1[ tx_type == 'CEX' ] %>% group_by(contract_address) %>% summarize( n.cex.1=n(), amount.cex.1=sum(amount), amount.cex.usd.1=sum(amount_usd), n.users.cex.to.1=n_distinct(to_address), n.users.cex.from.1=n_distinct(from_address), net.cex.amount.1=sum(ifelse(is.na(to_label), -amount, amount)), net.cex.amount.usd.1=sum(ifelse(is.na(to_label), amount_usd, -amount_usd)) )

	swap.30 <- t.30[ tx_type == 'Swap' ] %>% group_by(contract_address) %>% summarize( n.swap.30=n(), amount.swap.30=sum(amount), amount.usd.swap.30=sum(amount_usd), n.users.swap.to.30=n_distinct(to_address), n.users.swap.from.30=n_distinct(from_address), net.swap.amount.30=sum(ifelse(is.na(to_label), -amount, amount)), net.swap.amount.usd.30=sum(ifelse(is.na(to_label), amount_usd, -amount_usd)), swap.from.amount.30=sum(ifelse(is.na(from_label), amount, 0)), swap.to.amount.30=sum(ifelse(is.na(to_label), amount, 0)) )
	swap.7 <- t.7[ tx_type == 'Swap' ] %>% group_by(contract_address) %>% summarize( n.swap.7=n(), amount.swap.7=sum(amount), amount.usd.swap.7=sum(amount_usd), n.users.swap.to.7=n_distinct(to_address), n.users.swap.from.7=n_distinct(from_address), net.swap.amount.7=sum(ifelse(is.na(to_label), -amount, amount)), net.swap.amount.usd.7=sum(ifelse(is.na(to_label), amount_usd, -amount_usd)), swap.from.amount.7=sum(ifelse(is.na(from_label), amount, 0)), swap.to.amount.7=sum(ifelse(is.na(to_label), amount, 0)) )
	swap.1 <- t.1[ tx_type == 'Swap' ] %>% group_by(contract_address) %>% summarize( n.swap.1=n(), amount.swap.1=sum(amount), amount.swap.usd.1=sum(amount_usd), n.users.swap.to.1=n_distinct(to_address), n.users.swap.from.1=n_distinct(from_address), net.swap.amount.1=sum(ifelse(is.na(to_label), -amount, amount)), net.swap.amount.usd.1=sum(ifelse(is.na(to_label), amount_usd, -amount_usd)), swap.from.amount.1=sum(ifelse(is.na(from_label), amount, 0)), swap.to.amount.1=sum(ifelse(is.na(to_label), amount, 0)) )

	lp.30 <- t.30[ tx_type == 'LP' ] %>% group_by(contract_address) %>% summarize( n.lp.30=n(), amount.lp.30=sum(amount), amount.usd.lp.30=sum(amount_usd), n.users.lp.to.30=n_distinct(to_address), n.users.lp.from.30=n_distinct(from_address), lp.from.amount.30=sum(ifelse(is.na(from_label), amount, 0)), lp.to.amount.30=sum(ifelse(is.na(to_label), amount, 0)), net.lp.amount.30=sum(ifelse(is.na(to_label), -amount, amount)), net.lp.amount.usd.30=sum(ifelse(is.na(to_label), amount_usd, -amount_usd)) )
	lp.7 <- t.7[ tx_type == 'LP' ] %>% group_by(contract_address) %>% summarize( n.lp.7=n(), amount.lp.7=sum(amount), amount.usd.lp.7=sum(amount_usd), n.users.lp.to.7=n_distinct(to_address), n.users.lp.from.7=n_distinct(from_address), lp.from.amount.7=sum(ifelse(is.na(from_label), amount, 0)), lp.to.amount.7=sum(ifelse(is.na(to_label), amount, 0)), net.lp.amount.7=sum(ifelse(is.na(to_label), -amount, amount)), net.lp.amount.usd.7=sum(ifelse(is.na(to_label), amount_usd, -amount_usd)) )
	lp.1 <- t.1[ tx_type == 'LP' ] %>% group_by(contract_address) %>% summarize( n.lp.1=n(), amount.lp.1=sum(amount), amount.usd.lp.1=sum(amount_usd), n.users.lp.to.1=n_distinct(to_address), n.users.lp.from.1=n_distinct(from_address), lp.from.amount.1=sum(ifelse(is.na(from_label), amount, 0)), lp.to.amount.1=sum(ifelse(is.na(to_label), amount, 0)), net.lp.amount.1=sum(ifelse(is.na(to_label), -amount, amount)), net.lp.amount.usd.1=sum(ifelse(is.na(to_label), amount_usd, -amount_usd)) )

	n.users.cex.30 <- tx[ tx_type == 'CEX' ] %>% group_by(contract_address) %>% summarize( n.users.cex.30=n_distinct(address) )
	n.users.cex.7 <- tx.7[ tx_type == 'CEX' ] %>% group_by(contract_address) %>% summarize( n.users.cex.7=n_distinct(address) )
	n.users.cex.1 <- tx.1[ tx_type == 'CEX' ] %>% group_by(contract_address) %>% summarize( n.users.cex.1=n_distinct(address) )

	n.users.swap.30 <- tx[ tx_type == 'Swap' ] %>% group_by(contract_address) %>% summarize( n.users.swap.30=n_distinct(address) )
	n.users.swap.7 <- tx.7[ tx_type == 'Swap' ] %>% group_by(contract_address) %>% summarize( n.users.swap.7=n_distinct(address) )
	n.users.swap.1 <- tx.1[ tx_type == 'Swap' ] %>% group_by(contract_address) %>% summarize( n.users.swap.1=n_distinct(address) )

	n.users.lp.30 <- tx[ tx_type == 'LP' ] %>% group_by(contract_address) %>% summarize( n.users.lp.30=n_distinct(address) )
	n.users.lp.7 <- tx.7[ tx_type == 'LP' ] %>% group_by(contract_address) %>% summarize( n.users.lp.7=n_distinct(address) )
	n.users.lp.1 <- tx.1[ tx_type == 'LP' ] %>% group_by(contract_address) %>% summarize( n.users.lp.1=n_distinct(address) )

	time.between.swaps.tmp <- time.between.swaps
	time.between.swaps.tmp[is.na(fr_date), fr_date := eval(cur_date)]
	time.between.swaps.tmp[, days_between_swap := fr_date - to_date ]
	time.between.swaps.tmp[, days_between_swap_is_0 := as.integer(days_between_swap == 0) ]
	time.between.swaps.avg <- time.between.swaps.tmp %>% group_by(contract_address) %>% summarize( days.between.swaps.avg=mean(days_between_swap),days.between.swaps.avg.cap=mean(ifelse(days_between_swap > 21, 21, days_between_swap)), days.between.swaps.pct.0=mean(days_between_swap_is_0) )
	# write.csv(time.between.swaps.avg, '~/Downloads/tmp.csv', row.names=F)

	cur <- base
	cur$date <- cur_date
	cur <- merge(cur, swap.30, all.x=T)
	cur <- merge(cur, swap.7, all.x=T)
	cur <- merge(cur, swap.1, all.x=T)
	cur <- merge(cur, lp.30, all.x=T)
	cur <- merge(cur, lp.7, all.x=T)
	cur <- merge(cur, lp.1, all.x=T)

	cur <- merge(cur, n.users.swap.30, all.x=T)
	cur <- merge(cur, n.users.swap.7, all.x=T)
	cur <- merge(cur, n.users.swap.1, all.x=T)
	cur <- merge(cur, n.users.lp.30, all.x=T)
	cur <- merge(cur, n.users.lp.7, all.x=T)
	cur <- merge(cur, n.users.lp.1, all.x=T)

	cur <- merge(cur, n.cex, all.x=T)
	cur <- merge(cur, n.dex, all.x=T)

	cur <- merge(cur, time.between.swaps.avg, all.x=T)

	cur <- merge(cur, tot_supply, all.x=T)

	cur <- merge(cur, gini, all.x=T)

	daily.metrics <- rbind( daily.metrics, cur )
	print(proc.time() - ptm)
}

g <- daily.metrics %>% group_by(date) %>% summarize(n=n()) %>% as.data.table()
g

save(daily.metrics, transaction_detail, price, eth_price, labels, file = "~/data_science/viz/token_tracker/train_data.RData")
load("~/data_science/viz/token_tracker/train_data.RData")

# daily.metrics[, lp.from.amount.30 := NULL ]
# daily.metrics[, lp.to.amount.30 := NULL ]
# daily.metrics[, swap.from.amount.30 := NULL ]
# daily.metrics[, swap.to.amount.30 := NULL ]

# daily.metrics[, lp.from.amount.7 := NULL ]
# daily.metrics[, lp.to.amount.7 := NULL ]
# daily.metrics[, swap.from.amount.7 := NULL ]
# daily.metrics[, swap.to.amount.7 := NULL ]

# daily.metrics[, lp.from.amount.1 := NULL ]
# daily.metrics[, lp.to.amount.1 := NULL ]
# daily.metrics[, swap.from.amount.1 := NULL ]
# daily.metrics[, swap.to.amount.1 := NULL ]

# daily.metrics <- merge(daily.metrics, extra.metrics, all.x=T, by=c('contract_address','date'))
daily.metrics[is.na(daily.metrics)] <- 0

origin_date <- transaction_detail %>% group_by(contract_address) %>% summarize(origin_date=min(date))

dt <- price[order(contract_address, date)]
dt <- merge( dt, origin_date )
dt[, token_age := date - origin_date]
dt <- merge( dt, eth_price, all.x=TRUE, by=c('date') )
# head(dt)

dt <- dt[order(contract_address, date)]
dt[, eth_price_1d := shift(eth_price, 1), by=contract_address]
dt[, price_1d := shift(price, 1), by=contract_address]
dt[, price_7d := shift(price, 7), by=contract_address]
dt[, price_30d := shift(price, 30), by=contract_address]
dt[, price_90d := shift(price, 90), by=contract_address]

dt[, rel_price_chg_1d := (price / eth_price) / (price_1d / eth_price_in_1_days) - 1 ]
dt[, price_chg_1d := (price / price_1d) - 1 ]
dt[, price_chg_7d := (price / price_7d) - 1 ]
dt[, price_chg_30d := (price / price_30d) - 1 ]
dt[, price_chg_90d := (price / price_90d) - 1 ]

dt[, price_volatility_1d := abs(rel_price_chg_1d) ]
dt[, price_volatility_7d := rollmean(price_volatility_1d, 7, align='right', fill=NA), by=contract_address]
dt[, price_volatility_30d := rollmean(price_volatility_1d, 30, align='right', fill=NA), by=contract_address]
dt[, price_volatility_90d := rollmean(price_volatility_1d, 90, align='right', fill=NA), by=contract_address]

mn_date <- min(daily.metrics$date)
dt <- dt[ date >= eval(mn_date) ]
dt <- merge( dt, daily.metrics, all.x=TRUE, by=c('contract_address','date') )
dt <- merge( dt, labels, all.x=TRUE, by=c('contract_address') )
dt[, net.users.swap := n.users.swap.from.30 - n.users.swap.to.30]
dt[, net.users.swap.ratio := n.users.swap.from.30 / n.users.swap.to.30]
dt[, has.cex := as.integer(n.cex > 0) ]
q <- price %>% group_by(contract_address) %>% summarize(p_05=quantile(price, 0.05), p_95=quantile(price, 0.95)) %>% as.data.table()
q[contract_address == '0x956f47f50a910163d8bf957cf5846d573e7f87ca']
stable <- q[ p_05 > 0.97 & p_95 < 1.03, list(contract_address) ]
stable[, is_stable := 1]
dt <- merge( dt, stable, all.x=TRUE, by=c('contract_address') )
dt[, log_price:=log(price)]

# high means more people are selling (bad, need pos coef)
dt[, net.swap.ratio.30 := swap.from.amount.30 / swap.to.amount.30 ]
dt[, net.swap.ratio.7 := swap.from.amount.7 / swap.to.amount.7 ]
dt[, net.swap.ratio.1 := swap.from.amount.1 / swap.to.amount.1 ]

dt[, net.users.swap.ratio.30 := n.users.swap.from.30 / n.users.swap.to.30 ]
dt[, net.users.swap.ratio.7 := n.users.swap.from.7 / n.users.swap.to.7 ]
dt[, net.users.swap.ratio.1 := n.users.swap.from.1 / n.users.swap.to.1 ]

# high means more people are getting into (good, need neg coef)
dt[, net.lp.ratio.30 := lp.from.amount.30 / lp.to.amount.30 ]
dt[, net.lp.ratio.7 := lp.from.amount.7 / lp.to.amount.7 ]
dt[, net.lp.ratio.1 := lp.from.amount.1 / lp.to.amount.1 ]

dt[, net.users.lp.ratio.30 := n.users.lp.from.30 / n.users.lp.to.30 ]
dt[, net.users.lp.ratio.7 := n.users.lp.from.7 / n.users.lp.to.7 ]
dt[, net.users.lp.ratio.1 := n.users.lp.from.1 / n.users.lp.to.1 ]

for (col in c('net.swap.ratio.30','net.swap.ratio.7','net.swap.ratio.1', 'net.users.swap.ratio.30','net.users.swap.ratio.7','net.users.swap.ratio.1','net.lp.ratio.30','net.lp.ratio.7','net.lp.ratio.1', 'net.users.lp.ratio.30','net.users.lp.ratio.7','net.users.lp.ratio.1')) {
	dt[ is.na(get(col)), eval(col) := 0 ]
	dt[ get(col) > 3, eval(col) := 3 ]
	# dt[ get(col) > 1.25, eval(col) := 1.25 ]
	# dt[ get(col) < 0.75, eval(col) := 0.75 ]
}

# head(dt[, list(symbol)])

# head(train[order(net.swap.ratio.30)][date == as.Date('2021-09-20'), list(symbol, contract_address, n.users.swap.from.30, swap.from.amount.30, n.users.swap.to.30, swap.to.amount.30, net.swap.ratio.30)])
# write.csv(train[order(net.swap.ratio.30)][date == as.Date('2021-09-20'), list(symbol, contract_address, n.users.swap.from.30, swap.from.amount.30, n.users.swap.to.30, swap.to.amount.30, net.swap.ratio.30)], '~/Downloads/tmp2.csv', row.names=F)

dt <- merge( dt, symbol, all.x=TRUE )
dt[, token_label := paste0(symbol, " (", contract_address, ")")]
# tail(dt[ date == as.Date('2021-09-20')][order(net.users.swap.ratio.30)][, list(symbol, net.users.swap.ratio.30)], 20)
# head(dt[ date == as.Date('2021-09-20')][order(net.users.swap.ratio.30)][, list(symbol, net.users.swap.ratio.30)], 20)
# mean(dt$net.users.swap.ratio.30)

dt[, n.users.swap.is.25 := as.integer(n.users.swap.1 >= 25) ]
dt[, n.users.swap.is.50 := as.integer(n.users.swap.1 >= 50) ]
dt[, n.users.swap.is.100 := as.integer(n.users.swap.1 >= 100) ]
dt <- dt[order(contract_address, date)]
dt[, days.with.100.swappers := cumsum(n.users.swap.is.100), by = list(contract_address, rleid(n.users.swap.is.100 == 0L))]
dt[, days.with.50.swappers := cumsum(n.users.swap.is.50), by = list(contract_address, rleid(n.users.swap.is.50 == 0L))]
dt[, days.with.25.swappers := cumsum(n.users.swap.is.25), by = list(contract_address, rleid(n.users.swap.is.25 == 0L))]

dt[, pct_price_chg := (price_in_7_days - price) / price]
dt[, eth_pct_price_chg := (eth_price_in_7_days - eth_price) / eth_price]
dt[, price_drop_50 := as.integer(pct_price_chg < -.5)]
dt[, rel_price_drop_50 := as.integer( ((price_in_7_days / eth_price_in_7_days) / (price / eth_price)) < .5)]
dt[, market_cap := price * tot_supply]
dt[, log_market_cap := log(market_cap)]
dt[, log_tot_supply := log(tot_supply)]

# TODO: > 50?
train <- dt[ !is.na(price_volatility_30d) & n.users.swap.from.30 > 0 & n.users.swap.to.30 > 0 & is.na(is_stable) & (is_wrapped==FALSE | is.na(is_wrapped)) & !is.na(price) & !is.na(price_drop_50) & !is.na(price_chg_1d) & !is.na(price_chg_1d) & !is.na(market_cap) & !is.na(tot_supply) & market_cap > 0 & market_cap > 0 ]
train <- train[order(contract_address, date)]
train[, valid := rollsum(rel_price_drop_50, 2, align='right', fill=0), by=contract_address ]
train[, valid := valid - rel_price_drop_50 ]
# head(train[rel_price_drop_50 > 0, list( symbol, date, valid, rel_price_drop_50)])
# head(train[ symbol == 'XDEX', list( symbol, date, valid, rel_price_drop_50)])
# nrow(train[rel_price_drop_50 > 0])
# nrow(train[valid > 0])
# nrow(train[valid == 0 & rel_price_drop_50 > 0])
# train <- train[valid == 0]
print(paste0("Training on ",nrow(train)," rows with ",length(unique(train$contract_address))," tokens"))

median(train[rel_price_drop_50 == 1]$net.users.lp.ratio.7)
median(train[rel_price_drop_50 == 0]$net.users.lp.ratio.7)

# write.csv( head(train), '~/Downloads/tmp.csv', row.names=F )

# train[ symbol == 'DOGIRA', list(date, price, price_volatility_1d, price_volatility_7d, price_volatility_30d) ]

# train[ symbol == 'DOGIRA' & date >= as.Date('2021-05-19'), list(date, swap.from.amount.30, swap.to.amount.30, n.users.swap.to.30, n.users.swap.from.30, net.users.swap.ratio.30, net.swap.ratio.30) ]
# write.csv(train[ symbol == 'DOGIRA', list(date, swap.from.amount.30, swap.to.amount.30, n.users.swap.to.30, n.users.swap.from.30, net.users.swap.ratio.30, net.swap.ratio.30) ], '~/Downloads/tmp.csv', row.names=F)

# train[ symbol == 'DOGIRA' ]$contract_address[1]

# head(train[order(net.users.swap.ratio.30)][ n.users.swap.from.30 > 0 & n.users.swap.to.30 > 0 , list(symbol, date, net.users.swap.ratio.30)], 20)
# nrow(train[ n.users.swap.from.30 > 0 & n.users.swap.to.30 > 0 ])
# nrow(train)

# head(train[, list(net.swap.ratio.30)])



pred_cols <- c( 'pct_balance_cex', 'days.between.swaps.pct.0','gini','days.with.25.swappers','n.users.lp.30','token_age','log_price','log_market_cap','price_volatility_30d','net.users.lp.ratio.7' )
formula <- paste0( 'rel_price_drop_50 ~ 1 + ', paste(pred_cols, collapse=' + '))
fit <- glm( as.formula(formula), data=train, family = "binomial" )
summary(fit)

rf <- randomForest(rel_price_drop_50 ~ pct_balance_cex + days.between.swaps.pct.0 + gini + days.with.25.swappers + n.users.lp.30 + token_age + price + market_cap + price_volatility_30d + net.users.lp.ratio.7 , data=train, importance=TRUE, maxnodes=20, nodesize=100)
i <- importance(rf, conditional=TRUE) %>% as.data.frame()
i$feature <- rownames(i)
i <- i %>% as.data.table()
i[, importance := IncNodePurity / sum(IncNodePurity)]
feature_importance <- i[, list(feature, importance)][order(importance)]
print(feature_importance)

# TODO
# call it token stability index
# "here are the major factors we found to be connected with price stability for a token"
# price relative to ETH
# variable importance

write.csv(feature_importance, '~/Downloads/feature_importance.csv')

train$pred = predict(rf, train)
# train[, grp := as.integer(round(percent_rank(pred) * 1))]
# train %>% group_by( grp ) %>% summarize(m=median(net.users.lp.ratio.7))
# head(unique(train[order(pred), list(symbol)]))
mx_date <- max(train$date)
publish <- train[ date == eval(mx_date) ]

group_func <- function(x) {
	if (x < .11) return('Extremely Low')
	if (x < .22) return('Very Low')
	if (x < .33) return('Low')
	if (x < .44) return('Moderately Low')
	if (x < .56) return('Moderate')
	if (x < .67) return('Moderately High')
	if (x < .78) return('High')
	if (x < .89) return('Very High')
	return('Extremely High')
}
publish.cols <- c('pred', pred_cols)
std.cols <- c()

for (p in pred_cols) {
	col.s <- paste0(p, '_std')
	std.cols <- c( std.cols, col.s )
	publish[, eval(col.s) := scale(get(p))]
	publish[, eval(col.s) := ifelse(get(col.s) < -3, -3, ifelse(get(col.s) > 3, 3, get(col.s))) ]
	col <- paste0(p, '_percentile')
	publish[, eval(col) := percent_rank(get(p))]
	colg <- paste0(p, '_percentile_grp')
	publish[, eval(colg) := group_func( get(col) )]
	publish.cols <- c( publish.cols, col, col.s, colg )
}

publish[, days.between.swaps.pct.0_percentile := 1 - days.between.swaps.pct.0_percentile ]
publish[, gini_percentile := 1 - gini_percentile ]
publish[, price_volatility_30d_percentile := 1 - price_volatility_30d_percentile ]

publish[, d.1 := (pct_balance_cex_std * 0.05) - (gini_std * 0.17) + (token_age_std * 0.04) + (log_price_std * 0.17) + (log_market_cap_std * 0.09) - (price_volatility_30d_std * 0.47)  ]
publish[, d.2 := - (days.between.swaps.pct.0_std * 0.08) + (days.with.25.swappers_std * 0.01) + (n.users.lp.30_std * 0.06) + (net.users.lp.ratio.7_std * 0.01) ]

publish[, d.1 := ((pct_balance_cex_percentile - 0.5) * 0.05) + ((gini_percentile - 0.5) * 0.17) + ((token_age_percentile - 0.5) * 0.04) + ((log_price_percentile - 0.5) * 0.17) + ((log_market_cap_percentile - 0.5) * 0.09) + ((price_volatility_30d_percentile - 0.5) * 0.47)  ]
publish[, d.2 := ((days.between.swaps.pct.0_percentile - 0.5) * 0.08) + ((days.with.25.swappers_percentile - 0.5) * 0.01) + ((n.users.lp.30_percentile - 0.5) * 0.06) + ((net.users.lp.ratio.7_percentile - 0.5) * 0.01) ]

publish[, stability_rank := rank(pred)]
publish.cols <- c( publish.cols, 'd.1', 'd.2', 'stability_rank' )

publish.cols <- c( 'symbol','contract_address','token_label','price','market_cap', publish.cols )

farm_data <- publish[, ..publish.cols]

plot <- ggplot(data=farm_data, aes(x = d.1, y = d.2)) +
	xlab('Token Activity') +
	ylab('Token Stability') +
	geom_point()

head(farm_data[order(pred), list(symbol, pred)])
tail(farm_data[order(pred), list(symbol, pred)])


data <- as.data.frame(head(farm_data[ symbol=='TRIBE' , list(pct_balance_cex_percentile, gini_percentile, token_age_percentile, log_price_percentile, log_market_cap_percentile, price_volatility_30d_percentile, days.between.swaps.pct.0_percentile, days.with.25.swappers_percentile, n.users.lp.30_percentile, net.users.lp.ratio.7_percentile)], 1))
data <- rbind(rep(1, length(data)), rep(0,length(data)), data)
radarchart(data, axistype=1, pcol=rgb(0.2,0.5,0.5,0.9), pfcol=rgb(0.2,0.5,0.5,0.5), plwd=4, cglcol="grey", cglty=1, axislabcol="grey", caxislabels=seq(0,20,5), cglwd=0.8)

get_label <- function(x) {
	if (x == 'net.users.lp.ratio.7') return('LP ratio')
	if (x == 'days.with.25.swappers') return('Sustained swapping')
	if (x == 'token_age') return('Token age')
	if (x == 'pct_balance_cex') return('CEX token balances')
	if (x == 'n.users.lp.30') return('LP popularity')
	if (x == 'days.between.swaps.pct.0') return('Token turnover')
	if (x == 'market_cap') return('Token market cap')
	if (x == 'price') return('Token price')
	if (x == 'gini') return('Gini coefficient')
	if (x == 'price_volatility_30d') return('Recent price volatility')
	return("")
}
feature_importance$label <- lapply(feature_importance$feature, get_label)
feature_importance[, label := mapply(get_label, feature)]
feature_importance <- feature_importance[order(-importance)]

# farm_data[ d.1 < -1, d.1 := -1]
# farm_data[ d.2 < -1, d.2 := -1]
# farm_data[symbol == 'ARTEON']
farm_data[, token_age:=as.integer(token_age)]


cur_balance <- balance[, head(.SD, 1), by = list(contract_address, address)]
cur_balance[ is.na(project_name), project_name := 'wallet' ]
cur_balance <- cur_balance[balance > 0] %>% group_by(contract_address, project_name) %>% summarize(balance=sum(balance)) %>% as.data.table()
cur_balance[, rk:=rank(-balance), by=contract_address]
cur_balance[, tot:=sum(balance), by=contract_address]
cur_balance[, pct:=balance / tot]
head(cur_balance[rk > 10])
cur_balance[ rk > 10 & pct < 0.01 & project_name != 'wallet', project_name := 'other project' ]
cur_balance <- cur_balance %>% group_by( contract_address, project_name ) %>% summarize(pct=sum(pct)) %>% as.data.table()

head(cur_balance[ !is.na(project_name) | !is.na(l1_label)  | !is.na(l2_label) ])
head(cur_balance[ grp == 'Other Project Contract' ])
g <- cur_balance[ grp == 'Other Project Contract' ] %>% group_by(project_name) %>% summarize(n=n()) %>% as.data.table()
g <- cur_balance %>% group_by(project_name) %>% summarize(n=n()) %>% as.data.table()
g <- g[order(-n)]
head(g)
g <- cur_balance %>% group_by(project_name) %>% summarize(n=n()) %>% as.data.table()
g <- g[order(-n)]
head(g)
unique(cur_balance[ !is.na(project_name) | !is.na(l1_label)  | !is.na(l2_label) ]$grp)
cur_balance <- cur_balance[order(-balance)]


cur_ownership <- tx[ tx_type != 'LP' & address != '0x0000000000000000000000000000000000000000'  & address != '0x000000000000000000000000000000000000dead' ] %>% group_by( contract_address, address ) %>% summarize(net_amt=sum(net_amt)) %>% as.data.table()
cur_ownership <- merge( cur_ownership, address_labels, all.x=T )
cur_ownership <- cur_ownership[order(contract_address, -net_amt)]
cur_ownership[, rk := rank(-net_amt), by=contract_address]
cur_ownership <- cur_ownership[ rk <= 10, list(contract_address, address, net_amt)]
length(unique(cur_ownership$address)) / length(unique(cur_ownership$contract_address))
wallet_amt <- tx %>% group_by( contract_address, address ) %>% summarize(wallet_amt=sum(net_amt)) %>% as.data.table()
wallet_amt[, amount:=pmax(0, wallet_amt)]
wallet_amt[, other_label:='wallet']

tmp_tx <- merge(tx, cur_ownership[, list(contract_address, address)])
tmp <- tmp_tx[ tx_type == 'LP' & !is.na(other_label) ] %>% group_by(contract_address, address, other_label) %>% summarize(net_amt=sum(net_amt)) %>% as.data.table()
tmp <- tmp[ net_amt < 0 ]
tmp[, amount:=-net_amt]

tmp <- rbind(
	wallet_amt[, list(contract_address, address, other_label, amount)]
	, tmp[, list(contract_address, address, other_label, amount)]
)
tmp[, tot:=sum(amount), by=list(contract_address, address)]
tmp[, pct:=amount / tot]
colnames(tmp)[3] <- 'label'
head(tmp)

top_holders <- merge(tmp, cur_ownership[, list(contract_address, address)])
length(unique(top_holders$address)) / length(unique(top_holders$contract_address))


save(feature_importance, file = "~/data_science/viz/token_tracker/feature_importance.RData")
save(farm_data, feature_importance, cur_balance, top_holders, file = "~/data_science/viz/token_tracker/farm_data.RData")
save(rf, file = "~/data_science/viz/token_tracker/random_forest_model.RData")


