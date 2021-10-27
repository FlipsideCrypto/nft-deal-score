source("~/data_science/util/util_functions.R")
library('DescTools')
library('zoo')
library('tidyr')


TOKENS <- c(
	'0xdbdb4d16eda451d0503b854cf79d55697f90c8df'  # ALCX
	,'0x956f47f50a910163d8bf957cf5846d573e7f87ca' # FEI
	,'0xc7283b66eb1eb5fb86327f08e1b5816b0720212b' # TRIBE
	# ,'0xcc8fa225d80b9c7d42f96e9570156c65d6caaa25' # SLP
	# ,'0xbb0e17ef65f82ab018d8edd776e8dd940327b28b' # AXS
	# ,'0xa2b4c0af19cc16a6cfacce81f192b024d625817d' # MEME (KISHU INU)
	# ,'0x77fba179c79de5b7653f68b5039af940ada60ce0' # Ampleforth Governance
	# ,'0x761d38e5ddf6ccf6cf7c55759d5210750b5d60f3' # Dogelon
	,'0x853d955acef822db058eb8505911ed77f175b99e' # FRAX
	,'0x896e145568624a498c5a909187363ae947631503' # WASABIX

	# ,'0x00a8b738e453ffd858a7edf03bccfe20412f0eb0' # AllianceBlock
	# ,'0x8888801af4d980682e47f1a9036e589479e835c5' # 88mph
	# ,'0x67c597624b17b16fb77959217360b7cd18284253' # BenchMark
	# ,'0xd23ac27148af6a2f339bd82d0e3cff380b5093de' # Siren
)
TOKEN_LIST <- paste0("'",paste(TOKENS, collapse="','"),"'")



ptm <- proc.time()
query <- paste0("
WITH base AS (
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
	WHERE b.address <> '0x000000000000000000000000000000000000dead'
		AND b.address <> '0x0000000000000000000000000000000000000000'
		AND date >= '2020-01-01'
		AND b.contract_address IN (",TOKEN_LIST,")
)
SELECT * FROM base WHERE rn = 1
")
daily_balance <- QuerySnowflake({query})
# g[contract_address == '0xbb0e17ef65f82ab018d8edd776e8dd940327b28b']
proc.time() - ptm
# unique(daily_balance[, list(project_name, l1_label, l2_label)])
# g <- daily_balance %>% group_by( contract_address, address, grp, project_name, l1_label, l2_label ) %>% summarize( balance=max(balance) ) %>% as.data.table()
# g <- g[order(contract_address, -balance)]
# g <- g[ !is.na(project_name) | !is.na(l1_label) | !is.na(l2_label) ]
# g[, tot := sum(balance), by=list(contract_address) ]
# g[, pct := balance / tot ]
# write.csv( g, '~/Downloads/tmp.csv', row.names=F )


calendar <- unique( daily_balance[, list(date)] )
addys <- unique( daily_balance[, list(contract_address, address)] )
calendar[, m:=1]
addys[, m:=1]

m <- merge( calendar, addys, by=.EACHI, allow.cartesian=T )
m <- merge( m, daily_balance[, list( contract_address, address, date, balance )], all.x=T, by=c('contract_address','address','date') )
m[, cur_balance:=zoo::na.locf(balance, na.rm = FALSE), c('contract_address', 'address')]
m <- m[ !is.na(cur_balance) ]
m <- merge( m, unique(daily_balance[, list(address, grp)]), by=c('address') )

g <- m %>% group_by( contract_address, date, grp ) %>% summarize( balance=sum(cur_balance) ) %>% as.data.table()
a <- unique( g[, list(date)] )
b <- unique( g[, list(contract_address, grp)] )
a[, m := 1]
b[, m := 1]
c <- merge( a, b, by=.EACHI, allow.cartesian=T )
c <- merge( c, g, all.x=T, by=c( 'contract_address','date','grp' ) )
c[is.na(c)] <- 0

daily_token_balance_groups <- c %>%
  ungroup() %>%
  drop_na() %>%
  complete(contract_address, date, grp, fill = list(`n()` = 0)) %>%
  as.data.table()





# load("~/data_science/viz/token_tracker/train_data.RData")

transactions <- transactions[ address %in% TOKENS ]
top5 <- top5[ contract_address %in% TOKENS ]
colnames(top5)[1] <- 'address'


query <- paste0("WITH base AS (
	SELECT UPPER(symbol) AS symbol
	, token_address AS address
	FROM ethereum.token_prices_hourly_v2
	WHERE hour >= CURRENT_DATE - 7
	AND token_address IN (",TOKEN_LIST,")
	GROUP BY 1, 2
	HAVING COUNT(1) > 10
)
SELECT *, CONCAT(symbol, ' (', address, ')') AS token_name
FROM base
ORDER BY 3 ASC
")
tokens <- QuerySnowflake({query})
# swap volume
# tx volume

query <- paste0("
	SELECT token_address AS address
	, DATE_TRUNC(day, hour) AS date
	, AVG(price) AS price
	FROM ethereum.token_prices_hourly_v2
	WHERE token_address IN (",TOKEN_LIST,")
	GROUP BY 1, 2
	ORDER BY 1, 2 DESC
")
prices <- QuerySnowflake({query})


########################
ptm <- proc.time()
query <- paste0("
	WITH valid AS (
		SELECT contract_address
		FROM ethereum.udm_events
		WHERE amount > 0
		GROUP BY 1
		HAVING MIN(block_timestamp) >= '2021-01-01'
			AND COUNT(DISTINCT tx_id) >= 50
			AND contract_address IN (",TOKEN_LIST,")

	), swaps AS (
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
	), lps AS (
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
	), base AS (
		SELECT 
		u.tx_id
		, c.n_events
		, CASE 
			WHEN to_label = from_address AND to_label IS NOT NULL THEN 'Intra-Protocol Transfer'
			WHEN origin_function_name = 'redeem' THEN 'Airdrop'
			WHEN s.tx_id IS NOT NULL 
				OR origin_function_name IN ( 'swap','swapETHForExactTokens','swapExactETHForTokens','swapExactTokensForETH','swapExactTokensForTokens','swapTokensForExactTokens' ) 
				THEN 'Swap'
			WHEN l.tx_id IS NOT NULL THEN 'LP'
			WHEN origin_function_name IN ('addLiquidity','deposit','removeLiquidity','removeLiquidityWithPermit','removeLiquidity','withdrawAll','withdraw') THEN 'LP'
			WHEN origin_function_name = 'purchase' AND from_label IS NOT NULL THEN 'Purchase'
			WHEN origin_function_name IN ('borrow','repayBorrow') THEN 'Borrowing'
			WHEN COALESCE( to_label_type, from_label_type ) = 'cex' THEN 'CEX'
			WHEN l.tx_id IS NULL AND COALESCE( to_label_type, from_label_type ) = 'dex' THEN 'Swap'
			WHEN u.origin_function_signature = '0xb7e93121' THEN 'Burn'
			WHEN c.n_events = 1 AND origin_function_name = 'transfer' THEN 'Direct Transfer'
			WHEN r.tx_id IS NOT NULL AND from_label IS NOT NULL THEN 'Reward'
			ELSE 'Other'
		END AS tx_type
		, u.origin_function_name
		, u.block_timestamp
		, DATE_TRUNC(day, u.block_timestamp) AS date
		, u.contract_address
		, u.symbol
		, u.amount
		, u.to_address
		, u.to_label
		, u.to_label_type
		, u.from_address
		, u.from_label
		, u.from_label_type
		, COALESCE(u.to_label, u.from_label) AS exchange
		FROM ethereum.udm_events u
		LEFT JOIN cnt c ON c.tx_id = u.tx_id
		LEFT JOIN swaps s ON s.tx_id = u.tx_id AND s.contract_address = u.contract_address
		LEFT JOIN lps l ON l.tx_id = u.tx_id AND l.contract_address = u.contract_address
		LEFT JOIN rewards r ON r.tx_id = u.tx_id AND r.contract_address = u.contract_address
		JOIN valid v ON v.contract_address = u.contract_address
		WHERE u.amount > 0
			AND u.block_timestamp >= '2020-01-01'
			AND u.contract_address IN (",TOKEN_LIST,")
	), formatted AS (
		SELECT tx_id
		, contract_address
		, from_address AS address
		, from_label AS label
		, from_label_type AS label_type
		, to_address AS partner_address
		, to_label AS partner_label
		, to_label_type AS partner_label_type
		, tx_type
		, origin_function_name
		, block_timestamp
		, date
		, symbol
		, CASE WHEN tx_type IN ('LP','Borrowing') THEN 0 ELSE -amount END AS net_amt
		, -amount net_amt_all
		, exchange
		FROM base
		WHERE from_address <> '0x0000000000000000000000000000000000000000'
		UNION ALL
		SELECT tx_id
		, contract_address
		, to_address AS address
		, to_label AS label
		, to_label_type AS label_type
		, from_address AS partner_address
		, from_label AS partner_label
		, from_label_type AS partner_label_type
		, tx_type
		, origin_function_name
		, block_timestamp
		, date
		, symbol
		, CASE WHEN tx_type IN ('LP','Borrowing') THEN 0 ELSE amount END AS net_amt
		, amount net_amt_all
		, exchange
		FROM base
		WHERE to_address <> '0x0000000000000000000000000000000000000000'
	), summary AS (
		SELECT contract_address
		, address
		, date
		, SUM(net_amt) AS net_amt
		FROM formatted
		GROUP BY 1, 2, 3
	), calendar AS (
		SELECT contract_address, date
		FROM summary
		GROUP BY 1, 2
	), addys AS (
		SELECT contract_address, address
		FROM summary
		GROUP BY 1, 2
	), expanded AS (
		SELECT c.*
		, a.address
		FROM calendar c
		JOIN addys a ON a.contract_address = c.contract_address
	), joined AS (
		SELECT e.*
		, COALESCE(s.net_amt, 0) AS net_amt
		FROM expanded e
		LEFT JOIN summary s ON s.contract_address = e.contract_address
			AND s.address = e.address
			AND s.date = e.date
	), cumu AS (
		SELECT j.*
		, SUM(net_amt) OVER (PARTITION BY j.contract_address, j.address ORDER BY date ASC) AS cur_net_amt
		FROM joined j
		LEFT JOIN flipside_dev_db.silver.ethereum_address_labels l ON l.address = j.address
		WHERE l.address IS NULL
	), avg AS (
		SELECT contract_address
		, date
		, SUM(cur_net_amt) AS tot_amt
		FROM cumu
		WHERE cur_net_amt > 0
		GROUP BY 1, 2
	), top5_0 AS (
		SELECT contract_address
		, address
		, date
		, cur_net_amt
		, ROW_NUMBER() OVER (PARTITION BY contract_address, date ORDER BY cur_net_amt DESC) AS rn
		FROM cumu
	)
	SELECT a.*, address, cur_net_amt, cur_net_amt / tot_amt AS pct_ownership
	FROM avg a 
	LEFT JOIN top5_0 t ON t.contract_address = a.contract_address
		AND t.date = a.date
	WHERE rn <= 5
")
top5_detail <- QuerySnowflake({query})



ptm <- proc.time()
query <- paste0("
	WITH s AS (
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
	)
	SELECT 
	u.tx_id
	, CASE 
		WHEN to_label = from_address AND to_label IS NOT NULL THEN 'Intra-Protocol Transfer'
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
	, DATE_TRUNC(day, block_timestamp) AS date
	, contract_address
	, to_address
	, to_label
	, from_address
	, from_label
	, amount
	, amount_usd
	FROM ethereum.udm_events u
	LEFT JOIN cnt c ON c.tx_id = u.tx_id
	LEFT JOIN s ON s.tx_id = u.tx_id
	LEFT JOIN l ON l.tx_id = u.tx_id
	WHERE amount > 0
		AND block_timestamp >= CURRENT_DATE - 300
		AND contract_address IN (",TOKEN_LIST,")
")
transaction_detail <- QuerySnowflake({query})
proc.time() - ptm

head(top5_detail)
head(top5_detail[])

tmp <- transaction_detail[ (contract_address == '0xdbdb4d16eda451d0503b854cf79d55697f90c8df') & ((from_address == '0xc02ad7b9a9121fc849196e844dc869d2250df3a6') | (to_address == '0xc02ad7b9a9121fc849196e844dc869d2250df3a6')) ]
tmp[, net_amt := ifelse(to_address==from_address, 0, ifelse(from_address=='0xdbdb4d16eda451d0503b854cf79d55697f90c8df', -amount, amount)) ]

tmp %>% summarize( net_amt=sum(net_amt) )
tmp[tx_type=='LP'] %>% summarize( net_amt=sum(net_amt) )
tmp <- tmp[order(-date)]

write.csv(tmp, '~/Downloads/tmp.csv', row.names=F)

transaction_detail[ (contract_address == '0xdbdb4d16eda451d0503b854cf79d55697f90c8df') & ((from_address == '0xc02ad7b9a9121fc849196e844dc869d2250df3a6') | (to_address == '0xc02ad7b9a9121fc849196e844dc869d2250df3a6')) ]


save(daily_token_balance_groups, transactions, top5, tokens, prices, top5_detail, transaction_detail, file = "~/data_science/viz/token_tracker/farm_data.RData")
