--, LAG(cumu_net_amt, 1) OVER (PARTITION BY contract_address, date ORDER BY cumu_net_amt) AS prv_cumu_net_amt
--, (COALESCE(prv_cumu_net_amt, 0) + cumu_net_amt) / 2 AS h_b
, cumu_net_amt AS h_b
, (rn * avg_amt) - h_b AS h_a
, 1 / n_addys AS w
, h_a * w AS area_a
, h_b * w AS area_b


ptm <- proc.time()
query <- paste0("
	WITH s AS (
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
	), r AS (
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
			WHEN origin_function_name = 'redeem' THEN 'Airdrop'
			WHEN s.tx_id IS NOT NULL 
				OR origin_function_name IN ( 'swap','swapETHForExactTokens','swapExactETHForTokens','swapExactTokensForETH','swapExactTokensForTokens','swapTokensForExactTokens' ) 
				THEN 'Swap'
			WHEN l.tx_id IS NOT NULL THEN 'LP'
			WHEN origin_function_name IN ('addLiquidity','deposit') AND to_label IS NOT NULL THEN 'LP'
			WHEN origin_function_name IN ('removeLiquidity','removeLiquidityWithPermit','removeLiquidity','withdrawAll','withdraw') AND from_label IS NOT NULL THEN 'LP'
			WHEN origin_function_name = 'purchase' AND from_label IS NOT NULL THEN 'Purchase'
			WHEN origin_function_name IN ('borrow') AND from_label IS NOT NULL THEN 'Borrowing'
			WHEN origin_function_name IN ('repayBorrow') AND to_label IS NOT NULL THEN 'Borrowing'
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
		LEFT JOIN s ON s.tx_id = u.tx_id AND s.contract_address = u.contract_address
		LEFT JOIN l ON l.tx_id = u.tx_id AND l.contract_address = u.contract_address
		LEFT JOIN r ON r.tx_id = u.tx_id AND r.contract_address = u.contract_address
		WHERE u.amount > 0
			AND u.block_timestamp >= '2020-01-01'
			AND u.contract_address = '0xdbdb4d16eda451d0503b854cf79d55697f90c8df'
	)
	SELECT tx_id
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
	, contract_address
	, symbol
	, CASE WHEN tx_type IN ('LP','Borrowing') THEN 0 ELSE -amount END AS net_amt
	, -amount net_amt_all
	, exchange
	FROM base
	UNION ALL
	SELECT tx_id
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
	, contract_address
	, symbol
	, CASE WHEN tx_type IN ('LP','Borrowing') THEN 0 ELSE amount END AS net_amt
	, amount net_amt_all
	, exchange
	FROM base
")
user_transactions <- QuerySnowflake({query})


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
	), base AS (
		SELECT 
		u.tx_id
		, c.n_events
		, CASE 
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
		, origin_function_name
		, block_timestamp
		, contract_address
		, symbol
		, amount
		, to_address
		, to_label
		, to_label_type
		, from_address
		, from_label
		, from_label_type
		, COALESCE(to_label, from_label) AS exchange
		FROM ethereum.udm_events u
		LEFT JOIN cnt c ON c.tx_id = u.tx_id
		LEFT JOIN s ON s.tx_id = u.tx_id
		LEFT JOIN l ON l.tx_id = u.tx_id
		WHERE contract_address IN ( '0x956f47f50a910163d8bf957cf5846d573e7f87ca', '0xc7283b66Eb1EB5FB86327f08e1B5816b0720212B' )
			AND amount > 0
	), f AS (
		SELECT from_address AS address
		, contract_address
		, SUM(-amount) AS net_amt
		FROM base
		WHERE NOT tx_type IN ( 'Borrowing', 'LP' )
		GROUP BY 1, 2
	), t AS (
		SELECT to_address AS address
		, contract_address
		, SUM(amount) AS net_amt
		FROM base
		WHERE NOT tx_type IN ( 'Borrowing', 'LP' )
		GROUP BY 1, 2
	), final AS (
		SELECT COALESCE(f.address, t.address) AS address
		, COALESCE(f.contract_address, t.contract_address) AS contract_address
		, SUM(COALESCE(f.net_amt, 0) + COALESCE(t.net_amt, 0)) AS net_amt
		FROM f
		FULL OUTER JOIN t ON t.address = f.address
	    GROUP BY 1, 2
	)
	SELECT f.*
	, project_name
	, address_name
	, l1_label
	, l2_label
	FROM final f
	LEFT JOIN flipside_dev_db.silver.ethereum_address_labels l ON l.address = f.address
")
ownership <- QuerySnowflake({query})






query <- paste0("
WITH d AS (
	SELECT contract_address
	, MAX(balance_date) AS balance_date
	FROM ethereum.erc20_balances 
	WHERE balance_date >= CURRENT_DATE - 7
		AND contract_address IN ( '0x956f47f50a910163d8bf957cf5846d573e7f87ca', '0xc7283b66Eb1EB5FB86327f08e1B5816b0720212B' )
	GROUP BY 1
)
SELECT user_address AS address
, b.contract_address
, SUM(balance) AS balance
FROM ethereum.erc20_balances b 
JOIN d ON d.balance_date = b.balance_date 
WHERE balance > 0
	AND user_address <> '0x000000000000000000000000000000000000dead'
	AND b.contract_address IN ( '0x956f47f50a910163d8bf957cf5846d573e7f87ca', '0xc7283b66Eb1EB5FB86327f08e1B5816b0720212B' )
GROUP BY 1, 2
ORDER BY 2, 3 DESC
")
balance <- QuerySnowflake({query})

dt <- merge( ownership, balance, all=TRUE )
dt[is.na(dt)] <- 0
dt[, dff:=round(abs(net_amt - balance))]
dt <- dt[order(-net_amt)]
head(dt, 20)


nrow(swaps)
head(swaps)

head(swaps[ tx_type=='Other' ])
head(swaps[ (tx_type=='Other') & is.na(origin_function_name) & !is.na(exchange) ])

swaps[ (tx_type=='Other') & (to_address=='0x0000000000000000000000000000000000000000') ] %>%
	group_by( origin_function_name ) %>%
	summarize( n=n(), amount=sum(amount) ) %>%
	as.data.table()

head(swaps[ (to_label == 'genesis') ])
head(swaps[ (tx_type=='Other') & is.na(origin_function_name) & exchange=='uniswap' ])
head(swaps[ (tx_type=='Other') & origin_function_name=='exit' ])

g <- swaps %>% 
	group_by( tx_type ) %>%
	summarize( n=n(), amount=sum(amount) )


g <- swaps %>% 
	group_by( tx_type, origin_function_name ) %>%
	summarize( n=n(), amount=sum(amount) ) %>% 
	as.data.table()

g[order( tx_type, -n )]
g[order( -amount )]

head(swaps[ (origin_function_name == 'purchase') & (tx_type == 'Other') ][order(-amount)])

swaps[ (from_address=='0x5d6446880fcd004c851ea8920a628c70ca101117') | (to_address=='0x5d6446880fcd004c851ea8920a628c70ca101117') ]


SELECT origin_event_name, COUNT(1) AS n
FROM ethereum.udm_events
WHERE contract_address IN ( '0x956f47f50a910163d8bf957cf5846d573e7f87ca', '0xc7283b66Eb1EB5FB86327f08e1B5816b0720212B' )	
GROUP BY 1
ORDER BY 2 DESC



SELECT event_name, COUNT(1) AS n2
FROM ethereum.events_emitted
WHERE tx_to_label_type = 'dex'
GROUP BY 1 
ORDER BY 2 DESC

SELECT DISTINCT tx_id
FROM ethereum.events_emitted
WHERE event_name = 'UpdateLiquidityLimit'
LIMIT 100


WITH base AS (
	SELECT origin_function_name
	, to_address AS address
	, COALESCE(to_label, '') AS label
	, COALESCE(symbol, contract_address) AS token
	, amount AS amt
	FROM ethereum.udm_events
	WHERE tx_id = '0x2b037ac214c642fc9f367758c5b9fdd124092189a9f3a8b1d4aa50470c50a3e9'
	AND amount > 0
	UNION ALL
	SELECT origin_function_name
	, from_address AS address
	, COALESCE(from_label, '') AS label
	, COALESCE(symbol, contract_address) AS token
	, -amount AS amt
	FROM ethereum.udm_events
	WHERE tx_id = '0x2b037ac214c642fc9f367758c5b9fdd124092189a9f3a8b1d4aa50470c50a3e9'
	AND amount > 0
	)
SELECT origin_function_name, address, label, token, SUM(amt) AS amt
FROM base
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC


SELECT DISTINCT tx_id
FROM ethereum.events_emitted
WHERE event_name = 'RemoveLiquidityImbalance'
LIMIT 100

SELECT event_name, COUNT(1) AS nn
FROM ethereum.events_emitted
WHERE tx_to_label ilike '%curve%'
GROUP BY 1
ORDER BY 2 DESC

WITH base AS (
	SELECT DISTINCT tx_id
	FROM ethereum.events_emitted
	WHERE event_name = 'UpdateLiquidityLimit'
	AND tx_to_label ilike '%curve%'
	LIMIT 100
)
SELECT e.*
FROM ethereum.events_emitted e
JOIN base b ON b.tx_id = e.tx_id
ORDER BY e.tx_id, e.event_name

SELECT symbol, amount
FROM ethereum.udm_events
WHERE amount > 0
AND tx_id = ''

SELECT event_name, COUNT(1) AS nn


SELECT *
FROM ethereum.events_emitted
WHERE tx_id = '0x5296099349b3e77a8752c2fbee24a08fbacc8f950277876c405e52791235e484'


(
	'Deposit'
	, 'Withdrawal'
	, 'IncreaseLiquidity'
	, 'DecreaseLiquidity'
	, 'AddLiquidity'
	, 'RemoveLiquidityOne'
	, 'RemoveLiquidity'
	, 'LiquidityRemoved'
)

SELECT event_name, COUNT(1) AS n2
FROM ethereum.events_emitted
WHERE event_name ilike 'dex'
GROUP BY 1 
ORDER BY 2 DESC




SELECT COUNT(DISTINCT token_address)
FROM ethereum.token_prices_hourly_v2
WHERE hour >= CURRENT_DATE - 7