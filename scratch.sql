

WITH base AS (
  SELECT DISTINCT address, symbol
  FROM silver_crosschain.ntr
  WHERE hodl = 0
  AND symbol = 'SUSHI'
  AND reward > 0
), h AS (
  SELECT DISTINCT address, symbol
  FROM silver_crosschain.ntr
  WHERE hodl = reward
  AND symbol = 'SUSHI'
)
SELECT *
FROM silver_crosschain.ntr n
JOIN base b ON b.address = n.address AND b.symbol = n.symbol 
JOIN h ON h.address = n.address AND h.symbol = n.symbol 



SELECT *
FROM silver_crosschain.ntr
WHERE symbol = 'SUSHI' 
//AND address = '0xa677bdf3a32ca287419343a6a2057fd4362ea996'
//WHERE tx_id = 'C2C6997EBFA385F621FB2FA684FE62AA717719A833286385FB00FE8DB49CA1FF'
ORDER BY xfer_date, reward DESC
LIMIT 1000


-- amt, timeframe, grouping, token, tot, pct, hodl_pct, metric, is_flipside

WITH base AS (
  SELECT address
  , symbol
  , MIN(xfer_date) AS mn_date
  FROM silver_crosschain.ntr
  GROUP BY 1
)
SELECT n.address
, n.symbol
, DATEDIFF('days', mn_date, xfer_date) AS days
, reward
, hodl
, unlabeled_transfer
, stake
, cex_deposit
, nft_buy
, dex_swap
FROM silver_crosschain.ntr n
JOIN base b ON b.address = n.address AND b.symbol = n.symbol


-- Automatic row-sampling now built in.
-- As always, beware estimate errors from missing Flipside identical rows.
-- Check example block balances with the below URL format:
-- https://thornode.ninerealms.com/bank/balances/[address]?height=[block_id]


/*
--'Run Selected' to check the transfer events sum for just one block.
SELECT *
FROM thorchain.transfer_events
WHERE (CONCAT(to_address, from_address) LIKE '%thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt%')
  AND (block_id = 5205058)
*/

WITH
target AS
(
SELECT 
  TRIM('thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt') AS target_address,
  SPLIT('THOR.RUNE', '-')[0] AS target_asset,
  10000 AS target_rows 
  --For displaying full ranges in Flipside, this should be below 100000 (one hundred thousand).
  --In practice, there may be up to two extra rows per asset type (starting and ending non-zero balance estimates)
),
/*
thor1v8ppstuf6e3x0r4glqc68d5jqcs2tf38cg2q6y [Minter Module - THOR.RUNE Switcher (from IOU.RUNE) and Synth Minter/Burner Module ('Genesis address')]
thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt [Reserve Module]
thor17gw75axcnr8747pkanye45pnrwk7p9c3cqncsv [Bond Module]
thor1g98cy3n9mmjrpn0sxmn63lztelera37n8n67c0 [Pool Module]

thor160yye65pf9rzwrgqmtgav69n6zlsyfpgm9a7xk is the THORSwap affiliate fee wallet.
*/

SELECT SPLIT(pool_name, '-')[0] AS pool_name
, SUM(CASE WHEN from_asset LIKE '%/%' THEN -from_amount ELSE to_amount END) AS net_amt
FROM thorchain.swaps
WHERE (from_asset = 'THOR.RUNE' AND to_asset LIKE '%/%')
OR (to_asset = 'THOR.RUNE' AND from_asset LIKE '%/%')
group by 1

burn as (select 
  REGEXP_SUBSTR (pool_name,'[^\.]+',1,1)  as chain,
--  REGEXP_SUBSTR (pool_name,'[^\.]+',1,2)  as assets,     
  count(distinct tx_id) as NoOfBURNTransactions,
  sum(to_amount) as amountBURN,
  sum(to_amount_usd) as AmountBURNUSD
from thorchain.swaps
WHERE 
to_asset = 'THOR.RUNE' AND from_asset LIKE '%/%' 
group by 1)




WITH a AS (
  SELECT from_address AS address
  , pool_name
  , block_timestamp::date AS date
  , SUM( CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) AS lp_units
  FROM thorchain.liquidity_actions 
  GROUP BY 1, 2, 3
), cumu1 AS (
  SELECT address
  , pool_name
  , date
  , lp_units
  , SUM(lp_units) OVER (PARTITION BY address, pool_name ORDER BY date) AS cumu_units
  FROM a
), cumu AS (
  SELECT *
  , cumu_units - lp_units AS prv_units
  FROM cumu1
), mn1 AS (
  SELECT address
  , pool_name
  , MAX(date) AS date
  FROM cumu
  WHERE prv_units <= 0
  GROUP BY 1, 2
  UNION ALL
  SELECT address
  , pool_name
  , MIN(date) AS date
  FROM a
  GROUP BY 1, 2
), mn AS (
  SELECT address
  , pool_name
  , MAX(date) AS date
  FROM mn1
  GROUP BY 1, 2
), cur AS (
  SELECT address
  , pool_name
  , SUM(lp_units) AS lp_units
  FROM a
  GROUP BY 1, 2
  HAVING SUM(lp_units) > 0
), tot AS (
  SELECT pool_name
  , SUM(lp_units) AS tot_units
  FROM cur
  GROUP BY 1
), tvl1 AS (
  SELECT pool_name
  , asset_amount_usd + rune_amount_usd AS tvl
  , ROW_NUMBER() OVER (PARTITION BY pool_name ORDER BY block_timestamp DESC) AS rn
  FROM thorchain.pool_block_balances
  WHERE block_timestamp >= CURRENT_DATE - 10
), tvl AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY tvl DESC) AS pool_rank
  FROM tvl1
  WHERE rn = 1
), base AS (
  SELECT c.address
  , SPLIT( c.pool_name, '-' )[0]::string AS pool_name
  , SPLIT( c.pool_name, '.' )[0]::string AS chain
  , c.lp_units / t.tot_units AS pct
  , mn.date
  , DATEDIFF('days', mn.date, CURRENT_TIMESTAMP) AS days
  , mn.date
  , tvl.tvl
  , tvl.pool_rank
  , pct * days AS w_days
  FROM cur c
  JOIN tot t ON t.pool_name = c.pool_name
  LEFT JOIN mn ON mn.pool_name = c.pool_name AND mn.address = c.address
  JOIN tvl ON tvl.pool_name = c.pool_name
)
SELECT pool_name
, chain
, tvl
, pool_rank
, SUM(pct) AS pct
, SUM(w_days) AS w_days
FROM base
GROUP BY 1, 2, 3, 4




with LP_ONE AS (
  SELECT
  DISTINCT ( from_address ) as wallet_added, pool_name, sum(stake_units) as amount_added
  FROM thorchain.liquidity_actions
  WHERE lp_action = 'add_liquidity'
  GROUP BY wallet_added,pool_name
),
LP_TWO AS (
    SELECT 
    DISTINCT(from_address) as wallet_removed, pool_name, sum(stake_units) as amount_removed
  
  FROM thorchain.liquidity_actions
WHERE lp_action = 'remove_liquidity'
  GROUP BY wallet_removed,pool_name
),
LP_THREE AS (
    SELECT 
    x.pool_name, x.wallet_added, amount_added-ifnull(amount_removed,0) as remaining
  
  FROM LP_ONE x
FULL OUTER join LP_TWO y
    ON x.wallet_added = y.wallet_removed and x.pool_name = y.pool_name
WHERE remaining > 0
  GROUP BY x.pool_name,wallet_added,remaining
),
LP_FOUR AS (
    SELECT 
    block_timestamp::date as date, x.pool_name, x.wallet_added
  
    FROM LP_THREE x
join thorchain.liquidity_actions y 
    ON x.pool_name = y.pool_name and x.wallet_added = y.from_address
WHERE lp_action = 'add_liquidity'
),
LP_FIVE AS (
    SELECT
    pool_name, wallet_added, min (date) as first_added
  
  FROM LP_FOUR
  GROUP BY pool_name,wallet_added
)
SELECT 
    split(pool_name, '-')[0] as "Pool Name", avg(datediff(day, first_added, CURRENT_DATE)) as "Average LP day"
  
  FROM LP_FIVE
  GROUP BY "Pool Name"
  ORDER BY "Average LP day" ASC


with a as (
  select 
    from_address,
    pool_name,
    SUM(case when lp_action like 'add_liquidity' then stake_units else -1 * stake_units end) as lp_units
  from thorchain.liquidity_actions 
  where not from_address is null 
  group by 1,2  
), b as (
  select 
    pool_name  as p1,
    avg((rune_amount_usd + asset_amount_usd)/stake_units) as ppu
  from thorchain.liquidity_actions 
  where block_timestamp > CURRENT_DATE - 2
  group by 1
), c as (
  select 
    from_address,
    pool_name, 
    lp_units * ppu as liquidity_provided_usd,
    case when liquidity_provided_usd < 1000 then 'Shrimp'
      when liquidity_provided_usd < 10000 then 'Fish'
      when liquidity_provided_usd < 100000 then 'Shark'
      when liquidity_provided_usd < 1000000 then 'Whale' end as balance_group
  from a 
    left outer join b 
      ON p1 = pool_name 
  where not balance_group is null
), d as (
  select 
    from_address as fa1,
    pool_name,
    min(date_trunc('day', block_timestamp)) as min_day
  from thorchain.liquidity_actions 
  group by 1, 2 
), e as (
  select 
    from_address,
    c.pool_name,
    liquidity_provided_usd,
    DATEDIFF(week, min_day, CURRENT_DATE) as lp_age,
     balance_group
  from c 
    left outer join d on fa1 = from_address AND d.pool_name = c.pool_name
)

select
--  pool_name,
--  balance_group,
--- avg(lp_age) as avg_lp_age
    pool_name,
    SUM(lp_age * liquidity_provided_usd) / sum(liquidity_provided_usd) LPer_age 
from e 
where lp_age > 0
and liquidity_provided_usd > 0
group by 1


  
changes AS
(
SELECT block_timestamp, block_id, 
  SPLIT(asset, '-')[0] AS asset,
  (
  POWER(10,-8) * CASE
  WHEN from_address = (SELECT target_address FROM target) THEN -1 * amount_e8
  WHEN to_address = (SELECT target_address FROM target) THEN amount_e8
  ELSE 0 END
  ) AS change
FROM thorchain.transfer_events
WHERE (change <> 0) AND (from_address <> to_address)
),

blockchanges AS
( --To filter out cases where something flows through an address without changing its balance.
SELECT block_timestamp, block_id, asset,
  CAST(SUM(change) AS DECIMAL(17,8)) AS blockchange,
  FLOOR(blockchange) AS integers,
  CAST(blockchange - FLOOR(blockchange) AS DECIMAL(17,8)) AS decimals
FROM changes
GROUP BY block_timestamp, block_id, asset
HAVING blockchange <> 0
),

cumulative1 AS
(
SELECT DISTINCT block_timestamp, block_id, asset, 
  SUM(blockchange) OVER(PARTITION BY asset ORDER BY block_id ASC) AS balance_estimate,
  CAST(SUM(blockchange) OVER(PARTITION BY asset ORDER BY block_id ASC) AS DECIMAL(17,8)) AS balance_estimate_2,
  --Note that a CAST( AS DECIMAL(17,8)) here breaks decimals_estimate too, somehow.
  --Bewildering, since decimals_estimate does not use balance_estimate here.
  SUM(integers) OVER(PARTITION BY asset ORDER BY block_id ASC) AS integers_estimate1,
  SUM(decimals) OVER(PARTITION BY asset ORDER BY block_id ASC) AS decimals_estimate1,
  integers_estimate1 + FLOOR(decimals_estimate1) AS integers_estimate,
  CAST(decimals_estimate1 - FLOOR(decimals_estimate1) AS DECIMAL(17,8)) AS decimals_estimate
  --Note that this CAST is not necessary if selecting from cumulative1 directly, 
  --but is necessary if selecting from cumulative which selects from cumulative1.
FROM blockchanges
),
  
cumulative AS
(
SELECT DISTINCT block_timestamp, block_id, asset, 
  balance_estimate, balance_estimate_2, integers_estimate, decimals_estimate
FROM cumulative1
),

rowcounting AS
(
SELECT 
  COUNT(cumulative.*) AS rowcount,
  MAX(target_rows) AS target_rows
FROM cumulative, target
)

SELECT cumulative.*
FROM cumulative, rowcounting

WHERE block_id BETWEEN 4786560 AND 4786561 
--For checking particular blocks, commenting out the below QUALIFY section with slash-asterisk comments.  
--Note 4786560, 4786561 for balance_estimate inaccuracy.
/*
QUALIFY (
  (MOD((ROW_NUMBER() OVER(PARTITION BY asset ORDER BY block_id ASC)), CEIL(rowcount/target_rows)) = 0)  
  --Sampling for data-handling. COUNT(block_id)
  OR (ROW_NUMBER() OVER(PARTITION BY asset ORDER BY block_id ASC) = 1)  
  --Include the first balance estimate for each asset.
  OR (ROW_NUMBER() OVER(PARTITION BY asset ORDER BY block_id DESC) = 1)  
  --Include the last balance estimate for each asset.
  )
*/
ORDER BY block_id DESC




WITH a AS (
  SELECT from_address AS address
  , SUM(-rune_amount) AS amt
  FROM thorchain.transfers
  WHERE from_address = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt'
  AND block_id <= 4786561
  GROUP BY 1
  UNION ALL
  SELECT to_address AS address
  , SUM(rune_amount) AS amt
  FROM thorchain.transfers
  WHERE to_address = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt'
  AND block_id <= 4786561
  GROUP BY 1
)
SELECT address
, SUM(amt) AS amt
FROM a
GROUP BY 1
ORDER BY 2


WITH a AS (
  SELECT from_address AS address
  , SUM(-rune_amount) AS amt
  FROM flipside_dev_db.thorchain.transfers
  WHERE from_address = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt'
  AND block_id <= 4786561
  GROUP BY 1
  UNION ALL
  SELECT to_address AS address
  , SUM(rune_amount) AS amt
  FROM flipside_dev_db.thorchain.transfers
  WHERE to_address = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt'
  AND block_id <= 4786561
  GROUP BY 1
)
SELECT address
, SUM(amt) AS amt
FROM a
GROUP BY 1
ORDER BY 2

113254564
113254489


SELECT *
FROM BRONZE_MIDGARD_2_6_9_20220405.MIDGARD_BLOCK_LOG
WHERE height = 4786561

WITH a AS (
  SELECT from_addr AS address
  , SUM(-amount_e8) * POWER(10, -8) AS amt
  FROM bronze_midgard_2_6_9_20220405.midgard_transfer_events
  WHERE from_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt'
  AND block_timestamp <= 1647916734066108230
  GROUP BY 1
  UNION ALL
  SELECT to_addr AS address
  , SUM(amount_e8) * POWER(10, -8) AS amt
  FROM bronze_midgard_2_6_9_20220405.midgard_transfer_events
  WHERE to_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt'
  AND block_timestamp <= 1647916734066108230
  GROUP BY 1
)
SELECT address
, SUM(amt) AS amt
FROM a
GROUP BY 1
ORDER BY 2



WITH base AS (
  SELECT from_addr AS from_address, to_addr AS to_address, asset, amount_e8 * POWER(10, -8) AS rune_amount, TO_TIMESTAMP(block_timestamp) AS block_timestamp, COUNT(1) AS n
  FROM bronze_midgard_2_6_9_20220405.midgard_transfer_events
  WHERE (to_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' OR from_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt')
  GROUP BY 1, 2, 3, 4, 5
), gold AS (
  SELECT from_address, to_address, asset, rune_amount, block_timestamp, COUNT(1) AS n2
  FROM thorchain.transfer_events
  WHERE (to_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' OR from_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt')
  GROUP BY 1, 2, 3, 4, 5
)
SELECT b.*, g.n2
FROM base b
LEFT JOIN gold g 
  ON g.from_address = b.from_address 
  AND g.to_address = b.to_address
  AND g.asset = b.asset
  AND g.rune_amount = b.rune_amount
  AND g.block_timestamp = b.block_timestamp
  AND b.n <> COALESCE(g.n2, 0)


113254564
113254489



/*
--'Run Selected' to check the transfer events sum for just one block.
SELECT *
FROM thorchain.transfer_events
WHERE (CONCAT(to_address, from_address) LIKE '%thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt%')
  AND (block_id = 5205058)
*/

WITH
target AS
(
SELECT 
  TRIM('thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt') AS target_address,
  SPLIT('THOR.RUNE', '-')[0] AS target_asset,
  10000 AS target_rows 
  --For displaying full ranges in Flipside, this should be below 100000 (one hundred thousand).
  --In practice, there may be up to two extra rows per asset type (starting and ending non-zero balance estimates)
),
/*
thor1v8ppstuf6e3x0r4glqc68d5jqcs2tf38cg2q6y [Minter Module - THOR.RUNE Switcher (from IOU.RUNE) and Synth Minter/Burner Module ('Genesis address')]
thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt [Reserve Module]
thor17gw75axcnr8747pkanye45pnrwk7p9c3cqncsv [Bond Module]
thor1g98cy3n9mmjrpn0sxmn63lztelera37n8n67c0 [Pool Module]

thor160yye65pf9rzwrgqmtgav69n6zlsyfpgm9a7xk is the THORSwap affiliate fee wallet.
*/
  
changes AS
(
SELECT block_timestamp, block_id, 
  SPLIT(asset, '-')[0] AS asset,
  (
  POWER(10,-8) * CASE
  WHEN from_address = (SELECT target_address FROM target) THEN -1 * amount_e8
  WHEN to_address = (SELECT target_address FROM target) THEN amount_e8
  ELSE 0 END
  ) AS change
FROM thorchain.transfer_events
WHERE (change <> 0) AND (from_address <> to_address)
),

blockchanges AS
( --To filter out cases where something flows through an address without changing its balance.
SELECT block_timestamp, block_id, asset,
  CAST(SUM(change) AS DECIMAL(17,8)) AS blockchange,
  FLOOR(blockchange) AS integers,
  CAST(blockchange - FLOOR(blockchange) AS DECIMAL(17,8)) AS decimals
FROM changes
GROUP BY block_timestamp, block_id, asset
HAVING blockchange <> 0
),

cumulative1 AS
(
SELECT DISTINCT block_timestamp, block_id, asset, 
  SUM(blockchange) OVER(PARTITION BY asset ORDER BY block_id ASC) AS balance_estimate,
  --CAST(SUM(blockchange) OVER(PARTITION BY asset ORDER BY block_id ASC) AS DECIMAL(17,8)) AS balance_estimate,
  --Note that a CAST( AS DECIMAL(17,8)) here breaks decimals_estimate too, somehow.
  --Bewildering, since decimals_estimate does not use balance_estimate here.
  SUM(integers) OVER(PARTITION BY asset ORDER BY block_id ASC) AS integers_estimate1,
  SUM(decimals) OVER(PARTITION BY asset ORDER BY block_id ASC) AS decimals_estimate1,
  integers_estimate1 + FLOOR(decimals_estimate1) AS integers_estimate,
  CAST(decimals_estimate1 - FLOOR(decimals_estimate1) AS DECIMAL(17,8)) AS decimals_estimate
  --Note that this CAST is not necessary if selecting from cumulative1 directly, 
  --but is necessary if selecting from cumulative which selects from cumulative1.
FROM blockchanges
),
  
cumulative AS
(
SELECT DISTINCT block_timestamp, block_id, asset, 
  balance_estimate, integers_estimate, decimals_estimate
FROM cumulative1
),

rowcounting AS
(
SELECT 
  COUNT(cumulative.*) AS rowcount,
  MAX(target_rows) AS target_rows
FROM cumulative, target
)

SELECT cumulative.*
FROM cumulative, rowcounting

WHERE block_id BETWEEN 4786560 AND 4786561 
--For checking particular blocks, commenting out the below QUALIFY section with slash-asterisk comments.  
--Note 4786560, 4786561 for balance_estimate inaccuracy.
/*
QUALIFY (
  (MOD((ROW_NUMBER() OVER(PARTITION BY asset ORDER BY block_id ASC)), CEIL(rowcount/target_rows)) = 0)  
  --Sampling for data-handling. COUNT(block_id)
  OR (ROW_NUMBER() OVER(PARTITION BY asset ORDER BY block_id ASC) = 1)  
  --Include the first balance estimate for each asset.
  OR (ROW_NUMBER() OVER(PARTITION BY asset ORDER BY block_id DESC) = 1)  
  --Include the last balance estimate for each asset.
  )
*/
ORDER BY block_id DESC


SELECT block_timestamp::date AS date
, SUM(il_protection_usd) AS il_protection_usd
FROM THORCHAIN.LIQUIDITY_ACTIONS
WHERE il_protection_usd > 0
GROUP BY 1


WITH a AS (
  SELECT from_address AS address
  , SUM(-rune_amount) AS amt
  FROM thorchain.transfers
  GROUP BY 1
  UNION ALL
  SELECT to_address AS address
  , SUM(rune_amount) AS amt
  FROM thorchain.transfers
  GROUP BY 1
)
SELECT address
, SUM(amt) AS amt
FROM a
GROUP BY 1
ORDER BY 2


SELECT *
FROM thorchain.prices
ORDER BY block_timestamp DESC
LIMIT 100

SELECT split(memo, ':')[1]::string AS address
, *
FROM thorchain.bond_events
WHERE address = 'thor1xd4j3gk9frpxh8r22runntnqy34lwzrdkazldh'



SELECT split(memo, ':')[1]::string AS address
, *
FROM thorchain.bond_events

    

SELECT split(memo, ':')[1]::string AS address
, *
FROM thorchain.bond_events
WHERE address = 'thor1xd4j3gk9frpxh8r22runntnqy34lwzrdkazldh'


SELECT COUNT(1)
FROM midgard.bond_events



SELECT split(memo, ':')[1]::string AS address
, split(to_address, '\n') AS detail
, e8 * POWER(10, -8) AS amt
, asset_e8 * POWER(10, -8) AS asset_amt
, CASE WHEN bond_type = 'bond_paid' THEN amt ELSE -amt END AS net_amt
, SUM(net_amt) OVER (ORDER BY block_timestamp, net_amt DESC) AS cumu_net_amt
, *
FROM thorchain.bond_events
WHERE (
  address = 'thor12qwtrq4njj2s29gq56jun43dvxalejaksptqqn' 
  OR from_address like '%thor12qwtrq4njj2s29gq56jun43dvxalejaksptqqn%'
  OR to_address like '%thor12qwtrq4njj2s29gq56jun43dvxalejaksptqqn%'
)
ORDER BY block_timestamp

SELECT split(memo, ':')[1]::string AS address
, SUM(CASE WHEN bond_type = 'bond_paid' THEN e8 ELSE -e8 END) * POWER(10, -8)  AS amt
FROM thorchain.bond_events
WHERE bond_type IN ('bond_paid','bond_returned')
GROUP BY 1


SELECT *
FROM thorchain.update_node_account_status_events
WHERE node_address = 'thor100dyywzrxaqsrlamkd7ssspc8ppf46306farkz'


SELECT day
, bonding_earnings
, liquidity_earnings
FROM thorchain.block_rewards



WITH s AS (
  SELECT node_address AS address
  , current_status
  , block_timestamp::date AS date
  , ROW_NUMBER() OVER (PARTITION BY node_address, date ORDER BY block_timestamp DESC) AS rn
  FROM thorchain.update_node_account_status_events
), a AS (
  SELECT split(memo, ':')[1]::string AS address
  , block_timestamp::date AS date
  , SUM(CASE WHEN bond_type = 'bond_paid' THEN e8 ELSE -e8 END) * POWER(10, -8)  AS amt
  FROM thorchain.bond_events
  WHERE bond_type IN ('bond_paid','bond_returned')
  GROUP BY 1, 2
), base AS (
  SELECT a.*, SUM(amt) OVER (PARTITION BY address ORDER BY date) AS cumu_amt
  FROM a
), calendar AS (
  SELECT DISTINCT date FROM base
  UNION
  SELECT DISTINCT date FROM s
), joined AS (
  SELECT c.date
  , b.date AS amt_date
  , b.address
  , b.cumu_amt
  , ROW_NUMBER() OVER (PARTITION BY address, c.date ORDER BY b.date DESC) AS rn
  FROM calendar c
  JOIN base b ON b.date <= c.date
), j2 AS (
  SELECT j.address
  , j.date
  , date_trunc('month', j.date) AS month
  , j.amt_date
  , s.date AS status_date
  , j.cumu_amt
  , s.current_status
  , ROW_NUMBER() OVER (PARTITION BY j.address, j.date ORDER BY s.date DESC) AS rn
  FROM joined j
  JOIN s ON s.address = j.address AND s.date <= j.date
  WHERE j.rn = 1
), b2 AS (
  SELECT * FROM j2 
  WHERE rn = 1
  AND current_status = 'Active'
), b3 AS (
  SELECT address
  , current_status
  , month
  , cumu_amt
  , ROW_NUMBER() OVER (PARTITION BY address, month ORDER BY date DESC) AS rn
  FROM j2
  WHERE rn = 1
), pooled AS (
  SELECT pool_name
  , date_trunc('month', block_timestamp) AS month
  , rune_amount AS pooled_rune
  , ROW_NUMBER() OVER (PARTITION BY pool_name, month ORDER BY block_timestamp DESC) AS rn
  FROM thorchain.pool_block_balances
), bonded AS (
  SELECT month
  , SUM(cumu_amt) AS active_bonded_rune
  FROM b3
  WHERE rn = 1 AND current_status = 'Active'
  GROUP BY 1
), p AS (
  SELECT month
  , SUM(pooled_rune) AS pooled_rune
  FROM pooled
  WHERE rn = 1
  GROUP BY 1
), rewards AS (
  SELECT date_trunc('month', day::date) AS month
  , SUM(bonding_earnings::float) AS bonding_earnings
  , SUM(liquidity_earnings::float) AS liquidity_earnings
  FROM thorchain.block_rewards
  WHERE day != '2021-07-15'
  GROUP BY 1
)
SELECT p.*
, b.active_bonded_rune, b.active_bonded_rune / p.pooled_rune AS bonded_to_pooled_ratio
, bonding_earnings / b.active_bonded_rune AS bonded_rewards_ratio
, liquidity_earnings / b.pooled_rune AS lp_rewards_ratio
FROM p
JOIN bonded b ON b.month = p.month
JOIN rewards r ON r.month = p.month




WITH s AS (
  SELECT node_address AS address
  , current_status
  , block_timestamp::date AS date
  , ROW_NUMBER() OVER (PARTITION BY node_address, date ORDER BY block_timestamp DESC) AS rn
  FROM thorchain.update_node_account_status_events
  -- WHERE address = 'thor12fw3syyy4ff78llh3fvhrvdy7xnqlegvru7seg'
), a AS (
  SELECT split(memo, ':')[1]::string AS address
  , block_timestamp::date AS date
  , SUM(CASE WHEN bond_type = 'bond_paid' THEN e8 ELSE -e8 END) * POWER(10, -8)  AS amt
  FROM thorchain.bond_events
  WHERE bond_type IN ('bond_paid','bond_returned')
  -- AND address = 'thor12fw3syyy4ff78llh3fvhrvdy7xnqlegvru7seg'
  GROUP BY 1, 2
), base AS (
  SELECT a.*, SUM(amt) OVER (PARTITION BY address ORDER BY date) AS cumu_amt
  FROM a
), calendar AS (
  SELECT DISTINCT date FROM base
  UNION
  SELECT DISTINCT date FROM s
), joined AS (
  SELECT c.date
  , b.date AS amt_date
  , b.address
  , b.cumu_amt
  , ROW_NUMBER() OVER (PARTITION BY address, c.date ORDER BY b.date DESC) AS rn
  FROM calendar c
  JOIN base b ON b.date <= c.date
), j2 AS (
  SELECT j.address
  , j.date
  , j.amt_date
  , s.date AS status_date
  , j.cumu_amt
  , s.current_status
  , ROW_NUMBER() OVER (PARTITION BY j.address, j.date ORDER BY s.date DESC) AS rn
  FROM joined j
  JOIN s ON s.address = j.address AND s.date <= j.date
  WHERE j.rn = 1
)
SELECT * FROM j2 
WHERE rn = 1





WITH s AS (
  SELECT node_address AS address
  , current_status
  , block_timestamp::date AS date
  , ROW_NUMBER() OVER (PARTITION BY node_address, date ORDER BY block_timestamp DESC) AS rn
  FROM thorchain.update_node_account_status_events
), a AS (
  SELECT split(memo, ':')[1]::string AS address
  , block_timestamp::date AS date
  , SUM(CASE WHEN bond_type = 'bond_paid' THEN e8 ELSE -e8 END) * POWER(10, -8)  AS amt
  FROM thorchain.bond_events
  WHERE bond_type IN ('bond_paid','bond_returned')
  GROUP BY 1, 2
), base AS (
  SELECT a.*, SUM(amt) OVER (PARTITION BY address ORDER BY date) AS cumu_amt
  FROM a
), calendar AS (
  SELECT DISTINCT date FROM base
), joined AS (
  SELECT c.date
  , b.address
  , b.cumu_amt
  , ROW_NUMBER() OVER (PARTITION BY address ORDER BY b.date DESC) AS rn
  FROM calendar c
  JOIN base b ON b.date <= c.date
), j2 AS (
  SELECT j.address
  , j.date
  , j.cumu_amt
  , s.current_status
  , ROW_NUMBER() OVER (PARTITION BY j.address, j.date ORDER BY s.date DESC) AS rn
  FROM joined j
  JOIN s ON s.address = j.address AND s.date <= j.date
)
SELECT * FROM j2 WHERE rn = 1



WITH base AS (
  SELECT timestamp, height
  FROM thorchain.block_log
  WHERE timestamp = 1650397837878161529
)
SELECT t.*
FROM thorchain.transfer_events t
JOIN base b ON b.timestamp = t.block_timestamp
WHERE (from_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' OR to_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt')
AND from_addr <> to_addr



WITH base AS (
  SELECT split(memo, ':')[1]::string AS address
  , block_timestamp::date AS date
  , SUM(CASE WHEN bond_type = 'bond_paid' THEN e8 ELSE -e8 END) * POWER(10, -8)  AS amt
  FROM thorchain.bond_events
  WHERE address = 'thor1xd4j3gk9frpxh8r22runntnqy34lwzrdkazldh'
  AND bond_type IN ('bond_paid','bond_returned')
  GROUP BY 1, 2
)
SELECT date
, SUM(amt) OVER (ORDER BY date) AS cumu_bond
FROM base
ORDER BY 1

with inflow as (select to_address,
  sum(rune_amount) as rune_inflow
from thorchain.transfers
where asset = 'THOR.RUNE'
  group by 1),

outflow as (select from_address,
  sum(rune_amount) as rune_outflow
from thorchain.transfers
where asset = 'THOR.RUNE'
  group by 1),

holdings as (select to_address AS from_address,
  ifnull(rune_inflow, 0) - ifnull(rune_outflow, 0) as holding_rune
from inflow a 
full outer join outflow b 
  on a.to_address = b.from_address),

adding as (select 
  from_address,
  pool_name,
  sum(stake_units) as add_liq
  from thorchain.liquidity_actions
where
lp_action = 'add_liquidity'
  group by 1,2),

removing as (select 
  from_address,
  pool_name,
  sum(stake_units) as remove_liq
  from thorchain.liquidity_actions
where lp_action = 'remove_liquidity'
    group by 1,2),

stake_unitz as (select a.from_address,
  a.pool_name,
  (add_liq - ifnull(remove_liq,0)) / total_stake as percentage
  from adding a 
  left join removing b 
  on a.pool_name = b.pool_name and a.from_address = b.from_address
  join thorchain.pool_block_statistics c
  on a.pool_name = c.asset
where day in (select max(day) from thorchain.pool_block_statistics)
and percentage > 0),
--and lp_action = 'remove_liquidity'

rune_lped as (select a.from_address, 
percentage * rune_liquidity as rune_lping
from stake_unitz a 
join thorchain.daily_pool_stats b 
on a.pool_name = b.pool_name
where day in (select max(day) from thorchain.daily_pool_stats)),

lps as(select from_address,
sum(rune_lping) as total_rune_lped
from rune_lped 
group by 1),

base as (select 
  a.from_address,
  ifnull(holding_rune, 0) as rune_wallet,
  ifnull(total_rune_lped, 0) as rune_liquidity_pool,
  rune_wallet + rune_liquidity_pool as total_rune,
  case
  when total_rune <= 1 then 'total rune holdings <= 1'
  when total_rune > 1 and total_rune <= 10 then 'total rune holdings > 1 and <= 10'
  when total_rune > 10 and total_rune <= 100 then 'total rune holdings > 10 and <= 100'
  when total_rune > 100 and total_rune <= 1000 then 'total rune holdings > 100 and <= 1K'
  when total_rune > 1000 and total_rune <= 10000 then 'total rune holdings > 1K and <= 10K'
  when total_rune > 10000 and total_rune <= 100000 then 'total rune holdings > 10K and <= 100K'
  else 'total rune holdings > 100K'
  end as total_rune_ranges,
  case 
    when total_rune < -10 then '-10'
    when total_rune < -1 then '-1'
    when total_rune < 0 then '-0'
    when total_rune <= 1 then '0'
  when total_rune > 1 and total_rune <= 10 then '1'
  when total_rune > 10 and total_rune <= 100 then '2'
  when total_rune > 100 and total_rune <= 1000 then '3'
  when total_rune > 1000 and total_rune <= 10000 then '4'
  when total_rune > 10000 and total_rune <= 100000 then '5'
  else '9'
  end as ordering
from holdings a 
left join lps b
on a.from_address = b.from_address
  where (total_rune > 1) )

select *
from base 


SELECT *
FROM thorchain.swap_events
WHERE memo IS NOT NULL

SELECT split(memo, ':')[1] AS asset
, split(memo, ':')[3] AS lim
, split(memo, ':')[4] AS affiliate
, COUNT(1) AS n
FROM thorchain.swap_events s
WHERE block_timestamp >= CURRENT_DATE - 20
AND s.memo IS NOT NULL
GROUP BY 1, 2, 3

SELECT RIGHT(split(memo, ':')[3], 3)
, COUNT(1) AS n
FROM thorchain.swap_events s
WHERE block_timestamp >= CURRENT_DATE - 20
AND s.memo IS NOT NULL
AND from_address = 'thor1yep703ewakef0h2l9qel93xh96tvkm004pesq7'
GROUP BY 1
ORDER BY 2 DESC

SELECT RIGHT(split(memo, ':')[3], 3)
, COUNT(1) AS n
FROM thorchain.swap_events s
WHERE block_timestamp >= CURRENT_DATE - 20
AND s.memo IS NOT NULL
AND from_address = 'thor1yep703ewakef0h2l9qel93xh96tvkm004pesq7'
LIMIT 100

SELECT from_address
, COUNT(1) AS n
, AVG(CASE WHEN RIGHT(split(memo, ':')[3], 3) = '000' THEN 1 ELSE 0 END ) AS pct_000
FROM thorchain.swap_events s
WHERE block_timestamp >= CURRENT_DATE - 20
AND s.memo IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC

SELECT
RIGHT(split(memo, ':')[3], 3) AS lim
, COUNT(1) AS n
FROM thorchain.swap_events s
WHERE block_timestamp >= CURRENT_DATE - 20
AND s.memo IS NOT NULL
GROUP BY 1

SELECT
LEFT(split(memo, ':')[4], 10) AS aff
, COUNT(1) AS n
FROM thorchain.swap_events s
WHERE block_timestamp >= CURRENT_DATE - 20
AND s.memo IS NOT NULL
GROUP BY 1




with inflow as (select to_address,
  sum(rune_amount) as rune_inflow
from thorchain.transfers
where asset = 'THOR.RUNE'
  group by 1),

outflow as (select from_address,
  sum(rune_amount) as rune_outflow
from thorchain.transfers
where asset = 'THOR.RUNE'
  group by 1),

holdings as (select from_address,
  ifnull(rune_inflow, 0) - ifnull(rune_outflow, 0) as holding_rune
from inflow a 
full outer join outflow b 
  on a.to_address = b.from_address),

adding as (select 
  from_address,
  pool_name,
  sum(stake_units) as add_liq
  from thorchain.liquidity_actions
where
lp_action = 'add_liquidity'
  group by 1,2),

removing as (select 
  from_address,
  pool_name,
  sum(stake_units) as remove_liq
  from thorchain.liquidity_actions
where lp_action = 'remove_liquidity'
    group by 1,2),

stake_unitz as (select a.from_address,
  a.pool_name,
  (add_liq - ifnull(remove_liq,0)) / total_stake as percentage
  from adding a 
  left join removing b 
  on a.pool_name = b.pool_name and a.from_address = b.from_address
  join thorchain.pool_block_statistics c
  on a.pool_name = c.asset
where day in (select max(day) from thorchain.pool_block_statistics)
and percentage > 0),
--and lp_action = 'remove_liquidity'

rune_lped as (select a.from_address, 
percentage * rune_liquidity as rune_lping
from stake_unitz a 
join thorchain.daily_pool_stats b 
on a.pool_name = b.pool_name
where day in (select max(day) from thorchain.daily_pool_stats)),

lps as(select from_address,
sum(rune_lping) as total_rune_lped
from rune_lped 
group by 1),

base as (select 
  a.from_address,
  ifnull(holding_rune, 0) as rune_wallet,
  ifnull(total_rune_lped, 0) as rune_liquidity_pool,
  rune_wallet + rune_liquidity_pool as total_rune,
  case
  when total_rune <= 1 then 'total rune holdings <= 1'
  when total_rune > 1 and total_rune <= 10 then 'total rune holdings > 1 and <= 10'
  when total_rune > 10 and total_rune <= 100 then 'total rune holdings > 10 and <= 100'
  when total_rune > 100 and total_rune <= 1000 then 'total rune holdings > 100 and <= 1K'
  when total_rune > 1000 and total_rune <= 10000 then 'total rune holdings > 1K and <= 10K'
  when total_rune > 10000 and total_rune <= 100000 then 'total rune holdings > 10K and <= 100K'
  else 'total rune holdings > 100K'
  end as total_rune_ranges,
  case 
    when total_rune < -10 then '-10'
    when total_rune < -1 then '-1'
    when total_rune < 0 then '-0'
    when total_rune <= 1 then '0'
  when total_rune > 1 and total_rune <= 10 then '1'
  when total_rune > 10 and total_rune <= 100 then '2'
  when total_rune > 100 and total_rune <= 1000 then '3'
  when total_rune > 1000 and total_rune <= 10000 then '4'
  when total_rune > 10000 and total_rune <= 100000 then '5'
  else '9'
  end as ordering
from holdings a 
left join lps b
on a.from_address = b.from_address
  where (total_rune > 0 OR TRUE) )

select total_rune_ranges,
ordering,
count(distinct(from_address)) as number_of_wallet
from base 
group by 1,2
order by 2

 


with transfers as (
select sum(rune_amount) as total_receive, to_address as wallet
from thorchain.transfers
where asset = 'THOR.RUNE'
group by 2
union 
select sum(rune_amount * -1) as total_receive, from_address as wallet
from thorchain.transfers
where asset = 'THOR.RUNE'
group by 2
),

first_in as (
select min(block_timestamp::date) as first_in, to_address
from thorchain.transfers
group by 2
),

all_holders as (
select sum(total_receive) as total_rune_holdings, wallet 
from transfers
group by 2
),

lp as (
select sum(rune_amount) as total_add, from_address as lp_address
from thorchain.liquidity_actions
where lp_action = 'add_liquidity'
group by 2
union 
select sum(rune_amount * -1) as total_withdraw, from_address as lp_address
from thorchain.liquidity_actions
where lp_action = 'remove_liquidity'
group by 2
),

total_in_pool as (
select sum(total_add) as total_rune_pool, lp_address 
from lp 
group by 2
),

rune_balance as (
select wallet, total_rune_holdings, total_rune_pool
from all_holders hol 
left join total_in_pool lp on 
hol.wallet = lp.lp_address

),

rune_holders_period as (
select wallet, total_rune_holdings, first_in 
from rune_balance run 
left join first_in fir on
run.wallet = fir.to_address
where total_rune_holdings > 0
),

holding_period as (
select count(distinct wallet) as num_holders, '<7 days' as holding_period
from rune_holders_period 
where first_in > current_date - 7
group by 2
union 
select count(distinct wallet) as num_holders, '7-30 days' as holding_period
from rune_holders_period
where first_in > current_date - 30 
and first_in < current_date - 7 
group by 2
union 
select count(distinct wallet) as num_holders, '30-90 days' as holding_period
from rune_holders_period
where first_in > current_date - 90 
and first_in < current_date - 30
group by 2
union 
select count(distinct wallet) as num_holders, '90-180 days' as holding_period
from rune_holders_period
where first_in > current_date - 180 
and first_in < current_date - 90 
group by 2
union 
select count(distinct wallet) as num_holders, '180-365 days' as holding_period
from rune_holders_period
where first_in > current_date - 365
and first_in < current_date - 180
group by 2
),

distribution_holder as (
select count(distinct wallet) as num_holders,
case when total_rune_holdings > 0 and total_rune_holdings < 1000 then 'shrimp'
when total_rune_holdings >= 1000 and total_rune_holdings < 10000 then 'fish'
when total_rune_holdings >= 10000 and total_rune_holdings < 100000 then 'small whale'
when total_rune_holdings >= 100000 and total_rune_holdings < 1000000 then 'medium whale'
when total_rune_holdings >= 1000000 then 'boss whale'
end as holding_size, 
case when total_rune_holdings > 0 and total_rune_holdings < 1000 then '1-1k rune'
when total_rune_holdings >= 1000 and total_rune_holdings < 10000 then '1k-10k rune'
when total_rune_holdings >= 10000 and total_rune_holdings < 100000 then '10k - 100k'
when total_rune_holdings >= 100000 and total_rune_holdings < 1000000 then '100k - 1M rune'
when total_rune_holdings >= 1000000 then '>1M rune'
  end as size_type








conn = psycopg2.connect(
    host="vic5o0tw1w-repl.twtim97jsb.tsdb.cloud.timescale.com",
    user="tsdbadmin",
    password="yP4wU5bL0tI0kP3k"
)

SHOW COLUMNS in table 

SELECT block_timestamp
, block_id
, tx_id
, asset
, pool_deduct
, asset_e8
, COUNT(1) AS n
SELECT *
FROM bronze_midgard_2_6_9_20220405.midgard_fee_events
LIMIT 10
GROUP BY 1, 2, 3, 4, 5, 6
HAVING COUNT(1) > 1
ORDER BY 7 DESC
LIMIT 10


SELECT *
, COUNT(1) AS n
FROM thorchain.block_rewards
GROUP BY 1, 2, 3, 4, 5, 6
HAVING COUNT(1) > 1
ORDER BY 7 DESC
LIMIT 10

SELECT *
FROM thorchain.block_log
WHERE height = 4310545

timestamp = 1645094662447193601


SELECT *
FROM thorchain.transfers
ORDER BY block_timestamp 
LIMIT 100

WITH mx AS (
	SELECT pool_name, MAX(block_timestamp) AS mx
	FROM thorchain.pool_block_balances
	WHERE block_timestamp >= CURRENT_DATE - 2
	GROUP BY 1
)
SELECT SUM(b.asset_amount_usd + b.synth_amount_usd) AS assets_locked, SUM(b.rune_amount) AS rune_amount
FROM thorchain.pool_block_balances b
JOIN mx ON mx.mx = b.block_timestamp
	AND mx.pool_name = b.pool_name





SELECT *
FROM crosschain.address_labels
WHERE blockchain = 'solana'
AND label_subtype = 'nf_token_contract'
LIMIT 10

SELECT COUNT(DISTINCT project_name)
FROM crosschain.address_labels
WHERE blockchain = 'solana'
AND label_subtype = 'nf_token_contract'
LIMIT 10



SELECT 
coalesce(m.project_name, s.mint) as collection,
s.purchaser as wallet,
s.tx_id,
s.block_timestamp
from solana.fact_nft_mints s left join solana.dim_nft_metadata m on s.mint = m.mint
where block_timestamp >=  '2022-01-01'


SELECT program_id, COUNT(1) AS n_mints, SUM(mint_price) AS sol_volume
FROM solana.fact_nft_mints
GROUP BY 1

SELECT date_part('DAYOFWEEK', block_timestamp) AS weekday, COUNT(1) AS n_mints, SUM(mint_price) AS sol_volume
FROM solana.fact_nft_mints
WHERE program_id IN (
	'cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ'
	, 'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ'
)
GROUP BY 1


WITH collections AS (
	SELECT project_name AS collection
	, address AS mint_address
	FROM crosschain.address_labels
	WHERE blockchain = 'solana'
	AND label_subtype = 'nf_token_contract'
	UNION ALL 
	SELECT project_name AS collection
	, mint AS mint_address
	FROM solana.dim_nft_metadata
), c2 AS (
	SELECT DISTINCT collection
	, mint_address
	FROM collections
), base AS (
	SELECT date_part('DAYOFWEEK', block_timestamp) AS weekday
	, block_timestamp
	, DATEADD('HOURS', -4, block_timestamp) AS est_time
	, date_part('DAYOFWEEK', DATEADD('HOURS', -4, block_timestamp)) AS est_weekday
	, COALESCE(c2.collection, 'Unknown') AS collection
	, mint_price
	FROM solana.fact_nft_mints m 
	LEFT JOIN collections c2 ON c2.mint_address = m.mint
	WHERE program_id IN (
		'cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ'
		, 'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ'
	)
	AND weekday < 1 OR weekday > 6
)
SELECT collection
, CASE WHEN est_weekday = 1 THEN 'Monday'
	WHEN est_weekday = 2 THEN 'Tuesday'
	WHEN est_weekday = 3 THEN 'Wednesday'
	WHEN est_weekday = 4 THEN 'Thursday'
	WHEN est_weekday = 5 THEN 'Friday'
	WHEN est_weekday = 6 THEN 'Saturday'
	ELSE 'Sunday' 
END AS clean_weekday
, COUNT(1) AS n_mints
, SUM(mint_price) AS sol_volume
FROM base
GROUP BY 1, 2


WITH collections AS (
	SELECT project_name AS collection
	, address AS mint_address
	FROM crosschain.address_labels
	WHERE blockchain = 'solana'
	AND label_subtype = 'nf_token_contract'
	UNION ALL 
	SELECT project_name AS collection
	, mint AS mint_address
	FROM solana.dim_nft_metadata
), c2 AS (
	SELECT DISTINCT LOWER(collection) AS collection
	, mint_address
	FROM collections
)
SELECT date_part('DAYOFWEEK', block_timestamp) AS weekday
, block_timestamp
, DATEADD('HOURS', -4, block_timestamp) AS est_time
, date_part('DAYOFWEEK', DATEADD('HOURS', -4, block_timestamp)) AS est_weekday
, COALESCE(c2.collection, 'Unknown') AS collection
, mint_price
FROM solana.fact_nft_mints m 
LEFT JOIN collections c2 ON c2.mint_address = m.mint
WHERE block_timestamp >= '2022-01-01'
	AND program_id IN (
	'cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ'
	, 'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ'
)
AND mint_price < 100
ORDER BY mint_price DESC
LIMIT 1000


SELECT *
FROM solana.fact_nft_mints m 
WHERE mint = 





WITH collections AS (
	SELECT project_name AS collection
	, address AS mint_address
	FROM crosschain.address_labels
	WHERE blockchain = 'solana'
	AND label_subtype = 'nf_token_contract'
	UNION ALL 
	SELECT project_name AS collection
	, mint AS mint_address
	FROM solana.dim_nft_metadata
), c2 AS (
	SELECT DISTINCT collection
	, mint_address
	FROM collections
)
SELECT * FROM c2 WHERE collection = 'Okay Bears'

SELECT *
FROM solana.fact_nft_mints
WHERE mint IN ('13VcCoRBqyXsWNBJZQituivTtWd8USbzfRsUBBkSbw6Y','2CMQnGJMq1U611Dbim5ALqPQCDQQ1jseYLQfAhkxZ9cY')



data-science
DKiZEzE-CZ+o59}L
boatpartydotbiz

[Python]
Enabled = true
Executable = /opt/python/3.10.4/bin/python

use_python('/opt/python/3.10.4/bin/python')

Sys.setenv(RETICULATE_PYTHON = '/opt/python/3.10.4/bin/python')
RETICULATE_PYTHON='/opt/python/3.10.4/bin/python'
library(reticulate)


grep -r 'update_nft_deal_score_data.RMD' ./
grep -R "touch" .
find ./ -name 'update_nft_deal_score_data.RMD' -print


Solana Monkey Business: https://api.flipsidecrypto.com/api/v2/queries/645b0dbc-f932-4389-a9b7-fcb1ae0c2c70/data/latest
Okay Bears: https://api.flipsidecrypto.com/api/v2/queries/265c2f2e-58cd-456b-951c-2bc49866c6ea/data/latest
Catalina Whale Mixer: https://api.flipsidecrypto.com/api/v2/queries/a66a796b-4e47-4acf-8195-69ea782c8f67/data/latest


LunaBulls: https://api.flipsidecrypto.com/api/v2/queries/4ddc8d39-ef6b-4f19-a1a2-893e3f597a5d/data/latest
Galactic Punks: https://api.flipsidecrypto.com/api/v2/queries/adeef975-6eeb-41f9-9952-cd82c5a4c668/data/latest
Galactic Angels: https://api.flipsidecrypto.com/api/v2/queries/0c450359-c3e9-4dd6-a349-20841a0ce1e0/data/latest
Levana Dragon Eggs: https://api.flipsidecrypto.com/api/v2/queries/f05e24fd-ad62-480b-905a-15ea334cbf48/data/latest

MAYC: https://api.flipsidecrypto.com/api/v2/queries/881d8b52-f6f7-4333-b92a-35a075e5ec65/data/latest





WITH base AS (
	SELECT * 
	FROM flipside_prod_db.bronze.prod_data_science_uploads_1748940988
	WHERE record_content[0]:collection IS NOT NULL
	AND record_metadata:key like '%nft-deal-score-rankings-%'
	AND record_content[0]:collection = 'Solana Monkey Business'
), base2 AS (
	SELECT t.value:collection::string AS collection
	, t.value:cur_floor AS old_floor
	, t.value:cur_sd AS old_sd
	, t.value:deal_score_rank AS deal_score_rank
	, t.value:fair_market_price AS old_fair_market_price
	, t.value:lin_coef AS lin_coef
	, t.value:log_coef AS log_coef
	, t.value:rarity_rank AS rarity_rank
	, t.value:token_id AS token_id
	, ROW_NUMBER() OVER (PARTITION BY collection, token_id ORDER BY record_metadata:CreateTime DESC) AS rn
	FROM base
	, LATERAL FLATTEN(
	input => record_content
	) t
), base3 AS (
	SELECT *
	FROM flipside_prod_db.bronze.prod_data_science_uploads_1748940988
	WHERE record_content[0]:collection IS NOT NULL
	AND record_metadata:key like '%nft-deal-score-floors-%'
), base4 AS (
	SELECT t.value:collection::string AS collection
	, t.value:cur_floor AS new_floor
	, b.*
	, ROW_NUMBER() OVER (PARTITION BY collection ORDER BY record_metadata:CreateTime DESC) AS rn
	FROM base3 b
	, LATERAL FLATTEN(
	input => record_content
	) t
), base5 AS (
	SELECT b2.*
	, b4.new_floor
	FROM base2 b2
	JOIN base4 b4 ON b2.collection = b4.collection AND b4.rn = 1
	WHERE b2.rn = 1
), base6 AS (
	SELECT *
	, old_sd * new_floor / old_floor AS cur_sd
	, old_fair_market_price + ((new_floor - old_floor) * lin_coef) + (( new_floor - old_floor) * log_coef * old_fair_market_price / old_floor) AS new_fair_market_price
	FROM base5
)
SELECT collection
, token_id
, deal_score_rank
, rarity_rank
, new_floor AS floor_price
, ROUND(CASE WHEN new_fair_market_price < floor_price THEN floor_price ELSE new_fair_market_price END, 2) AS fair_market_price
, ROUND(CASE WHEN new_fair_market_price - cur_sd < floor_price * 0.975 THEN floor_price * 0.975 ELSE new_fair_market_price - cur_sd END, 2) AS price_low
, ROUND(CASE WHEN new_fair_market_price + cur_sd < floor_price * 1.025 THEN floor_price * 1.025 ELSE new_fair_market_price + cur_sd END, 2) AS price_high
FROM base6


WITH base3 AS (
	SELECT *
	FROM flipside_prod_db.bronze.prod_data_science_uploads_1748940988
	WHERE record_content[0]:collection IS NOT NULL
	AND record_metadata:key like '%nft-deal-score-floors-%'
), base4 AS (
	SELECT t.value:collection::string AS collection
	, t.value:cur_floor AS new_floor
	, b.*
	, ROW_NUMBER() OVER (PARTITION BY collection ORDER BY record_metadata:CreateTime DESC) AS rn
	FROM base3 b
	, LATERAL FLATTEN(
	input => record_content
	) t
)
SELECT * FROM base4



block_timestamp = 1648541784259230029

SELECT *
FROM FLIPSIDE_PROD_DB.bronze_midgard_2_6_9_20220405.MIDGARD_UNSTAKE_EVENTS
WHERE tx = '3E439D4DCA401602116BE07E2FB8751D6F491EE908E04A779D48780DF3972201'


SELECT MAX(timestamp)
FROM FLIPSIDE_PROD_DB.bronze_midgard_2_6_9_20220405.midgard_block_log

SELECT ABS(timestamp - 1648541784259230029) AS abs_dff
, (timestamp - 1648541784259230029) AS dff
, *
FROM FLIPSIDE_PROD_DB.bronze_midgard_2_6_9_20220405.midgard_block_log
ORDER BY abs_dff
LIMIT 100
WHERE timestamp = 1648541784259230029

SELECT l1.height AS missing_block_id
FROM FLIPSIDE_PROD_DB.bronze_midgard_2_6_9_20220405.midgard_block_log l1
LEFT JOIN FLIPSIDE_PROD_DB.bronze_midgard_2_6_9_20220405.midgard_block_log l2 ON l2.height = l1.height + 1
LEFT JOIN FLIPSIDE_PROD_DB.bronze_midgard_2_6_9_20220405.midgard_block_log l3 ON l3.height = l1.height - 1
WHERE (l2.height IS NULL OR l3.height IS NULL)
ORDER BY l1.height

SELECT *
FROM silver.prices_v2
WHERE recorded_at >= CURRENT_DATE - 2
AND symbol ilike '%gold%'
AND (name ilike '%defi%' OR name ilike '%dfk%')
LIMIT 100

SELECT date_trunc('week', block_timestamp) AS hour
, COUNT(1) AS n
, AVG(CASE WHEN SUCCEEDED = TRUE THEN 1.0 ELSE 0 END) AS pct_succeed
FROM solana.fact_transactions
WHERE block_timestamp >= CURRENT_DATE - 90
GROUP BY 1


Hat_Big Crown
Mouth_Wagmi Beach
Body_Oil Slick



1648541784259230029
1648541754719682517
29539547512 / 29539547512
29539547512 / 1000000000
35261826303 / 1000000000

not in 

SELECT *
FROM FLIPSIDE_PROD_DB.thorchain_midgard_public.unstake_events
WHERE tx = '3E439D4DCA401602116BE07E2FB8751D6F491EE908E04A779D48780DF3972201'

SELECT *
FROM FLIPSIDE_PROD_DB.THORCHAIN_MIDGARD_PUBLIC.BLOCK_LOG
WHERE timestamp >= 1648541784259230029
ORDER BY timestamp DESC
LIMIT 100

SELECT MAX(timestamp)
FROM FLIPSIDE_PROD_DB.THORCHAIN_MIDGARD_PUBLIC.BLOCK_LOG


SELECT *
FROM FLIPSIDE_PROD_DB.thorchain_midgard_public.block_log
WHERE timestamp = 1648541784259230029
ORDER BY 


SELECT *
FROM thorchain.unstake_events
WHERE emit_rune_e8 + emit_asset_e8 = 0

SELECT *
FROM thorchain.unstake_events
WHERE tx_id = '5815E4A96E9CBCD3CD24CADA4EB7765E0BFDC81D59ED945309BCD755FC969797'

SELECT *
FROM thorchain.liquidity_actions
WHERE RUNE_AMOUNT = 0 and asset_amount = 0


SELECT lp_action, COUNT(1) AS n
FROM thorchain.liquidity_actions
WHERE RUNE_AMOUNT = 0 and asset_amount = 0
GROUP BY 1

SELECT date_trunc('month', block_timestamp) AS month, COUNT(1) AS n
FROM thorchain.liquidity_actions
WHERE RUNE_AMOUNT = 0 and asset_amount = 0
GROUP BY 1

903 / 907 are remove_liquidity
Months are spread out



Research Question: How much value did each pool win/loose because of synths?
@Orion (9R)#3332 just to clarify - what do you mean by a pool winning / losing value in this case?


server {
    listen 80;
    server_name ec2-54-91-227-50.compute-1.amazonaws.com;
    location / {
        proxy_pass http://54.91.227.50;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
     }
}


WITH
  burns as (
  SELECT 
SPLIT_PART(x.from_asset, ',', 0) AS chain
count(distinct x.tx_id) as burns,
  sum(x.from_amount_usd) as volume_burnt
FROM thorchain.swaps x, thorchain.swap_events y
WHERE x.tx_id=y.tx_id and memo like '%/%' and x.from_asset like '%/%' and x.to_asset not like '%/%'
GROUP BY 1
ORDER BY 1 DESC
  ),
  mints as (
  SELECT 
SPLIT_PART(x.to_asset, ',', 0) AS chain
count(distinct x.tx_id) as mints,
  sum(x.to_amount_usd) as volume_mint
FROM thorchain.swaps x, thorchain.swap_events y
WHERE x.tx_id=y.tx_id and memo like '%/%' and x.to_asset like '%/%' and x.from_asset not like '%/%'
GROUP BY 1
ORDER BY 1 DESC
),
  rune_price as(
  SELECT
  trunc(hour,'day') as date,
  avg(price) as rune_price
  from ethereum.token_prices_hourly
  where symbol ='RUNE'
  group by 1
  )
SELECT
x.chain,
mints,
burns,
volume_mint,
volume_burnt,
mints - burns AS net_mint_usd
from mints x, burns y where x.chain=y.chain
order by 1 asc


SELECT *
FROM THORCHAIN.BOND_ACTIONS
WHERE to_address like 'node%'

SELECT SUM(CASE WHEN from_address = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' THEN -rune_amount ELSE rune_amount END) AS amount
, MAX(block_timestamp) as mx
, MAX(block_timestamp) as mx2
FROM thorchain.transfers 
WHERE (from_address = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' OR to_address = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt')
AND from_address <> to_address

SELECT SUM(CASE WHEN from_address = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' THEN -rune_amount ELSE rune_amount END) AS amount
, MAX(block_timestamp) as mx
, MAX(block_timestamp) as mx2
FROM thorchain.transfers 
WHERE (from_address = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' OR to_address = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt')
AND from_address <> to_address

SELECT SUM(CASE WHEN from_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' THEN -amount_e8 ELSE amount_e8 END) * POWER(10, -8) AS amount
, MAX(block_timestamp) as mx
FROM midgard.transfers 
WHERE (from_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' OR to_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt')
AND from_addr <> to_addr

SELECT SUM(CASE WHEN from_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' THEN -amount_e8 ELSE amount_e8 END) * POWER(10, -8) AS amount
, MAX(block_timestamp) as mx
FROM bronze_midgard_2_6_9_20220405 
WHERE (from_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' OR to_addr = 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt')
AND from_addr <> to_addr




111,373,009
111,369,480

112109286 - 112109338

SELECT *
FROM bronze_midgard_2_6_9_20220405.bond_actions
WHERE to_addr like 'node%'





WITH base AS (
	SELECT native_to_address, COUNT(1) AS n
	FROM thorchain.swaps
	WHERE block_timestamp >= '2022-04-01'
	GROUP BY 1
)
select b.n, s.* from thorchain.swaps s 
JOIN base b ON b.native_to_address = s.native_to_address
where block_timestamp >= '2022-04-10'
ORDER BY block_timestamp, n, tx_id
LIMIT 100

SELECT *
FROM thorchain.pool_balance_change_events
WHERE block_id = 5486400



SELECT MIN(block_timestamp) AS mn
, MAX(block_timestamp) AS mx
, COUNT(1) AS n
FROM flipside_dev_db.thorchain.liquidity_actions


SELECT MIN(block_timestamp) AS mn
, MAX(block_timestamp) AS mx
, COUNT(1) AS n
FROM flipside_dev_db.thorchain.liquidity_actions


SELECT MIN(block_timestamp) AS mn
, MAX(block_timestamp) AS mx
, COUNT(1) AS n
FROM flipside_dev_db.thorchain.bond_actions

SELECT 



https://app.flipsidecrypto.com/dashboard/small-lp-actions-LD1XQ9


https://app.flipsidecrypto.com/dashboard/pool-ranks-UbtLg9
https://app.flipsidecrypto.com/dashboard/price-shift-_-JpTq
https://app.flipsidecrypto.com/dashboard/tho-rchain-average-age-of-synth-holders-Z2AXIx
https://app.flipsidecrypto.com/dashboard/thor-64-standardized-tvl-over-time-all-pools-Zf6w-L
https://app.flipsidecrypto.com/dashboard/tho-rchain-pool-ranks-RNNzza
https://app.flipsidecrypto.com/dashboard/thorchain-synth-mints-burns-aH0lCY

https://discord.com/channels/889577356681945098/889577399308656662/951960381411192842

solana-keygen recover 'prompt:?key=3/0' --outfile ~/.config/solana/tmp.json


SELECT *
, instructions[0]:parsed:info:lamports / POWER(10, 9) AS sol_amount
FROM solana.fact_transactions
WHERE block_timestamp >= '2022-01-02'
AND tx_id = '5H6UQqbxa2wtryax6SAZgjXB9B6Za4ip6GsheqopAsbLCrLMPvYf35H551SaAKNy6bi6BceRGtkwwP9LRoN7RiVo'


SELECT *
FROM solana.fact_transfers
WHERE block_timestamp >= '2022-01-02'
AND tx_id = '393xRouisz4DuMxzARAqPy7FVYQZtkfpmAMuDXXj39uvASFYtMijHM9hyVzXSocsB4fk2woLNfnWTM4qXJxJsWBw'

SELECT lp.tx_id
, lp.signers[0] as signer
, t.instructions[0]:parsed:info:lamports / POWER(10, 9) AS sol_amount
FROM solana.fact_staking_lp_actions lp
JOIN solana.fact_transactions t ON t.tx_id = lp.tx_id
WHERE lp.block_timestamp >= '2022-01-01'
AND lp.event_type = 'delegate' 
AND tx_id = '393xRouisz4DuMxzARAqPy7FVYQZtkfpmAMuDXXj39uvASFYtMijHM9hyVzXSocsB4fk2woLNfnWTM4qXJxJsWBw'
LIMIT 10

sudo apt-get build-dep python E: You must put some 'deb-src' URIs in your sources.list


SELECT project_name
, COUNT(1) AS n
, SUM( CASE WHEN address IS NULL THEN 1 ELSE 0 END) AS n_nulls
FROM crosschain.address_labels
WHERE blockchain = 'solana'
AND label_subtype = 'nf_token_contract'
GROUP BY 1


WITH base AS (
	SELECT * 
	FROM flipside_prod_db.bronze.prod_data_science_uploads_1748940988
	WHERE record_content[0]:collection IS NOT NULL
)
SELECT t.value FROM base
, LATERAL FLATTEN(
input => record_content
) t



SELECT *
FROM solana.fact_transactions

WHERE block_timestamp >= CURRENT_DATE - 2
AND instructions[0]:program_id::string = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
LIMIT 100


SELECT instructions[0]:data::string AS data
, COUNT(1) AS n
FROM solana.fact_transactions
WHERE block_timestamp >= CURRENT_DATE - 2
AND instructions[0]:programId::string = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
AND tx_id = '4wfERHL5E5mDhDA4AB6bbhxtwjWh5kvPWyVVD8rWuZ6rWxzBctxhXRPHNxxU6Jpfx34dG8R3nWqvLZRGzSmHsErJ'
GROUP BY 1

SELECT instructions[0]:data::string AS data
, *
FROM solana.fact_transactions
WHERE block_timestamp >= CURRENT_DATE - 2
AND instructions[0]:programId::string = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
AND tx_id = '4wfERHL5E5mDhDA4AB6bbhxtwjWh5kvPWyVVD8rWuZ6rWxzBctxhXRPHNxxU6Jpfx34dG8R3nWqvLZRGzSmHsErJ'
LIMIT 100

WITH base AS (
	SELECT instructions[0]:data::string AS data
	, instructions[0]:programId::string AS program_id
	, *
	FROM solana.fact_transactions
	WHERE block_timestamp >= CURRENT_DATE - 2
	//AND instructions[0]:programId::string = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
	AND tx_id = '25XwyS9jjZqs9xxqtVkRPRDRkydsrcCG9QMeqJMeY32ZQzMrSCpc9mdbUJFxMQDMfCNfB1E122krRKLTHW2JPoEx'
	LIMIT 10
)
SELECT t.value FROM base
, LATERAL FLATTEN(
input => instructions
) t





-- MEv1 De-Listing
WITH mints AS (
	SELECT DISTINCT project_name
	, mint
	, token_id
	FROM solana.dim_nft_metadata
)
SELECT pre_token_balances[0]:mint::string AS mint
, t.*
, m.project_name
, m.token_id
FROM solana.fact_transactions t
JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
WHERE block_timestamp >= CURRENT_DATE - 30
AND tx_id = '3CxhnTCXYX1zH6HbNESFsZLwdHfTe7RUYF8tAgB168hciVjUGggp2PwVEsnDvpd2kNqMha7kH2be7NtSTppAnXzn'
AND instructions[0]:data = 'TE6axTojnpk'
AND instructions[0]:programId = 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8'
AND succeeded = TRUE
LIMIT 100

ENwHiaH9NA9veUqRzGozjWnTuR9xcNvHcPZVFMi3Ca9L


WITH mints AS (
	SELECT DISTINCT project_name
	, mint
	, token_id
	FROM solana.dim_nft_metadata
)
SELECT instructions[0]:data AS data
, COUNT(1) AS n
, COUNT(1) AS nn
FROM solana.fact_transactions t
JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
WHERE block_timestamp >= '2022-04-17'
AND instructions[0]:programId = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
AND succeeded = TRUE
GROUP BY 1
ORDER BY 2 DESC


-- MEv2 De-Listing
WITH mints AS (
	SELECT DISTINCT project_name
	, mint
	, token_id
	FROM solana.dim_nft_metadata
), rem AS (
	SELECT pre_token_balances[0]:mint::string AS mint
	, m.project_name
	, m.token_id
	, t.tx_id AS remove_tx
	, block_timestamp AS remove_time
	, ROW_NUMBER() OVER (PARTITION BY mint ORDER BY block_timestamp DESC) AS rn
	FROM solana.fact_transactions t
	JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
	WHERE block_timestamp >= CURRENT_DATE - 3
	AND LEFT(instructions[0]:data::string, 4) IN ('ENwH','3GyW')
	AND instructions[0]:programId = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
), add AS (
	SELECT pre_token_balances[0]:mint::string AS mint
	, m.project_name
	, m.token_id
	, t.tx_id AS listing_tx
	, block_timestamp AS listing_time
	, ROW_NUMBER() OVER (PARTITION BY mint ORDER BY block_timestamp DESC) AS rn
	FROM solana.fact_transactions t
	JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
	WHERE block_timestamp >= CURRENT_DATE - 3
	AND LEFT(instructions[0]:data::string, 4) IN ('2B3v')
	AND instructions[0]:programId = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
	AND succeeded = TRUE
)
SELECT a.*
, r.remove_tx
, r.remove_time
, CASE WHEN r.remove_time IS NULL OR a.listing_time > r.remove_time THEN 1 ELSE 0 END AS is_listed
FROM add a
LEFT JOIN rem r ON r.mint = a.mint AND r.rn = 1
WHERE a.rn = 1

thor12209cxpf4mpm8qxdyzmm4k8mfuyjt4fysnwyjj

WITH base AS (
	SELECT from_address AS address 
	, block_timestamp::date AS date
	, -from_amount AS amount
	, 'From Swap' AS tx_type
	FROM thorchain.swaps
	WHERE from_address = 'thor12209cxpf4mpm8qxdyzmm4k8mfuyjt4fysnwyjj'
	AND from_asset = 'THOR.RUNE'

	UNION ALL

	SELECT native_to_address AS address 
	, block_timestamp::date AS date
	, to_amount AS amount
	, 'To Swap' AS tx_type
	FROM thorchain.swaps
	WHERE native_to_address = 'thor12209cxpf4mpm8qxdyzmm4k8mfuyjt4fysnwyjj'
	AND to_asset = 'THOR.RUNE'

	UNION ALL

	SELECT from_address AS address 
	, block_timestamp::date AS date
	, -rune_amount AS amount
	, 'From Transfer' AS tx_type
	FROM thorchain.transfers
	WHERE from_address = 'thor12209cxpf4mpm8qxdyzmm4k8mfuyjt4fysnwyjj'

	UNION ALL

	SELECT to_address AS address 
	, block_timestamp::date AS date
	, rune_amount AS amount
	, 'To Transfer' AS tx_type
	FROM thorchain.transfers
	WHERE to_address = 'thor12209cxpf4mpm8qxdyzmm4k8mfuyjt4fysnwyjj'
)
SELECT *
FROM base
ORDER BY date DESC

SELECT *
FROM THORCHAIN.DAILY_POOL_STATS
WHERE pool_name ilike '%luna%'
ORDER BY day DESC


WITH base AS (
	SELECT from_address AS address 
	, block_timestamp::date AS date
	, rune_amount AS lp_amount
	, 'Add LP' AS tx_type
	FROM thorchain.liquidity_actions
	WHERE from_address = 'thor12209cxpf4mpm8qxdyzmm4k8mfuyjt4fysnwyjj'
	AND lp_action = 'add_liquidity'

	UNION ALL

	SELECT from_address AS address 
	, block_timestamp::date AS date
	, -rune_amount AS lp_amount
	, 'Rem LP' AS tx_type
	FROM thorchain.liquidity_actions
	WHERE from_address = 'thor12209cxpf4mpm8qxdyzmm4k8mfuyjt4fysnwyjj'
	AND lp_action = 'remove_liquidity'

	UNION ALL

	SELECT native_to_address AS address 
	, block_timestamp::date AS date
	, to_amount AS amount
	, 'To Swap' AS tx_type
	FROM thorchain.swaps
	WHERE native_to_address = 'thor12209cxpf4mpm8qxdyzmm4k8mfuyjt4fysnwyjj'
	AND to_asset = 'THOR.RUNE'

	UNION ALL

	SELECT from_address AS address 
	, block_timestamp::date AS date
	, -rune_amount AS amount
	, 'From Transfer' AS tx_type
	FROM thorchain.transfers
	WHERE from_address = 'thor12209cxpf4mpm8qxdyzmm4k8mfuyjt4fysnwyjj'

	UNION ALL

	SELECT to_address AS address 
	, block_timestamp::date AS date
	, rune_amount AS amount
	, 'To Transfer' AS tx_type
	FROM thorchain.transfers
	WHERE to_address = 'thor12209cxpf4mpm8qxdyzmm4k8mfuyjt4fysnwyjj'
)
SELECT *
FROM base
ORDER BY date DESC

-- 2B3v: listing
-- ENwH: de-listing
-- 3GyW: sale

WITH mints AS (
	SELECT DISTINCT project_name
	, mint
	, token_id
	FROM solana.dim_nft_metadata
)
SELECT pre_token_balances[0]:mint::string AS mint
, t.*
, m.project_name
, m.token_id
FROM solana.fact_transactions t
JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
WHERE block_timestamp >= CURRENT_DATE - 2
AND tx_id = '3CxhnTCXYX1zH6HbNESFsZLwdHfTe7RUYF8tAgB168hciVjUGggp2PwVEsnDvpd2kNqMha7kH2be7NtSTppAnXzn'
AND LEFT(instructions[0]:data::string, 4) IN ('2B3v')
AND instructions[0]:programId = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
AND succeeded = TRUE
LIMIT 100






SELECT MIN(block_timestamp) AS mn
, MAX(block_timestamp) AS mx
, COUNT(1) AS n
FROM flipside_prod_db.thorchain.liquidity_actions


SELECT MIN(block_timestamp) AS mn
, MAX(block_timestamp) AS mx
, COUNT(1) AS n
FROM flipside_prod_db.thorchain.prices

liquidity_actions
bond_actions


SELECT MIN(block_timestamp) AS mn
, MAX(block_timestamp) AS mx
, COUNT(1) AS n
FROM flipside_prod_db.thorchain.bond_actions


SELECT MIN(block_timestamp) AS mn
, MAX(block_timestamp) AS mx
, COUNT(1) AS n
FROM flipside_prod_db.thorchain.pool_block_balances
WHERE rune_amount > 0 and COALESCE(rune_amount_usd, 0) = 0

SELECT MIN(block_timestamp) AS mn
, MAX(block_timestamp) AS mx
, COUNT(1) AS n
FROM flipside_prod_db.thorchain.pool_block_balances
WHERE asset_amount > 0 and COALESCE(asset_amount_usd, 0) = 0

SELECT *
FROM flipside_prod_db.thorchain.pool_block_balances
WHERE (asset_amount > 0 and COALESCE(asset_amount_usd, 0) = 0)
OR (rune_amount > 0 and COALESCE(rune_amount_usd, 0) = 0)
ORDER BY block_timestamp DESC
LIMIT 10000

SELECT block_timestamp::date AS date
, COUNT(1) AS n
FROM flipside_prod_db.thorchain.pool_block_balances
WHERE (asset_amount > 0 and COALESCE(asset_amount_usd, 0) = 0)
OR (rune_amount > 0 and COALESCE(rune_amount_usd, 0) = 0)
ORDER BY block_timestamp DESC
GROUP BY 1
ORDER BY 1



WITH active_vault_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'active_vault_events' AS "table"
   FROM thorchain.active_vault_events),
     add_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'add_events' AS "table"
   FROM thorchain.add_events),
     asgard_fund_yggdrasil_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'asgard_fund_yggdrasil_events' AS "table"
   FROM thorchain.asgard_fund_yggdrasil_events),
     block_pool_depths_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'block_pool_depths' AS "table"
   FROM thorchain.block_pool_depths),
     block_rewards_cte AS
  (SELECT max(day)::string AS recency, min(day)::string AS start_time,
          'block_rewards' AS "table"
   FROM thorchain.block_rewards),
     bond_actions_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'bond_actions' AS "table"
   FROM thorchain.bond_actions),
     bond_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'bond_events' AS "table"
   FROM thorchain.bond_events),
     constants_cte AS
  (SELECT 'NA for this table' AS recency, 'NA for this table' AS start_time,
          'constants' AS "table"
   FROM thorchain.constants),
     daily_earnings_cte AS
  (SELECT max(day)::string AS recency, min(day)::string AS start_time,
          'daily_earnings' AS "table"
   FROM thorchain.daily_earnings),
     daily_pool_stats_cte AS
  (SELECT max(day)::string AS recency, min(day)::string AS start_time,
          'daily_pool_stats' AS "table"
   FROM thorchain.daily_pool_stats),
     daily_tvl_cte AS
  (SELECT max(day)::string AS recency, min(day)::string AS start_time,
          'daily_tvl' AS "table"
   FROM thorchain.daily_tvl),
     errata_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'errata_events' AS "table"
   FROM thorchain.errata_events),
     fee_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'fee_events' AS "table"
   FROM thorchain.fee_events),
     gas_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'gas_events' AS "table"
   FROM thorchain.gas_events),
     inactive_vault_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'inactive_vault_events' AS "table"
   FROM thorchain.inactive_vault_events),
     liquidity_actions_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'liquidity_actions' AS "table"
   FROM thorchain.liquidity_actions),
     message_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'message_events' AS "table"
   FROM thorchain.message_events),
     new_node_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'new_node_events' AS "table"
   FROM thorchain.new_node_events),
     outbound_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'outbound_events' AS "table"
   FROM thorchain.outbound_events),
     pending_liquidity_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'pending_liquidity_events' AS "table"
   FROM thorchain.pending_liquidity_events),
     pool_balance_change_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'pool_balance_change_events' AS "table"
   FROM thorchain.pool_balance_change_events),
     pool_block_balances_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'pool_block_balances' AS "table"
   FROM thorchain.pool_block_balances),
     pool_block_fees_cte AS
  (SELECT max(day)::string AS recency, min(day)::string AS start_time,
          'pool_block_fees' AS "table"
   FROM thorchain.pool_block_fees),
     pool_block_statistics_cte AS
  (SELECT max(day)::string AS recency, min(day)::string AS start_time,
          'pool_block_statistics' AS "table"
   FROM thorchain.pool_block_statistics),
     pool_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'pool_events' AS "table"
   FROM thorchain.pool_events),
     prices_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'prices' AS "table"
   FROM thorchain.prices),
     refund_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'refund_events' AS "table"
   FROM thorchain.refund_events),
     reserve_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'reserve_events' AS "table"
   FROM thorchain.reserve_events),
     rewards_event_entries_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'rewards_event_entries' AS "table"
   FROM thorchain.rewards_event_entries),
     rewards_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'rewards_events' AS "table"
   FROM thorchain.rewards_events),
     set_ip_address_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'set_ip_address_events' AS "table"
   FROM thorchain.set_ip_address_events),
     set_mimir_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'set_mimir_events' AS "table"
   FROM thorchain.set_mimir_events),
     set_node_keys_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'set_node_keys_events' AS "table"
   FROM thorchain.set_node_keys_events),
     set_version_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'set_version_events' AS "table"
   FROM thorchain.set_version_events),
     slash_amounts_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'slash_amounts' AS "table"
   FROM thorchain.slash_amounts),
     stake_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'stake_events' AS "table"
   FROM thorchain.stake_events),
     swap_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'swap_events' AS "table"
   FROM thorchain.swap_events),
     swaps_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'swaps' AS "table"
   FROM thorchain.swaps),
     switch_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'switch_events' AS "table"
   FROM thorchain.switch_events),
     thorname_change_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'thorname_change_events' AS "table"
   FROM thorchain.thorname_change_events),
     total_block_rewards_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'total_block_rewards' AS "table"
   FROM thorchain.total_block_rewards),
     total_value_locked_cte AS
  (SELECT max(day)::string AS recency, min(day)::string AS start_time,
          'total_value_locked' AS "table"
   FROM thorchain.total_value_locked),
     transfer_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'transfer_events' AS "table"
   FROM thorchain.transfer_events),
     transfers_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'transfers' AS "table"
   FROM thorchain.transfers),
     unstake_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'unstake_events' AS "table"
   FROM thorchain.unstake_events),
     update_node_account_status_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'update_node_account_status_events' AS "table"
   FROM thorchain.update_node_account_status_events),
     upgrades_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'upgrades' AS "table"
   FROM thorchain.upgrades),
     validator_request_leave_events_cte AS
  (SELECT max(block_timestamp)::string AS recency, min(block_timestamp)::string AS start_time,
          'validator_request_leave_events' AS "table"
   FROM thorchain.validator_request_leave_events)
  
SELECT *
FROM active_vault_events_cte
UNION
SELECT *
FROM add_events_cte
UNION
SELECT *
FROM asgard_fund_yggdrasil_events_cte
UNION
SELECT *
FROM block_pool_depths_cte
UNION
SELECT *
FROM block_rewards_cte
UNION
SELECT *
FROM bond_actions_cte
UNION
SELECT *
FROM bond_events_cte
UNION
SELECT *
FROM constants_cte
UNION
SELECT *
FROM daily_earnings_cte
UNION
SELECT *
FROM daily_pool_stats_cte
UNION
SELECT *
FROM daily_tvl_cte
UNION
SELECT *
FROM errata_events_cte
UNION
SELECT *
FROM fee_events_cte
UNION
SELECT *
FROM gas_events_cte
UNION
SELECT *
FROM inactive_vault_events_cte
UNION
SELECT *
FROM liquidity_actions_cte
UNION
SELECT *
FROM message_events_cte
UNION
SELECT *
FROM new_node_events_cte
UNION
SELECT *
FROM outbound_events_cte
UNION
SELECT *
FROM pending_liquidity_events_cte
UNION
SELECT *
FROM pool_balance_change_events_cte
UNION
SELECT *
FROM pool_block_balances_cte
UNION
SELECT *
FROM pool_block_fees_cte
UNION
SELECT *
FROM pool_block_statistics_cte
UNION
SELECT *
FROM pool_events_cte
UNION
SELECT *
FROM prices_cte
UNION
SELECT *
FROM refund_events_cte
UNION
SELECT *
FROM reserve_events_cte
UNION
SELECT *
FROM rewards_event_entries_cte
UNION
SELECT *
FROM rewards_events_cte
UNION
SELECT *
FROM set_ip_address_events_cte
UNION
SELECT *
FROM set_mimir_events_cte
UNION
SELECT *
FROM set_node_keys_events_cte
UNION
SELECT *
FROM set_version_events_cte
UNION
SELECT *
FROM slash_amounts_cte
UNION
SELECT *
FROM stake_events_cte
UNION
SELECT *
FROM swap_events_cte
UNION
SELECT *
FROM swaps_cte
UNION
SELECT *
FROM switch_events_cte
UNION
SELECT *
FROM thorname_change_events_cte
UNION
SELECT *
FROM total_block_rewards_cte
UNION
SELECT *
FROM total_value_locked_cte
UNION
SELECT *
FROM transfer_events_cte
UNION
SELECT *
FROM transfers_cte
UNION
SELECT *
FROM unstake_events_cte
UNION
SELECT *
FROM update_node_account_status_events_cte
UNION
SELECT *
FROM upgrades_cte
UNION
SELECT *
FROM validator_request_leave_events_cte





SELECT s.block_timestamp::date AS date
, COUNT(1) AS num_sales
, SUM(sales_amount) AS volume
, volume / num_sales AS avg_sale_price
FROM solana.dim_nft_metadata m
JOIN solana.fact_nft_sales s ON s.mint_address = m.mint_address
WHERE m.project_name = 'Cets On Creck'
GROUP BY 1



21:33:53  Failure in test dbt_utils_unique_combination_of_columns_thorchain__bond_events_TX_ID__BLOCK_ID (models/thorchain/thorchain__bond_events.yml)
21:33:53    Got 6806 results, configured to fail if != 0
21:33:53  
21:33:53    compiled SQL at target/compiled/sql_models/models/thorchain/thorchain__bond_events.yml/dbt_utils_unique_combination_o_4c33313a0a68966742499a57f9c413f2.sql
21:33:53  
21:33:53  Failure in test dbt_utils_unique_combination_of_columns_thorchain__daily_pool_stats_DAY__POOL_NAME (models/thorchain/thorchain__daily_pool_stats.yml)
21:33:53    Got 103 results, configured to fail if != 0
21:33:53  
21:33:53    compiled SQL at target/compiled/sql_models/models/thorchain/thorchain__daily_pool_stats.yml/dbt_utils_unique_combination_o_091de1e000a8d4666a014c0688a50b0f.sql
21:33:53  
21:33:53  Failure in test dbt_utils_unique_combination_of_columns_thorchain__pool_balance_change_events_BLOCK_ID__ASSET__REASON (models/thorchain/thorchain__pool_balance_change_events.yml)
21:33:53    Got 870 results, configured to fail if != 0
21:33:53  
21:33:53    compiled SQL at target/compiled/sql_models/models/thorchain/thorchain__pool_balance_change_events.yml/dbt_utils_unique_combination_o_309a465be5fc79461b0d64ac3c9308c9.sql
21:33:53  
21:33:53  Failure in test dbt_utils_unique_combination_of_columns_thorchain__pool_block_statistics_DAY__ASSET (models/thorchain/thorchain__pool_block_statistics.yml)
21:33:53    Got 103 results, configured to fail if != 0
21:33:53  
21:33:53    compiled SQL at target/compiled/sql_models/models/thorchain/thorchain__pool_block_statistics.yml/dbt_utils_unique_combination_o_e0e0f1f5a63053051ee1a1a182029461.sql
21:33:53  
21:33:53  Failure in test dbt_utils_unique_combination_of_columns_thorchain__refund_events_TX_ID__BLOCK_ID__TX_ID__TO_ADDRESS__FROM_ADDRESS__ASSET__ASSET_E8__ASSET_2ND (models/thorchain/thorchain__refund_events.yml)
21:33:53    Got 1 result, configured to fail if != 0
21:33:53  
21:33:53    compiled SQL at target/compiled/sql_models/models/thorchain/thorchain__refund_events.yml/dbt_utils_unique_combination_o_1602ea813e0daecea06daa5bf0ce53c0.sql
21:33:53  

thorchain__bond_events: dupes come from tx = "0000000000000000000000000000000000000000000000000000000000000000". did not implement any fix
thorchain__daily_pool_stats: dupes come from pool_block_statistics dupes, which I implemented a fix for
thorchain__pool_balance_change_events: have incorrect test, which I removed
thorchain__pool_block_statistics: implemented fix
thorchain__refund_events: not sure what the issue is there. need to do more digging


WITH base AS (
	SELECT *
	, synth_amount / asset_amount AS ratio
	, ROW_NUMBER() OVER (PARTITION BY pool_name ORDER BY block_timestamp DESC) AS rn
	FROM THORCHAIN.POOL_BLOCK_BALANCES 
	WHERE block_timestamp >= '2022-04-04'
	ORDER BY block_timestamp DESC, ratio DESC
	LIMIT 100
)
SELECT *
FROM base
WHERE rn = 1

SELECT pool_name
, SUM(COALESCE(rune_amount_usd, 0) + COALESCE(asset_amount_usd, 0)) AS amount_usd
, SUM(COALESCE(il_protection_usd, 0)) AS il_protection_usd
FROM thorchain.liquidity_actions
WHERE lp_action = 'remove_liquidity'
GROUP BY 1

goes as profile.yml in ~/.dbt/
dbt deps to install dependencies
dbt test 
dbt run --full-refresh -s models/thorchain
dbt test -s models/thorchain

SELECT *
SELECT MIN(block_timestamp) AS mn
, MIN(block_timestamp) AS mn2
FROM thorchain.swaps
WHERE 1=1
AND (
	from_asset LIKE '%/%'
	OR to_asset LIKE '%/%'
)
LIMIT 10

SELECT tx_id
, COUNT(1) AS n
FROM FLIPSIDE_DEV_DB.bronze_midgard_2_6_9_20220405.MIDGARD_FEE_EVENTS
GROUP BY 1
HAVING COUNT(1) > 1
ORDER BY 2 DESC

SELECT pool_name
, COUNT(1) AS n
, MAX(block_timestamp) AS m
, MAX(block_timestamp) AS mm
FROM thorchain.pool_block_statistics
GROUP BY 1

SELECT POOL_NAME
, COUNT(1) AS n
, MAX(BLOCK_TIMESTAMP) AS m
, MAX(BLOCK_TIMESTAMP) AS mm
FROM thorchain.block_pool_depths
GROUP BY 1
ORDER BY 2 DESC


SELECT COUNT(1) AS n
, COUNT(1) AS nn
FROM thorchain.pool_block_statistics

SELECT pool_name
, COUNT(1) AS n
, MAX(block_timestamp) AS mx
, MIN(block_timestamp) AS mn
FROM thorchain.prices
GROUP BY 1
ORDER BY 2 DESC

WITH base AS (
	SELECT *
	, ROW_NUMBER() OVER (PARTITION BY pool_name ORDER BY block_timestamp DESC) AS rn
	FROM thorchain.pool_block_balances
	WHERE block_timestamp >= '2022-04-01'
)
SELECT *
FROM base 
WHERE rn = 1

SELECT DISTINCT pool_name
FROM thorchain.pool_block_balances


SELECT DISTINCT pool_name
FROM thorchain.pool_block_depths



SELECT DISTINCT pool_name
FROM FLIPSIDE_DEV_DB.bronze_midgard_2_6_9_20220405.MIDGARD_BLOCK_POOL_DEPTHS


SELECT COUNT(1) AS n FROM flipside_dev_db.bronze_midgard_2_6_9.midgard_block_pool_depths
SELECT COUNT(1) AS n FROM flipside_dev_db.bronze_midgard_2_6_9_20220405.midgard_block_pool_depths

SELECT COUNT(1) AS n
, COUNT(1) AS nn
FROM flipside_dev_db.bronze_midgard_2_6_9.midgard_block_pool_depths

SELECT *
FROM BRONZE_MIDGARD_2_6_9.MIDGARD_SWAP_EVENTS
WHERE memo like '=:TERRA/LUNA:%:%'
AND pool like 'TERRA/LUNA'
ORDER BY block_timestamp DESC
LIMIT 100

WITH base AS (
	SELECT *
	, CASE WHEN memo like '=:TERRA/LUNA:%:%' THEN 1 ELSE 0 END AS is_synth
	FROM BRONZE_MIDGARD_2_6_9.MIDGARD_SWAP_EVENTS
	WHERE block_timestamp >= '2022-04-01'
	AND pool like 'TERRA/LUNA%'
	LIMIT 1000
)
SELECT is_synth, COUNT(1) AS 

SELECT asset_e8 / POWER(10, 8) AS asset_amt
, rune_e8 / POWER(10, 8) AS rune_amt
, synth_e8 / POWER(10, 8) AS synth_amt
, synth_amt / asset_amt AS synth_ratio
, *
FROM bronze_midgard_2_6_9.midgard_block_pool_depths
ORDER BY block_timestamp DESC, rune_e8 DESC
LIMIT 40

-- thorchain__refund_events
not sure what the issue is there. need to do more digging

SELECT *
FROM flipside_dev_db.bronze_midgard_2_6_9.midgard_refund_events
LIMIT 10

SELECT tx
, block_timestamp
, to_addr
, from_addr
, asset
, asset_e8
, COALESCE(asset_2nd, '')
, COUNT(1) AS n
, COUNT(1) AS nn
FROM flipside_dev_db.bronze_midgard_2_6_9.midgard_refund_events
GROUP BY 1, 2, 3, 4, 5, 6, 7
HAVING COUNT(1) > 1

SELECT *
FROM flipside_dev_db.bronze_midgard_2_6_9.midgard_refund_events
WHER tx = 'FBB528600711E455908F90340470CED18D24801F62ED123D7BF9A5F5E68466F1'

WITH base AS (
	SELECT tx
	, block_timestamp
	, COUNT(1) AS n
	, COUNT(1) AS nnn
	FROM flipside_dev_db.bronze_midgard_2_6_9.midgard_bond_events
	GROUP BY 1, 2
	HAVING COUNT(1) > 1
	ORDER BY 3 DESC
)
SELECT tx
, COUNT(1) AS n
, COUNT(1) AS nn
FROM base
GROUP BY 1
HAVING COUNT(1) > 1
ORDER BY 2 DESC


-- has 103 dupes
SELECT day
, asset
, COUNT(1) AS n
, COUNT(1) AS nn
FROM flipside_dev_db.thorchain.pool_block_statistics
GROUP BY 1, 2
HAVING COUNT(1) > 1
ORDER BY 3 DESC


-- has 134 dupes, fixed implemented by getting the latest status in the day
SELECT date(block_timestamp) AS day
, asset AS pool_name
, block_id
, COUNT(1) AS n
, COUNT(1) AS nn
FROM flipside_dev_db.thorchain.pool_events
GROUP BY 1, 2, 3
HAVING COUNT(1) > 1
ORDER BY 4 DESC



-- thorchain__pool_balance_change_events -> incorrect test

SELECT block_timestamp
, reason
, asset
, COUNT(1) AS n
, COUNT(1) AS nn
FROM flipside_dev_db.bronze_midgard_2_6_9.midgard_pool_balance_change_events
GROUP BY 1, 2, 3
HAVING COUNT(1) > 1
ORDER BY 4 DESC

SELECT *
FROM flipside_dev_db.bronze_midgard_2_6_9.midgard_pool_balance_change_events
WHERE block_timestamp = 1647355019937352448
AND asset = 'ETH.ETH'
AND reason = 'burn dust'








SELECT MAX(day) AS d
, MAX(day) AS dd
FROM flipside_dev_db.thorchain.daily_pool_stats



SELECT day
, pool_name
, COUNT(1) AS n
, COUNT(1) AS nn
FROM flipside_dev_db.thorchain.daily_pool_stats
GROUP BY 1, 2
HAVING COUNT(1) > 1
ORDER BY 3 DESC

SELECT day
, pool_name
, COUNT(1) AS n
, COUNT(1) AS nn
FROM flipside_dev_db.thorchain.daily_pool_stats
GROUP BY 1, 2
HAVING COUNT(1) > 1
ORDER BY 3 DESC

SELECT day
, asset
, COUNT(1) AS n
, COUNT(1) AS nn
FROM flipside_dev_db.thorchain.pool_block_statistics
GROUP BY 1, 2
HAVING COUNT(1) > 1
ORDER BY 3 DESC


WITH base AS (
	SELECT tx
	, block_timestamp
	, COUNT(1) AS n
	, COUNT(1) AS nnn
	FROM flipside_dev_db.bronze_midgard_2_6_9.midgard_bond_events
	GROUP BY 1, 2
	HAVING COUNT(1) > 1
	ORDER BY 3 DESC
)
SELECT tx
, COUNT(1) AS n
, COUNT(1) AS nn
FROM base
GROUP BY 1
HAVING COUNT(1) > 1
ORDER BY 2 DESC

SELECT *
FROM flipside_dev_db.bronze_midgard_2_6_9.midgard_bond_events
WHERE tx = '0000000000000000000000000000000000000000000000000000000000000000'
LIMIT 1000

SELECT block_id, tx_id
, COUNT(1) AS n
, COUNT(1) AS nn
FROM thorchain.bond_events
WHERE tx = '0000000000000000000000000000000000000000000000000000000000000000'
GROUP BY 1, 2
HAVING COUNT(1) > 1
ORDER BY 3 DESC


SELECT *
FROM thorchain.bond_events
WHERE tx = '0000000000000000000000000000000000000000000000000000000000000000'


SELECT project_name
, COUNT(1) AS n
, COUNT(1) AS nn
FROM solana.dim_nft_metadata
GROUP BY 1
ORDER BY 1



SELECT tx_id
, n.mint
, l.project_name AS lp
, m.project_name AS mp
, n.block_timestamp AS sale_date
, (inner_instruction:instructions[0]:parsed:info:lamports 
+ inner_instruction:instructions[1]:parsed:info:lamports 
+ inner_instruction:instructions[2]:parsed:info:lamports 
+ inner_instruction:instructions[3]:parsed:info:lamports) / POWER(10, 9) AS price
FROM solana.nfts n
LEFT JOIN crosschain.address_labels l ON LOWER(n.mint) = LOWER(l.address)
LEFT JOIN solana.dim_nft_metadata m ON LOWER(m.mint) = LOWER(l.address)
WHERE block_timestamp >= CURRENT_DATE - 200
AND instruction:data like '3UjLyJvuY4%'
AND COALESCE(l.project_name, m.project_name) IN ('degods','stoned ape crew','Astrals','Cets On Creck','DeFi Pirates')
LIMIT 100



SELECT tx_id
, s.mint
, l.project_name AS lp
, m.project_name AS mp
, s.block_timestamp AS sale_date
, sales_amount AS price
FROM solana.fact_nft_sales s
LEFT JOIN crosschain.address_labels l ON LOWER(s.mint) = LOWER(l.address)
LEFT JOIN solana.dim_nft_metadata m ON LOWER(m.mint) = LOWER(s.mint)
WHERE block_timestamp >= CURRENT_DATE - 200
AND COALESCE(l.project_name, m.project_name) IN ('degods','stoned ape crew','DeGods','Stoned Ape Crew','Astrals','Cets On Creck','DeFi Pirates')
LIMIT 100

SELECT tx_id
, n.mint
, n.mint


SELECT CAST(token_id AS INT) AS token_id
, token_metadata:Hat AS Hat
, token_metadata:Eyes AS Eyes
, token_metadata:Hair AS Hair
, token_metadata:Skin AS Skin
, token_metadata:Type AS Type
, token_metadata:Cloth AS Cloth
, token_metadata:Mouth AS Mouth
, token_metadata:Necklace AS Necklace
, token_metadata:Specialty AS Background
, token_metadata:"Attribute Count" AS Attribute_Count
FROM solana.dim_nft_metadata
WHERE project_name = 'Aurory'
ORDER BY CAST(token_id AS INT)

SELECT *
FROM thorchain.swaps
WHERE tx_id IN (
	'167CF767E6163475F307A6EDC34F37C70B108094A0BE7C8A718C13BFC3213ED1'
	, '9426EFC03348B4EAD11E97792B6CAFC049D2719B28CA49E84E6D627F4D1F74F6'
)

SELECT MAX(block_timestamp)
FROM solana.transactions
WHERE block_timestamp >= CURRENT_DATE - 5

SELECT 
MAX(block_timestamp) AS n
, MAX(block_timestamp) AS nn
FROM thorchain.swaps
WHERE block_timestamp >= CURRENT_DATE - 10

SELECT to_address
, COUNT(1) AS n
FROM thorchain.swaps
GROUP BY 1
ORDER BY 2 DESC
WHERE tx_id IN (
	'167CF767E6163475F307A6EDC34F37C70B108094A0BE7C8A718C13BFC3213ED1'
	, '9426EFC03348B4EAD11E97792B6CAFC049D2719B28CA49E84E6D627F4D1F74F6'
)

SELECT *
FROM thorchain.swaps
WHERE to_address = 'bnb1rv89nkw2x5ksvhf6jtqwqpke4qhh7jmudpvqmj'

SELECT *
FROM solana.dim_nft_metadata
WHERE project_name = 'Aurory'
LIMIT 10

SELECT program_id
, COUNT(1) AS n
FROM solana.nfts
WHERE block_timestamp >= '2022-03-01'
AND block_timestamp <= '2022-03-03'
GROUP BY 1

SELECT *
FROM solana.transfers
WHERE block_timestamp >= '2022-03-15'
AND tx_id = 'YuwerNe5KLLj8RFUj97igYm9icMxuYTHAvgy7C2dPMoebUDPmYQ7rXUrHqPuhkytZTZ74iduokutr6gghUDGZvM'


SELECT blockchain
, pool_name
, from_asset
, to_asset
, COUNT(1) AS n
, SUM(to_amount_usd) AS usd
FROM thorchain.swaps
WHERE block_timestamp >= '2022-03-09'
GROUP BY 1, 2, 3, 4
ORDER BY 6 DESC

SELECT *
FROM thorchain.swaps
WHERE block_timestamp >= '2022-03-09'
	AND pool_name = 'DOGE.DOGE'
ORDER BY block_timestamp DESC
LIMIT 100

SELECT *
FROM thorchain.swap_events
WHERE memo like '%/%'
LIMIT 100

SELECT *
FROM ethereum.nft_events

SELECT project_name
, COUNT(1) AS n
, SUM(CASE WHEN price_usd >= 1000000 THEN 1000000 ELSE 1000000 END) AS price_usd
FROM ethereum.nft_events
WHERE block_timestamp >= '2022-03-01'
AND price_usd >= 100
GROUP BY 1
ORDER BY 3 DESC

SELECT *
FROM thorchain.block_pool_depths
LIMIT 100


SELECT *
FROM solana.nfts
WHERE block_timestamp >= '2022-03-20'
AND tx_id = '4bjynBRfwRJ24vbYZDMVdrhN5GhZqpHDEtXZHSpNny8GzAr6X8NCBBy4g4jeXVCVmPeNbP1evyUDr6LZxhmwc11q'

SELECT project_name
, COUNT(1) AS n
FROM solana.nft_metadata
GROUP BY 1
ORDER BY 1

SELECT *
FROM solana.transfers
WHERE block_timestamp >= '2021-12-04'
AND block_timestamp <= '2021-12-08'
AND tx_id = '66suFvwSN83rCJ9VXAQXG21fGDYaWfcVbgiHHtfTL2fuLAZri61XuspKHwSPGjRJG7TZpR9dxDqioUSSLaBJB2Ue'


SELECT *
FROM solana.transfers 
WHERE block_timestamp >= '2022-02-06'
AND block_timestamp <= '2022-02-10'
AND source = '2L6j3wZXEByg8jycytabZitDh9VVMhKiMYv7EeJh6R2H'

SELECT *
FROM solana.transfers 
WHERE block_timestamp >= '2022-02-06'
AND block_timestamp <= '2022-02-10'
AND tx_id = 'BmtiqCyrPMG6tqXdukDQZsCyW1upaWVmjN6FLu62jwj3kiShQacSvCSGLUE3F3YsB8LAopj4yQGs58KjZKZHGAW'

SELECT record_content:results[0]:project_name AS project_name
, COUNT(1) AS n
, COUNT(1) AS n2
FROM bronze.prod_nft_metadata_uploads_1828572827
GROUP BY 1

SELECT *
, instructions[0]:programId AS programId
FROM solana.fact_transactions
WHERE block_timestamp >= '2022-02-24'
AND block_timestamp <= '2022-02-26'
AND tx_id IN (
	'5pTUfdXkHfG8WQhThmYW6fkVpp6oheTZRrLYD3mhANJ86XrkV75AQ8HNuj8CasHVrGHXGaep9nsNM4dTaVpYX4AB'
	, '9cUctD4sVARDyYQsV2zrpPuTatPS5aYZZuPehupDkUZLNfdRKP8zBmEqR4FnwdsSMEcV6y7VqJ92ap8vpCm5rfB'
)
AND instructions[0]:programId = 'Daovoteq2Y28gJyme6TNUXT9TxXrePiouFuHezkiozci'


SELECT *
, instructions[0]:programId AS programId
FROM solana.fact_transactions
WHERE block_timestamp >= '2022-03-05'
AND block_timestamp <= '2022-02-01'
AND tx_id IN (
	'5pTUfdXkHfG8WQhThmYW6fkVpp6oheTZRrLYD3mhANJ86XrkV75AQ8HNuj8CasHVrGHXGaep9nsNM4dTaVpYX4AB'
	, '9cUctD4sVARDyYQsV2zrpPuTatPS5aYZZuPehupDkUZLNfdRKP8zBmEqR4FnwdsSMEcV6y7VqJ92ap8vpCm5rfB'
)
AND instructions[0]:programId = 'Daovoteq2Y28gJyme6TNUXT9TxXrePiouFuHezkiozci'
LIMIT 1000

SELECT instructions[0]:accounts[6] AS acct_6
, instructions[0]:data AS data
, COUNT(1) AS n
, SUM(LEN(inner_instruction)) AS total_votes
FROM solana.fact_transactions
WHERE block_timestamp <= '2022-03-05'
	AND block_timestamp >= '2022-02-15'
	AND instructions[0]:programId = 'Daovoteq2Y28gJyme6TNUXT9TxXrePiouFuHezkiozci'
GROUP BY 1, 2


SELECT instructions[0]:accounts[6] AS acct_6
, instructions[0]:data AS data
, CASE WHEN ARRAY_SIZE(inner_instructions) = 1 THEN '1' 
	WHEN ARRAY_SIZE(inner_instructions) <= 3 THEN '2-3' 
	WHEN ARRAY_SIZE(inner_instructions) <= 9 THEN '4-9' 
	ELSE '10+'
END AS voting_power
, COUNT(1) AS n
FROM solana.fact_transactions
WHERE block_timestamp <= '2022-03-05'
	AND block_timestamp >= '2022-02-15'
	AND instructions[0]:programId = 'Daovoteq2Y28gJyme6TNUXT9TxXrePiouFuHezkiozci'
    AND succeeded = TRUE
GROUP BY 1, 2, 3


SELECT *
FROM 

SELECT *
, instructions[0]:programId AS programId
, instructions[0]:accounts[0] AS acct_0
, instructions[0]:accounts[1] AS acct_1
, instructions[0]:accounts[2] AS acct_2
, instructions[0]:accounts[3] AS acct_3
, instructions[0]:accounts[4] AS acct_4
, instructions[0]:accounts[5] AS acct_5
, instructions[0]:accounts[6] AS acct_6
FROM solana.fact_transactions
WHERE block_timestamp <= '2022-03-05'
AND block_timestamp >= '2022-03-02'
AND instructions[0]:programId = 'Daovoteq2Y28gJyme6TNUXT9TxXrePiouFuHezkiozci'
LIMIT 1000


WITH base AS (
	SELECT *
	, record_content:results[0]:project_name AS project_name
	, record_content:results[0]:token_id AS token_id_0
	, record_content:results[1]:token_id AS token_id_1
	FROM bronze.prod_nft_metadata_uploads_1828572827
	WHERE record_content:model:blockchain = 'solana'
	AND record_content:results[0]:project_name = 'Solana Monkey Business'
	ORDER BY _inserted_timestamp DESC
	LIMIT 10
)
SELECT b.*
, CASE WHEN m0.token_id IS NULL THEN 0 ELSE 1 END AS has_entry_0
, CASE WHEN m1.token_id IS NULL THEN 0 ELSE 1 END AS has_entry_1
FROM base b
LEFT JOIN solana.nft_metadata m0 ON m0.token_id = b.token_id_0
	AND m0.project_name = b.project_name
LEFT JOIN solana.nft_metadata m1 ON m1.token_id = b.token_id_1
	AND m1.project_name = b.project_name

SELECT *
FROM bronze.prod_nft_metadata_uploads_1828572827
WHERE record_content:model:blockchain = 'solana'
AND record_content:results[0]:project_name = 'Solana Monkey Business'
ORDER BY _inserted_timestamp DESC
LIMIT 10




SELECT block_timestamp
FROM thorchain.block_pool_depths
WHERE block_timestamp
order by block_timestamp DESC
GROUP BY 1
LIMIT 100


SELECT contract_name
, COUNT(1) AS n
FROM ethereum.nft_metadata
WHERE contract_name IN ('MutantApeYachtClub','bayc')
GROUP BY 1

SELECT *
FROM ethereum.nft_metadata
WHERE contract_name IN ('MutantApeYachtClub','bayc')
LIMIT 10000

SELECT contract_name
, token_id
, token_metadata:Background AS background
, token_metadata:Clothes AS clother
, token_metadata:Earring AS earring
, token_metadata:Eyes AS eyes
, token_metadata:Fur AS fur
, token_metadata:Hat AS hat
, token_metadata:Mouth AS mouth
FROM ethereum.nft_metadata
WHERE contract_name IN ('MutantApeYachtClub','bayc')
GROUP BY 1

SELECT pretokenbalances[0]:mint AS pre_mint_0
, pretokenbalances[1]:mint AS pre_mint_1
, posttokenbalances[0]:mint AS pos_mint_0
, posttokenbalances[1]:mint AS pos_mint_1
, CASE WHEN NOT COALESCE(pre_mint_0, '') IN ('', 'So11111111111111111111111111111111111111112') THEN pre_mint_0
	WHEN NOT COALESCE(pre_mint_1, '') IN ('', 'So11111111111111111111111111111111111111112') THEN pre_mint_1
	WHEN NOT COALESCE(pos_mint_0, '') IN ('', 'So11111111111111111111111111111111111111112') THEN pos_mint_0
	ELSE pos_mint_1 END
	AS clean_mint
, n.*
, inner_instruction:instructions[1]:parsed:info:amount AS amount
FROM solana.nfts n
WHERE block_timestamp >= '2022-03-14'
AND program_id = 'J7RagMKwSD5zJSbRQZU56ypHUtux8LRDkUpAPSKH4WPp'
LIMIT 10000

SELECT *
FROM solana.nfts
WHERE block_timestamp >= '2022-03-17'
AND tx_id = 'TxYhxw8KHyQsvUSSQascYWBdUFbzYSZcVHXFyex3npNRh5ib4mHoTJk6vxH2Eo7zSyZ71KFrvnpHRDNry3AASDN'


{
  "index": 4,
  "instructions": [
    {
      "parsed": {
        "info": {
          "amount": "0",
          "authority": "8LXpW8fPk757j5zMGnkLYFaPP46MrmtnG5H8W3LcsbNh",
          "destination": "AFf394pRN13JYGQYQ7pRQFHn4keGf5XXYpbASzqxpN2H",
          "source": "2S9uN7rZc2ztzipex5rt76nsm9G8vKuz3nZ4GZWekZFA"
        },
        "type": "transfer"
      },
      "program": "spl-token",
      "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    },
    {
      "parsed": {
        "info": {
          "amount": "9479999999",
          "authority": "8LXpW8fPk757j5zMGnkLYFaPP46MrmtnG5H8W3LcsbNh",
          "destination": "3BjpoZic969Wh8dvgYApuCKL3v5nzgpLAekvumD44qZJ",
          "source": "2S9uN7rZc2ztzipex5rt76nsm9G8vKuz3nZ4GZWekZFA"
        },
        "type": "transfer"
      },
      "program": "spl-token",
      "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    },
    {
      "parsed": {
        "info": {
          "amount": "1",
          "authority": "7Ppgch9d4XRAygVNJP4bDkc7V6htYXGfghX4zzG9r4cH",
          "destination": "3B2aKxDj4DH7Mo7R4Twdwvi6FLe7uDkX28FFHEv8UQoT",
          "source": "9yj8ZUM1ji2xTwePnHakrGW4gUsiswzvo27orp7v96JW"
        },
        "type": "transfer"
      },
      "program": "spl-token",
      "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    },
    {
      "parsed": {
        "info": {
          "account": "2S9uN7rZc2ztzipex5rt76nsm9G8vKuz3nZ4GZWekZFA",
          "destination": "ExHjH7nZufPmsbTZc1n9R1iWMGJPDaJTeDv3PA75iunH",
          "owner": "8LXpW8fPk757j5zMGnkLYFaPP46MrmtnG5H8W3LcsbNh"
        },
        "type": "closeAccount"
      },
      "program": "spl-token",
      "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    },
    {
      "parsed": {
        "info": {
          "account": "9yj8ZUM1ji2xTwePnHakrGW4gUsiswzvo27orp7v96JW",
          "authority": "7Ppgch9d4XRAygVNJP4bDkc7V6htYXGfghX4zzG9r4cH",
          "authorityType": "accountOwner",
          "newAuthority": "ExHjH7nZufPmsbTZc1n9R1iWMGJPDaJTeDv3PA75iunH"
        },
        "type": "setAuthority"
      },
      "program": "spl-token",
      "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    }
  ]
}

WITH base AS (
	SELECT pretokenbalances[0]:mint AS pre_mint_0
	, pretokenbalances[1]:mint AS pre_mint_1
	, posttokenbalances[0]:mint AS pos_mint_0
	, posttokenbalances[1]:mint AS pos_mint_1
	, CASE WHEN NOT COALESCE(pre_mint_0, '') IN ('', 'So11111111111111111111111111111111111111112') THEN pre_mint_0
		WHEN NOT COALESCE(pre_mint_1, '') IN ('', 'So11111111111111111111111111111111111111112') THEN pre_mint_1
		WHEN NOT COALESCE(pos_mint_0, '') IN ('', 'So11111111111111111111111111111111111111112') THEN pos_mint_0
		ELSE pos_mint_1 END
		AS clean_mint
	, n.*
	, inner_instruction:instructions[1]:parsed:info:amount AS amount
	FROM solana.nfts n
	WHERE block_timestamp >= '2022-03-14'
	AND program_id = 'J7RagMKwSD5zJSbRQZU56ypHUtux8LRDkUpAPSKH4WPp'
	LIMIT 10000
), cleaned AS (
	SELECT *
	, REPLACE(b.clean_mint, '"', '') AS clean_mint_2
	FROM base b
)
SELECT m.token_id
, b.*
FROM cleaned b
JOIN solana.nft_metadata m ON m.mint = clean_mint_2


SELECT *
FROM solana.nft_metadata 
WHERE mint = '98ofVuvQr1RiFKkZuehpgUJSkz9LUDqGEef4rvmXpfD7'




SELECT project_name
, token_id
, block_timestamp
, price
FROM ethereum.nft_events
WHERE project_name IN (
	'BoredApeYachtClub'
	, 'MutantApeYachtClub'
	, 'BoredApeKennelClub'
)

SELECT block_timestamp::date AS date
, to_asset
, pool_name
, CASE WHEN memo like '%/%' THEN 1 ELSE 0 END AS is_synth
, COUNT(1) AS n
FROM thorchain.swap_events
WHERE block_timestamp >= '2022-03-09'
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC

SELECT block_timestamp::date AS date
, CASE WHEN from_asset LIKE '%/%' OR to_asset LIKE '%/%' THEN 1 ELSE 0 END AS is_synth
, COUNT(1) AS n
FROM thorchain.swaps
WHERE block_timestamp >= '2022-03-09'
GROUP BY 1, 2
ORDER BY 1, 2

SELECT contract_name
, COUNT(1) AS n
FROM ethereum.nft_metadata
GROUP BY 1
ORDER BY 1

SELECT *
FROM ethereum.nft_metadata
WHERE contract_name IN ('MutantApeYachtClub','bayc')
ORDER BY contract_name, token_id


SELECT project_name
, COUNT(1) AS n
FROM solana.nft_metadata
GROUP BY 1
ORDER BY 1

SELECT *
FROM solana.nft_metadata
WHERE project_name = 'Solana Monkey Business'
LIMIT 10000


SELECT block_timestamp::date AS date
, COUNT(1) AS n
FROM solana.nfts
WHERE block_timestamp >= '2022-03-14'
AND program_id = 'J7RagMKwSD5zJSbRQZU56ypHUtux8LRDkUpAPSKH4WPp'
AND inner_instruction:instructions[1]:parsed:info:amount IS NOT NULL

SELECT m.token_id
, pretoken_balance[1]:mint AS pre_mint_1
, n.*
, inner_instruction:instructions[1]:parsed:info:amount AS amount
FROM solana.nfts n
JOIN solana.nft_metadata m ON m.mint = n.mint
WHERE block_timestamp >= '2022-03-14'
AND program_id = 'J7RagMKwSD5zJSbRQZU56ypHUtux8LRDkUpAPSKH4WPp'
LIMIT 100

SELECT
FROM solana.nfts
WHERE block_timestamp >= '2022-03-01'



SELECT *
FROM thorchain.swaps
LIMIT 10


SELECT from_asset
, to_asset
, pool_name
, CASE WHEN memo like '%/%' THEN 1 ELSE 0 END AS is_synth
, COUNT(1) AS n
FROM thorchain.swap_events
WHERE block_timestamp >= '2022-03-09'
GROUP BY 1, 2, 3
ORDER BY 4 DESC

cp -R props props-protocol find props \( -path "*/.git/*" -or -name ".git" \) -delete

And given t he quickly changing nature of such technology, most sources of traditional analytics 
are often caught playing "catch-up" rather than innovating.


SELECT pool
, from_asset
, to_asset
, COUNT(1) AS n
FROM thorchain_midgard_public
WHERE block_timestamp >= CURRENT_DATE - 3
GROUP BY 1, 2, 3
ORDER BY 4 DESC


SELECT *
FROM mdao_harmony.dfk_quest_rewards
WHERE log_id = '0xbfecf5e4d8e159c54103a0e7d8c21b27503be89e2048c02fabfb9cfa18e69f5b'


SELECT block_timestamp::date AS date
, COUNT(1) AS n
FROM algorand.transactions
GROUP BY 1
ORDER BY 1


SELECT *
FROM mdao_harmony.dfk_hero_updates
WHERE hero_id = 107255
ORDER BY block_timestamp

WITH base AS (
	SELECT hero_id
	, summoning_info_summonerid
	, summoning_info_assistantid
	, hero_info_class
	, hero_info_subclass
	, hero_stats_strength
	, hero_stats_intelligence
	, hero_stats_wisdom
	, hero_stats_luck
	, hero_stats_agility
	, hero_stats_vitality
	, hero_stats_endurance
	, hero_stats_dexterity
	, hero_stats_hp
	, hero_stats_mp
	, hero_stats_stamina
	, hero_primary_stat_growth_strength
	, hero_primary_stat_growth_intelligence
	, hero_primary_stat_growth_wisdom
	, hero_primary_stat_growth_luck
	, hero_primary_stat_growth_agility
	, hero_primary_stat_growth_vitality
	, hero_primary_stat_growth_endurance
	, hero_primary_stat_growth_dexterity
	, hero_primary_stat_growth_hpsm
	, hero_primary_stat_growth_hprg
	, hero_primary_stat_growth_hplg
	, hero_primary_stat_growth_mpsm
	, hero_primary_stat_growth_mprg
	, hero_primary_stat_growth_mplg
	, hero_secondary_stat_growth_strength
	, hero_secondary_stat_growth_intelligence
	, hero_secondary_stat_growth_wisdom
	, hero_secondary_stat_growth_luck
	, hero_secondary_stat_growth_agility
	, hero_secondary_stat_growth_vitality
	, hero_secondary_stat_growth_endurance
	, hero_secondary_stat_growth_dexterity
	, hero_secondary_stat_growth_hpsm
	, hero_secondary_stat_growth_hprg
	, hero_secondary_stat_growth_hplg
	, hero_secondary_stat_growth_mpsm
	, hero_secondary_stat_growth_mprg
	, hero_secondary_stat_growth_mplg
	, ROW_NUMBER() OVER (PARTITION BY hero_id ORDER BY block_timestamp) AS rn
	FROM mdao_harmony.dfk_hero_updates
), base2 AS (
	SELECT * 
	FROM base 
	WHERE rn = 1
)
SELECT b
FROM base2

Boaz Barak - CS 127 course about cryptography (you can talk about how you've bee working for a crypto company)
Finale Doshi-Velez - CS 181 course about machine learning
David Parkes - CS 181 course about machine learning

SELECT *
FROM mdao_harmony.dfk_quest_rewards
WHERE tx_hash = '0x3964399cdcb7f2de11a9da39a3d812fa4b9a88070caaa97d5d1ec5242d2f8d4b'


0xc855dea300000000000000000000000000000000000000000000000000000000000000600000000000000000000000006ff019415ee105acf2ac52483a33f5b43eadb8d000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000cc3c000000000000000000000000000000000000000000000000000000000000be8000000000000000000000000000000000000000000000000000000000000190b80000000000000000000000000000000000000000000000000000000000017df60000000000000000000000000000000000000000000000000000000000003158000000000000000000000000000000000000000000000000000000000001777d
0xf51333f50000000000000000000000000000000000000000000000000000000000000080000000000000000000000000e4154b6e5d240507f9699c730a496790a722df19000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000001a2f7000000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000018000000000000000000000000000000000000000000000000000000000000001a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
0xc855dea30000000000000000000000000000000000000000000000000000000000000060000000000000000000000000e259e8386d38467f0e7ffedb69c3c9c935dfaefc000000000000000000000000000000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000050000000000000000000000000000000000000000000000000000000000018345000000000000000000000000000000000000000000000000000000000000dada00000000000000000000000000000000000000000000000000000000000181f50000000000000000000000000000000000000000000000000000000000016280000000000000000000000000000000000000000000000000000000000000dbb0
0xc855dea300000000000000000000000000000000000000000000000000000000000000600000000000000000000000003132c76acf2217646fb8391918d28a16bd8a8ef4000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000030000000000000000000000000000000000000000000000000000000000018201000000000000000000000000000000000000000000000000000000000001663d000000000000000000000000000000000000000000000000000000000001a6e2
1: 0xf51333f50000000000000000000000000000000000000000000000000000000000000080000000000000000000000000e4154b6e5d240507f9699c730a496790a722df19000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000001a2f7000000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000018000000000000000000000000000000000000000000000000000000000000001a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
2: 0xc855dea300000000000000000000000000000000000000000000000000000000000000600000000000000000000000003132c76acf2217646fb8391918d28a16bd8a8ef4000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000030000000000000000000000000000000000000000000000000000000000018201000000000000000000000000000000000000000000000000000000000001663d000000000000000000000000000000000000000000000000000000000001a6e2
3: 0xc855dea300000000000000000000000000000000000000000000000000000000000000600000000000000000000000003132c76acf2217646fb8391918d28a16bd8a8ef400000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000001fbff00000000000000000000000000000000000000000000000000000000000233cc0000000000000000000000000000000000000000000000000000000000022d29000000000000000000000000000000000000000000000000000000000001c486

-- DFK level ups
SELECT log_id
, hero_id
, MIN(hero_state_level) AS mn_level
, MAX(hero_state_level) AS mx_level
FROM mdao_harmony.dfk_hero_updates
WHERE block_timestamp >= '2022-03-08'
GROUP BY 1, 2
HAVING mn_level < mx_level

SELECT *
FROM mdao_harmony.logs
WHERE tx_hash = '0x191f7317aedeef0ec87c3cb5834a798197deca7ab8885ace43bfb0ffbe7b744d'



SELECT RIGHT(LEFT(data, 10 + (64 * 2)), 2) AS s1
, RIGHT(LEFT(data, 10 + (64 * 3)), 2) AS s2
, RIGHT(LEFT(data, 10 + (64 * 4)), 2) AS s3
, *
FROM mdao_harmony.txs
WHERE block_timestamp >= '2022-03-09'
	AND native_to_address = 'one1qk2ds6efyvrk5gckat4yu89zsmd2zskpwyytuq'
	AND data like '0xfa863736%'
LIMIT 100

WITH base AS (
	SELECT RIGHT(LEFT(data, 10 + (64 * 2)), 2) AS s1
	, CASE WHEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) < RIGHT(LEFT(data, 10 + (64 * 4)), 2) THEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) ELSE RIGHT(LEFT(data, 10 + (64 * 4)), 2) END AS s2
	, CASE WHEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) > RIGHT(LEFT(data, 10 + (64 * 4)), 2) THEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) ELSE RIGHT(LEFT(data, 10 + (64 * 4)), 2) END AS s3
	, COUNT(1) AS n
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-03-09'
		AND native_to_address = 'one1qk2ds6efyvrk5gckat4yu89zsmd2zskpwyytuq'
		AND data like '0xfa863736%'
	GROUP BY 1, 2, 3
	ORDER BY 1, 2, 3
)
SELECT *
FROM base


SELECT tx_hash
, block_timestamp
, RIGHT(LEFT(data, 10 + 64), 64) AS hero_hash
, RIGHT(LEFT(data, 10 + (64 * 2)), 2) AS s1
, CASE WHEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) < RIGHT(LEFT(data, 10 + (64 * 4)), 2) THEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) ELSE RIGHT(LEFT(data, 10 + (64 * 4)), 2) END AS s2
, CASE WHEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) > RIGHT(LEFT(data, 10 + (64 * 4)), 2) THEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) ELSE RIGHT(LEFT(data, 10 + (64 * 4)), 2) END AS s3
FROM mdao_harmony.txs
WHERE block_timestamp >= '2022-03-09'
	AND native_to_address = 'one1qk2ds6efyvrk5gckat4yu89zsmd2zskpwyytuq'
	AND data like '0xfa863736%'
LIMIT 100

WITH base AS (
	SELECT *
	, ROW_NUMBER() OVER (PARTITION BY hero_id ORDER BY block_timestamp DESC) AS rn
	FROM mdao_harmony.dfk_hero_updates
)
SELECT *
FROM base WHERE rn = 1

And heal the earth which the angels have defiled, and announce the healing of the earth that I will heal it, and that not all the sons of men shall be destroyed through the mystery of all the things which the watchers have spoken and have taught their sons.



SELECT hero_state_level
, MAX(CAST hero_state_xp AS INT)
FROM mdao_harmony.dfk_hero_updates
GROUP BY 1

WITH base AS (
	SELECT hero_state_level
	, hero_state_xp
	FROM mdao_harmony.dfk_hero_updates
	GROUP BY 1, 2
	HAVING COUNT(1) > 10
)
SELECT hero_state_level
, MAX(hero_state_xp)
FROM base
GROUP BY 1

WITH start_meditation AS (
	SELECT tx_hash
	, block_timestamp
	, RIGHT(LEFT(data, 10 + 64), 64) AS hero_hash
	, RIGHT(LEFT(data, 10 + (64 * 2)), 2) AS s1
	, CASE WHEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) < RIGHT(LEFT(data, 10 + (64 * 4)), 2) THEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) ELSE RIGHT(LEFT(data, 10 + (64 * 4)), 2) END AS s2
	, CASE WHEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) > RIGHT(LEFT(data, 10 + (64 * 4)), 2) THEN RIGHT(LEFT(data, 10 + (64 * 3)), 2) ELSE RIGHT(LEFT(data, 10 + (64 * 4)), 2) END AS s3
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-03-09'
		AND native_to_address = 'one1qk2ds6efyvrk5gckat4yu89zsmd2zskpwyytuq'
		AND data like '0xfa863736%'
), end_meditation AS (
	SELECT tx_hash
	, block_timestamp
	, RIGHT(LEFT(data, 10 + 64), 64) AS hero_hash
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-03-09'
		AND native_to_address = 'one1qk2ds6efyvrk5gckat4yu89zsmd2zskpwyytuq'
		AND data like '0x756fcd69%'
)
SELECT s.*
, e.tx_hash AS end_hash
, ROW_NUMBER() OVER (PARTITION BY s.tx_hash ORDER BY s.block_timestamp DESC, e.block_timestamp) AS rn
FROM start_meditation s
JOIN end_meditation e ON e.hero_hash = s.hero_hash
	AND e.block_timestamp > s.block_timestamp
	AND DATEADD(hours, 24, s.block_timestamp) < e.block_timestamp



SELECT *
FROM mdao_harmony.dfk_hero_updates
WHERE block_timestamp >= '2022-03-09'
AND log_id like '0x9948be24eedafdf7185969524578f292faf6c642fc37d3a0b55c1582d650e607%'


WITH base AS (
	SELECT b.log_id
	, a.block_timestamp
	, b.block_timestamp
	, a.hero_state_level AS old_level
	, a.hero_stats_strength AS str_base
	, a.hero_stats_intelligence AS int_base
	, a.hero_stats_wisdom AS wis_base
	, a.hero_stats_luck AS lck_base
	, a.hero_stats_agility AS agi_base
	, a.hero_stats_vitality AS vit_base
	, a.hero_stats_endurance AS end_base
	, a.hero_stats_hp AS hp_base
	, a.hero_stats_mp AS mp_base
	, a.hero_stats_stamina AS stam_base
	, b.hero_stats_strength - a.hero_stats_strength AS str_inc
	, b.hero_stats_intelligence - a.hero_stats_intelligence AS int_inc
	, b.hero_stats_wisdom - a.hero_stats_wisdom AS wis_inc
	, b.hero_stats_luck - a.hero_stats_luck AS lck_inc
	, b.hero_stats_agility - a.hero_stats_agility AS agi_inc
	, b.hero_stats_vitality - a.hero_stats_vitality AS vit_inc
	, b.hero_stats_endurance - a.hero_stats_endurance AS end_inc
	, b.hero_stats_dexterity - a.hero_stats_dexterity AS dex_inc
	, b.hero_stats_hp - a.hero_stats_hp AS hp_inc
	, b.hero_stats_mp - a.hero_stats_mp AS mp_inc
	, b.hero_stats_stamina - a.hero_stats_stamina AS stam_inc
	, ROW_NUMBER() OVER (PARTITION BY a.hero_id ORDER BY a.block_timestamp DESC, b.block_timestamp) AS rn
	FROM mdao_harmony.dfk_hero_updates a
	JOIN mdao_harmony.dfk_hero_updates b ON a.hero_id = b.hero_id
		AND a.hero_state_level = b.hero_state_level - 1
		AND a.block_timestamp < b.block_timestamp
		AND DATEADD(hours, 24, a.block_timestamp) > b.block_timestamp
	WHERE a.block_timestamp >= '2022-03-01'
	AND a.hero_id = 107255
	ORDER BY b.block_timestamp
)
SELECT *
FROM base
WHERE rn = 1


select log_id
, tx_hash
, topics[1] as quest_id
from mdao_harmony.logs
WHERE quest_id IN (
	'0x00000000000000000000000000000000000000000000000000000000008d04c8'
)



SELECT *
FROM mdao_harmony.txs
WHERE block_timestamp >= '2022-02-20'
	AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
	AND data like '0x528be0a9%'
	AND tx_hash = '0x02d562dc33d8b92744aa73a685bc1938e324a8368a5977a1b9983a764d72e11d'

SELECT *
FROM mdao_harmony.txs
WHERE tx_hash = '0x2e929a6fc7326547eb0fd0cb52dd9bbe70f59229afa919ec9d48bed72bfc8903'

WITH q AS (
	SELECT DISTINCT tx_hash
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-03-01'
		AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND data like '0x528be0a9%'
), base AS (
	SELECT hero_id
	, block_timestamp
	, LEFT(log_id, LEN(log_id) - 5) AS tx_hash
	, MIN(hero_state_level) AS level
	, MIN(CAST(hero_professions_mining AS INT)) AS mn_mining
	, MIN(CAST(hero_professions_gardening AS INT)) AS mn_gardening
	, MIN(CAST(hero_professions_foraging AS INT)) AS mn_foraging
	, MIN(CAST(hero_professions_fishing AS INT)) AS mn_fishing
	, MAX(CAST(hero_professions_mining AS INT)) AS mx_mining
	, MAX(CAST(hero_professions_gardening AS INT)) AS mx_gardening
	, MAX(CAST(hero_professions_foraging AS INT)) AS mx_foraging
	, MAX(CAST(hero_professions_fishing AS INT)) AS mx_fishing
	, MIN(CAST(hero_state_xp AS INT)) AS mn_xp
	, MAX(CAST(hero_state_xp AS INT)) AS mx_xp
	FROM mdao_harmony.dfk_hero_updates u
	WHERE block_timestamp >= '2022-03-01'
	GROUP BY 1, 2, 3
)
SELECT b.*
, mx_xp - mn_xp AS xp_increase
, mx_fishing - mn_fishing AS fishing_increase
, mx_foraging - mn_foraging AS foraging_increase
, mx_gardening - mn_gardening AS gardening_increase
, mx_mining - mn_mining AS mining_increase
, CASE WHEN q.tx_hash IS NULL THEN 0 ELSE 1 END AS is_quest
FROM base b
LEFT JOIN q ON q.tx_hash = b.tx_hash
ORDER BY hero_id, block_timestamp
LIMIT 1000

WITH base AS (
	select try_base64_decode_string(tx_message:txn:apaa[0]::string) as code
	, block_timestamp::date
	, tx_group_id
	, app_id
	, ROW_NUMBER() OVER (PARTITION BY app_id ORDER BY block_timestamp DESC) AS rn
	from "flipside_dev_db"."ALGORAND"."APPLICATION_CALL_TRANSACTION" 
	where 1=1
	AND block_timestamp IS NOT NULL
	and code in ('sef','sfe')
)
SELECT * 
FROM base 
WHERE rn = 1

SELECT CASE WHEN block_timestamp IS NULL THEN 1 ELSE 0 END AS timstamp_is_null
, COUNT(1) AS n
, MAX(block_timestamp) AS mx
FROM algorand.application_call_transaction
GROUP BY 1



select max(block_timestamp)
from algorand.transactions 
WHERE block_timestamp >= '2022-03-01'

PactFi
PLXdumWjHCE6caDGNvwauHJChxFg7VJtZpC2uBqq8cU=

AlgoFi
yHcHFpy31C9CHHGm815PR4qRNL4STsJq4RElnEoDLjo=
LP
1iDBw/P9/SzpLnQI2eCUGYTaBlNsIousWSPQjEYaNdU=




SELECT 
tx_group_id
, try_base64_decode_string(tx_message:txn:note::string) AS code
, tx_message:txn:note::string AS note
, try_base64_decode_string(tx_message:txn:apaa[0]::string) AS code2
, (tx_message:txn:apaa[0]::string) AS apaa
, *
FROM algorand.application_call_transaction
WHERE block_timestamp >= '2022-03-01'
-- AND tx_group_id = 'PtUbYJnOE0+DTd+94YvepmCNub5NB6/vsjjP3ez6iYE='
AND code2 = 'ADDLIQ'
ORDER BY block_timestamp DESC
LIMIT 1000

SELECT *
FROM mdao_harmony.txs
WHERE block_timestamp >= '2022-03-01'
AND tx_hash = '0xeb43a66ee8c281bb9ee5b491e9e2036eeeaa8584e06727a8e69ab4e94b384f71'


0xc855dea30000000000000000000000000000000000000000000000000000000000000060000000000000000000000000569e6a4c2e3af31b337be00657b4c040c828dd7300000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000001e9f8


SELECT *
FROM algorand.application_call_transaction
WHERE block_timestamp >= '2022-03-04'
AND try_base64_decode_string(tx_message:txn:apaa[0]::string) = 'rpa1r'
ORDER BY block_timestamp DESC
LIMIT 100



select *
from algorand.transactions 
WHERE block_timestamp >= '2022-03-01'
AND try_base64_decode_string(tx_message:txn:apaa[0]::string) = 'swap'
AND tx_group_id = 'WfFULCOGyZUavqEyoT9CPidQuZQsxCCV3YsWy6MEjh0='
LIMIT 100

WITH a AS (
	SELECT 'Gold' AS token
	, price
	, ROW_NUMBER() OVER (ORDER BY timestamp DESC) AS rn
	FROM mdao_harmony.tokenprice_dfkgold
	UNION ALL
	SELECT 'GaiasTears' AS token
	, price
	, ROW_NUMBER() OVER (ORDER BY timestamp DESC) AS rn
	FROM mdao_harmony.tokenprice_gaiatear
	UNION ALL
	SELECT 'Jewel' AS token
	, price
	, ROW_NUMBER() OVER (ORDER BY timestamp DESC) AS rn
	FROM mdao_harmony.tokenprice_jewel
)
SELECT * 
FROM a
WHERE rn = 1


WITH name_map AS (
	SELECT token_name
	,evm_contract_address
	, COUNT(1) AS n
	FROM mdao_harmony.dfk_quest_rewards
	WHERE block_timestamp >= CURRENT_DATE - 14
	GROUP BY 1, 2
)
SELECT n.*, g.gold
FROM name_map n
LEFT JOIN mdao_harmony.dfk_item_to_gold g ON g.contract_address = n.evm_contract_address




WITH q AS (
	SELECT DISTINCT tx_hash
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-03-01'
		AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND data like '0x528be0a9%'
), base AS (
	SELECT hero_id
	, block_timestamp
	, LEFT(log_id, LEN(log_id) - 5) AS tx_hash
	, MIN(hero_state_level) AS level
	, MIN(CAST(hero_professions_mining AS INT)) AS mn_mining
	, MIN(CAST(hero_professions_gardening AS INT)) AS mn_gardening
	, MIN(CAST(hero_professions_foraging AS INT)) AS mn_foraging
	, MIN(CAST(hero_professions_fishing AS INT)) AS mn_fishing
	, MAX(CAST(hero_professions_mining AS INT)) AS mx_mining
	, MAX(CAST(hero_professions_gardening AS INT)) AS mx_gardening
	, MAX(CAST(hero_professions_foraging AS INT)) AS mx_foraging
	, MAX(CAST(hero_professions_fishing AS INT)) AS mx_fishing
	, MIN(CAST(hero_state_xp AS INT)) AS mn_xp
	, MAX(CAST(hero_state_xp AS INT)) AS mx_xp
	FROM mdao_harmony.dfk_hero_updates u
	WHERE block_timestamp >= '2022-03-01'
	GROUP BY 1, 2, 3
)
SELECT b.*
, mx_xp - mn_xp AS xp_increase
, mx_fishing - mn_fishing AS fishing_increase
, mx_foraging - mn_foraging AS foraging_increase
, mx_gardening - mn_gardening AS gardening_increase
, mx_mining - mn_mining AS mining_increase
, CASE WHEN q.tx_hash IS NULL THEN 0 ELSE 1 END AS is_quest
FROM base b
LEFT JOIN q ON q.tx_hash = b.tx_hash
ORDER BY hero_id, block_timestamp
LIMIT 1000

-- start gold mining
0x6033a41dcaf1852643db637cca601c1f55ee619331199e2bb01c5e8511623207
0x760500da5f7f137bd9bfc0a705a0230aab4e08ec000ee4fecc5d763e4ff4fc96
0x1fba20b42053972f77adee2415dc79d8484608c3b1cc82dc16f072d4fd423a99

-- gardening
0x39ef89ac1fb677c1fdeb1a55bc692d5e8a9a467633597ef06b096caebbf5a5c3
0xf51333f50000000000000000000000000000000000000000000000000000000000000080000000000000000000000000e4154b6e5d240507f9699c730a496790a722df19000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000001a2f7000000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000018000000000000000000000000000000000000000000000000000000000000001a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
0xf51333f50000000000000000000000000000000000000000000000000000000000000080000000000000000000000000e4154b6e5d240507f9699c730a496790a722df19000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000001a2f7000000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000018000000000000000000000000000000000000000000000000000000000000001a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
0xf51333f50000000000000000000000000000000000000000000000000000000000000080000000000000000000000000e4154b6e5d240507f9699c730a496790a722df19000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000001a2f7000000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000018000000000000000000000000000000000000000000000000000000000000001a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000

SELECT * 
FROM mdao_harmony.txs
WHERE block_timestamp >= '2022-03-05'
AND tx_hash IN (
	'0x6033a41dcaf1852643db637cca601c1f55ee619331199e2bb01c5e8511623207'
	, '0x760500da5f7f137bd9bfc0a705a0230aab4e08ec000ee4fecc5d763e4ff4fc96'
	, '0x1fba20b42053972f77adee2415dc79d8484608c3b1cc82dc16f072d4fd423a99'
)

0xc855dea300000000000000000000000000000000000000000000000000000000000000600000000000000000000000006ff019415ee105acf2ac52483a33f5b43eadb8d000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000b8c3000000000000000000000000000000000000000000000000000000000000e1b0000000000000000000000000000000000000000000000000000000000001109d000000000000000000000000000000000000000000000000000000000000ec6f


SELECT token_name
, COUNT(1) AS n
FROM mdao_harmony.dfk_quest_rewards
WHERE block_timestamp >= '2022-03-01'
GROUP BY 1
ORDER BY 2 DESC

-- start quest
WITH s AS (
	SELECT *
	, CASE
		WHEN data like '%569e6a4c2e3af31b337be00657b4c040c828dd73%' THEN 'Mining'
		WHEN data like '%e259e8386d38467f0e7ffedb69c3c9c935dfaefc%' THEN 'Fishing'
		WHEN data like '%3132c76acf2217646fb8391918d28a16bd8a8ef4%' THEN 'Foraging'
		WHEN data like '%e4154b6e5d240507f9699c730a496790a722df19%' THEN 'Gardening'
		ELSE 'Other'
	END AS quest_type
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-02-01'
		AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND native_from_address = 'one1r2kkl9unhvhd2rmacd304u48rt7u2zrkquazn7'
		AND data like '0xc855dea3%'
), e AS (
	SELECT tx_hash, block_timestamp, native_from_address
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-02-01'
		AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND native_from_address = 'one1r2kkl9unhvhd2rmacd304u48rt7u2zrkquazn7'
		AND data like '0x528be0a9%'
	GROUP BY 1, 2, 3
), r AS (
	SELECT log_id
	, token_name
	, calculated_value
	FROM mdao_harmony.dfk_quest_rewards
	WHERE block_timestamp >= '2022-02-01'
), base AS (
	SELECT hero_id
	, block_timestamp
	, LEFT(log_id, LEN(log_id) - 5) AS tx_hash
	, MIN(hero_state_level) AS level
	, MIN(CAST(hero_professions_mining AS INT)) AS mn_mining
	, MIN(CAST(hero_professions_gardening AS INT)) AS mn_gardening
	, MIN(CAST(hero_professions_foraging AS INT)) AS mn_foraging
	, MIN(CAST(hero_professions_fishing AS INT)) AS mn_fishing
	, MAX(CAST(hero_professions_mining AS INT)) AS mx_mining
	, MAX(CAST(hero_professions_gardening AS INT)) AS mx_gardening
	, MAX(CAST(hero_professions_foraging AS INT)) AS mx_foraging
	, MAX(CAST(hero_professions_fishing AS INT)) AS mx_fishing
	, MIN(CAST(hero_state_xp AS INT)) AS mn_xp
	, MAX(CAST(hero_state_xp AS INT)) AS mx_xp
	FROM mdao_harmony.dfk_hero_updates u
	WHERE block_timestamp >= '2022-03-01'
	GROUP BY 1, 2, 3
), m AS (
	SELECT s.quest_type
	, s.block_timestamp AS start_time
	, b.*
	, ROW_NUMBER() OVER (PARTITION BY s.tx_hash ORDER BY e.block_timestamp) AS rn
	FROM s
	JOIN e ON e.block_timestamp > s.block_timestamp 
		AND e.block_timestamp <= DATEADD('hour', 24, s.block_timestamp)
		AND s.native_from_address = e.native_from_address
	JOIN base b ON b.tx_hash = e.tx_hash
)
SELECT quest_type
, level
, AVG(mx_xp - mn_xp) AS avg_xp_inc
, COUNT(1) AS n
FROM m
WHERE rn = 1
GROUP BY 1, 2
ORDER BY 4 DESC

-- start quest
WITH s AS (
	SELECT *
	, CASE
		WHEN data like '%569e6a4c2e3af31b337be00657b4c040c828dd73%' THEN 'Mining'
		WHEN data like '%e259e8386d38467f0e7ffedb69c3c9c935dfaefc%' THEN 'Fishing'
		WHEN data like '%3132c76acf2217646fb8391918d28a16bd8a8ef4%' THEN 'Foraging'
		WHEN data like '%e4154b6e5d240507f9699c730a496790a722df19%' THEN 'Gardening'
		ELSE 'Other'
	END AS quest_type
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-03-01'
		AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND native_from_address = 'one1r2kkl9unhvhd2rmacd304u48rt7u2zrkquazn7'
		AND data like '0xc855dea3%'
), e AS (
	SELECT tx_hash, block_timestamp, native_from_address
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-03-01'
		AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND native_from_address = 'one1r2kkl9unhvhd2rmacd304u48rt7u2zrkquazn7'
		AND data like '0x528be0a9%'
	GROUP BY 1, 2, 3
), r AS (
	SELECT LEFT(log_id, LEN(log_id) - 5) AS tx_hash
	, token_name
	, calculated_value
	FROM mdao_harmony.dfk_quest_rewards
	WHERE block_timestamp >= '2022-03-01'
), base AS (
	SELECT hero_id
	, block_timestamp
	, LEFT(log_id, LEN(log_id) - 5) AS tx_hash
	, MIN(hero_state_level) AS level
	, MIN(CAST(hero_professions_mining AS INT)) AS mn_mining
	, MIN(CAST(hero_professions_gardening AS INT)) AS mn_gardening
	, MIN(CAST(hero_professions_foraging AS INT)) AS mn_foraging
	, MIN(CAST(hero_professions_fishing AS INT)) AS mn_fishing
	, MAX(CAST(hero_professions_mining AS INT)) AS mx_mining
	, MAX(CAST(hero_professions_gardening AS INT)) AS mx_gardening
	, MAX(CAST(hero_professions_foraging AS INT)) AS mx_foraging
	, MAX(CAST(hero_professions_fishing AS INT)) AS mx_fishing
	, MIN(CAST(hero_state_xp AS INT)) AS mn_xp
	, MAX(CAST(hero_state_xp AS INT)) AS mx_xp
	FROM mdao_harmony.dfk_hero_updates u
	WHERE block_timestamp >= '2022-03-01'
	GROUP BY 1, 2, 3
), m AS (
	SELECT s.quest_type
	, s.block_timestamp AS start_time
	, b.*
	, ROW_NUMBER() OVER (PARTITION BY s.tx_hash ORDER BY e.block_timestamp) AS rn
	FROM s
	JOIN e ON e.block_timestamp > s.block_timestamp 
		AND e.block_timestamp <= DATEADD('hour', 24, s.block_timestamp)
		AND s.native_from_address = e.native_from_address
	JOIN base b ON b.tx_hash = e.tx_hash
)
SELECT m.*
, mx_xp - mn_xp AS avg_xp_inc
, r.token_name
, r.calculated_value
FROM m
LEFT JOIN r ON r.tx_hash = m.tx_hash
WHERE rn = 1

-- quests with rewards by profession
WITH s AS (
	SELECT *
	, CASE
		WHEN data like '%569e6a4c2e3af31b337be00657b4c040c828dd73%' THEN 'Mining'
		WHEN data like '%e259e8386d38467f0e7ffedb69c3c9c935dfaefc%' THEN 'Fishing'
		WHEN data like '%3132c76acf2217646fb8391918d28a16bd8a8ef4%' THEN 'Foraging'
		WHEN data like '%e4154b6e5d240507f9699c730a496790a722df19%' THEN 'Gardening'
		ELSE 'Other'
	END AS quest_type
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-02-01'
		AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND data like '0xc855dea3%'
), e AS (
	SELECT tx_hash, block_timestamp, native_from_address
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-02-01'
		AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND data like '0x528be0a9%'
	GROUP BY 1, 2, 3
), r AS (
	SELECT LEFT(log_id, LEN(log_id) - 5) AS tx_hash
	, token_name
	, calculated_value
	FROM mdao_harmony.dfk_quest_rewards
	WHERE block_timestamp >= '2022-02-01'
), base AS (
	SELECT hero_id
	, block_timestamp
	, LEFT(log_id, LEN(log_id) - 5) AS tx_hash
	, MIN(hero_state_level) AS level
	, MIN(CAST(hero_stats_strength AS INT)) AS mn_str
	, MIN(CAST(hero_stats_dexterity AS INT)) AS mn_dex
	, MIN(CAST(hero_stats_agility AS INT)) AS mn_agi
	, MIN(CAST(hero_stats_vitality AS INT)) AS mn_vit
	, MIN(CAST(hero_stats_endurance AS INT)) AS mn_end
	, MIN(CAST(hero_stats_intelligence AS INT)) AS mn_int
	, MIN(CAST(hero_stats_wisdom AS INT)) AS mn_wis
	, MIN(CAST(hero_stats_luck AS INT)) AS mn_lck
	, MIN(CAST(hero_professions_mining AS INT)) AS mn_mining
	, MIN(CAST(hero_professions_gardening AS INT)) AS mn_gardening
	, MIN(CAST(hero_professions_foraging AS INT)) AS mn_foraging
	, MIN(CAST(hero_professions_fishing AS INT)) AS mn_fishing
	, MAX(CAST(hero_professions_mining AS INT)) AS mx_mining
	, MAX(CAST(hero_professions_gardening AS INT)) AS mx_gardening
	, MAX(CAST(hero_professions_foraging AS INT)) AS mx_foraging
	, MAX(CAST(hero_professions_fishing AS INT)) AS mx_fishing
	, MIN(CAST(hero_state_xp AS INT)) AS mn_xp
	, MAX(CAST(hero_state_xp AS INT)) AS mx_xp
	FROM mdao_harmony.dfk_hero_updates u
	WHERE block_timestamp >= '2022-02-01'
	GROUP BY 1, 2, 3
), m AS (
	SELECT s.quest_type
	, s.block_timestamp AS start_time
	, b.*
	, ROW_NUMBER() OVER (PARTITION BY s.tx_hash ORDER BY e.block_timestamp) AS rn
	FROM s
	JOIN e ON e.block_timestamp > s.block_timestamp 
		AND e.block_timestamp <= DATEADD('hour', 24, s.block_timestamp)
		AND s.native_from_address = e.native_from_address
	JOIN base b ON b.tx_hash = e.tx_hash
)
SELECT m.*
, mx_xp - mn_xp AS avg_xp_inc
, r.token_name
, r.calculated_value
FROM m
LEFT JOIN r ON r.tx_hash = m.tx_hash
WHERE rn = 1








SELECT *
, CASE
	WHEN data like '%569e6a4c2e3af31b337be00657b4c040c828dd73%' THEN 'Mining'
	WHEN data like '%e259e8386d38467f0e7ffedb69c3c9c935dfaefc%' THEN 'Fishing'
	WHEN data like '%3132c76acf2217646fb8391918d28a16bd8a8ef4%' THEN 'Foraging'
	WHEN data like '%e4154b6e5d240507f9699c730a496790a722df19%' THEN 'Gardening'
	WHEN data like '%0548214A0760a897aF53656F4b69DbAD688D8f29%' THEN 'WishingWell'
	ELSE 'Other'
END AS quest_type
FROM mdao_harmony.txs
WHERE block_timestamp >= '2022-02-01'
	AND from_address = '0x1aad6f9793bb2ed50f7dc362faf2a71afdc50876'
	AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
	AND data like '0xc855dea3%'

-- start quest
WITH s AS (
	SELECT *
	, CASE
		WHEN data like '%569e6a4c2e3af31b337be00657b4c040c828dd73%' THEN 'Mining'
		WHEN data like '%e259e8386d38467f0e7ffedb69c3c9c935dfaefc%' THEN 'Fishing'
		WHEN data like '%3132c76acf2217646fb8391918d28a16bd8a8ef4%' THEN 'Foraging'
		WHEN data like '%e4154b6e5d240507f9699c730a496790a722df19%' THEN 'Gardening'
		ELSE 'Other'
	END AS quest_type
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-03-06'
		AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND data like '0xc855dea3%'
)
SELECT *
FROM s
WHERE quest_type = 'Other'
LIMIT 100

WITH q AS (
	SELECT DISTINCT tx_hash
	FROM mdao_harmony.txs
	WHERE block_timestamp >= '2022-03-01'
		AND native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND data like '0x528be0a9%'
), base AS (
	SELECT hero_id
	, block_timestamp
	, LEFT(log_id, LEN(log_id) - 5) AS tx_hash
	, MIN(hero_state_level) AS level
	, MIN(CAST(hero_professions_mining AS INT)) AS mn_mining
	, MIN(CAST(hero_professions_gardening AS INT)) AS mn_gardening
	, MIN(CAST(hero_professions_foraging AS INT)) AS mn_foraging
	, MIN(CAST(hero_professions_fishing AS INT)) AS mn_fishing
	, MAX(CAST(hero_professions_mining AS INT)) AS mx_mining
	, MAX(CAST(hero_professions_gardening AS INT)) AS mx_gardening
	, MAX(CAST(hero_professions_foraging AS INT)) AS mx_foraging
	, MAX(CAST(hero_professions_fishing AS INT)) AS mx_fishing
	, MIN(CAST(hero_state_xp AS INT)) AS mn_xp
	, MAX(CAST(hero_state_xp AS INT)) AS mx_xp
	FROM mdao_harmony.dfk_hero_updates u
	WHERE block_timestamp >= '2022-03-01'
	GROUP BY 1, 2, 3
), base2 AS (
	SELECT b.*
	, mx_xp - mn_xp AS xp_increase
	, mx_fishing - mn_fishing AS fishing_increase
	, mx_foraging - mn_foraging AS foraging_increase
	, mx_gardening - mn_gardening AS gardening_increase
	, mx_mining - mn_mining AS mining_increase
	, CASE WHEN q.tx_hash IS NULL THEN 0 ELSE 1 END AS is_quest
	FROM base b
	JOIN q ON q.tx_hash = b.tx_hash
)
SELECT level
,AVG(xp_increase)
,COUNT(1) AS n
FROM base2
GROUP BY 1

SELECT token_name
,COUNT(1) AS n
FROM mdao_harmony.dfk_quest_rewards
WHERE block_timestamp >= '2022-03-01'
GROUP BY 1


select log_id
, tx_hash
, topics[1] as quest_id
from mdao_harmony.logs
WHERE tx_hash IN (
	'0x19020484cd737548759478053addf9fc877ed389e064f478a10d67209132227b'
	, '0x3fbccc087450a9273466dea29da1a0c4d8d5e3f07fd8ca97fd6676fd0b9a7ab3'
)

q_map AS (
	SELECT
	tx_hash
	, topics[1] as quest_id
	from mdao_harmony.logs
	WHERE block_timestamp >= '2022-02-01'
	AND evm_contract_address = '0x5100bd31b822371108a0f63dcfb6594b9919eaf4'
	GROUP BY 1, 2
)

with 
quest_start_table as (
  -- this query is used to select quests sent from my wallet to the quest contract
  select tx_hash
  from mdao_harmony.txs
  where from_address = '0x0ba43bae4613e03492e4c17af3b014b6c3202b9d' -- my address
  	and to_address = '0x5100bd31b822371108a0f63dcfb6594b9919eaf4' -- quest core contract
    --and tx_hash = '0x7d6f0613cc43cd928a2cc19c1fdb3718453145a77c31ada152c7cf4ab65bb66d' -- for testing purposes
   --and tx_hash ='0x5019b6b8518a3ab63803aab7fa4e09e751114953d863d078eea2f494fac6c835'
  and block_timestamp > current_date - 4
  order by block_timestamp desc
),
quest_id_table as (
  -- this query is used to select quests id that is a log from the quest contract
	select topics[1] as quest_id
  	from mdao_harmony.logs
  where 1=1
  and tx_hash in ( select tx_hash from quest_start_table ) 
  --and tx_hash = '0x7d6f0613cc43cd928a2cc19c1fdb3718453145a77c31ada152c7cf4ab65bb66d' -- for faster testing
  --and tx_hash ='0x5019b6b8518a3ab63803aab7fa4e09e751114953d863d078eea2f494fac6c835' -- testing
  and evm_contract_address = '0x5100bd31b822371108a0f63dcfb6594b9919eaf4' -- quest core contract
)
/*,
quest_log_getter as (
  -- this query is used to select rewards that is matching the questid
select * from mdao_harmony.txs
where from_address = '0x0ba43bae4613e03492e4c17af3b014b6c3202b9d' -- my address
  and to_address = '0x5100bd31b822371108a0f63dcfb6594b9919eaf4' -- quest core
  and block_timestamp >= '2022-02-10 02:14:56.000' -- test code , can remove
  and block_timestamp <= '2022-02-13 02:14:56.000' -- test code , can remove
  and data = '0x528be0a9000000000000000000000000000000000000000000000000000000000001011f' -- test code, can remove
)
*/

  -- select logs where quest_id matches topics[1] and topics[0] says quest reward (you probably need to filter more by contracts )
  select 
  substr(data,67,64) as token_address,
  substr(data,67+64,64) as amount_rewards
  from mdao_harmony.logs
where block_timestamp > current_date - 4
  and topics[1] in (select quest_id from quest_id_table) 
  and topics[0] = '0xd24d0ec0941a2f5cf71e34aab5120a6ec265b4ff45c78e510a05928202f82786' -- quest reward







SELECT *
FROM mdao_harmony.dfk_quest_rewards
WHERE block_timestamp >= '2022-03-01'
AND token_name = 'Jewels'
LIMIT 10

SELECT *
FROM mdao_harmony.dfk_hero_updates
WHERE hero_id = 107255
ORDER BY block_timestamp, hero_state_xp

SELECT *
FROM mdao_harmony.txs
WHERE native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
	AND data like '0x528be0a9%'
LIMIT 10

WITH base AS (
	SELECT DISTINCT tx_hash
	FROM mdao_harmony.txs
	WHERE native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND data like '0x528be0a9%'
), a AS (
	SELECT hero_state_level
	, hero_professions_mining
	, hero_professions_gardening
	, hero_professions_foraging
	, hero_professions_fishing
	, LEFT(log_id, LEN(log_id) - 5) AS tx_hash
	FROM mdao_harmony.dfk_hero_updates u
	JOIN base b ON b.tx_hash = tx_hash
	WHERE hero_id = 107255
)
SELECT *
FROM a


--How does that change based on their stats / level / what profession they do?
-- Labels are missing
-- Warrior = HERO_INFO_CLASS = '0'
-- Knight = HERO_INFO_CLASS = '1'
-- Thief = HERO_INFO_CLASS = '2'
-- Archer = HERO_INFO_CLASS = '3'
-- Priest = HERO_INFO_CLASS = '4'
-- Wizard = HERO_INFO_CLASS = '5'
-- Monk = HERO_INFO_CLASS = '6'
-- Pirate = HERO_INFO_CLASS = '7'
-- Paladin = HERO_INFO_CLASS = '16'
-- Darkknight = HERO_INFO_CLASS = '17'
-- Summoner = HERO_INFO_CLASS = '18'
-- Ninja = HERO_INFO_CLASS = '19'
-- Dragoon = HERO_INFO_CLASS = '24'
-- Sage = HERO_INFO_CLASS = '25'
-- Dreadknight = HERO_INFO_CLASS = '28'
WITH hero_info AS (
  --Grabs all the hero data
  --Need to join this table onto dfk_quest_rewards so that we are able
  -- to remove 'StartQuest' tx. Since XP is stored as a state, it messes up the AVG
  SELECT hi.*, SPLIT( LOG_ID , '-')  AS tx_hash FROM mdao_harmony.dfk_hero_updates hi)
  SELECT DISTINCT hi.HERO_INFO_CLASS, CAST(hi.HERO_STATE_LEVEL as int) as HERO_STATE_LEVEL, avg(hi.HERO_STATE_XP)
  FROM mdao_harmony.dfk_quest_rewards qr JOIN hero_info hi ON hi.tx_hash[0] = qr.TX_HASH  
  GROUP BY hi.HERO_INFO_CLASS , HERO_STATE_LEVEL ORDER BY hi.HERO_INFO_CLASS, HERO_STATE_LEVEL

WITH hero_info AS (
  SELECT HERO_STATE_LEVEL,HERO_STATE_XP, HERO_INFO_CLASS,CAST(hi.HERO_PROFESSIONS_MINING as int) as HERO_PROFESSIONS_MINING, CAST(hi.HERO_PROFESSIONS_GARDENING as int) as HERO_PROFESSIONS_GARDENING,
  CAST(hi.HERO_PROFESSIONS_FORAGING as int) as HERO_PROFESSIONS_FORAGING, CAST(hi.HERO_PROFESSIONS_FISHING as int) as HERO_PROFESSIONS_FISHING
  , SPLIT( LOG_ID , '-')  AS tx_hash FROM mdao_harmony.dfk_hero_updates hi)
  SELECT DISTINCT CAST(hi.HERO_STATE_LEVEL as int) as HERO_STATE_LEVEL, avg(hi.HERO_STATE_XP),
  CASE 
          WHEN 
            HERO_PROFESSIONS_MINING >= HERO_PROFESSIONS_GARDENING 
            AND HERO_PROFESSIONS_MINING >= HERO_PROFESSIONS_FORAGING
            AND HERO_PROFESSIONS_MINING >= HERO_PROFESSIONS_FISHING
  			AND HERO_PROFESSIONS_MINING != 0 THEN 'mining'
          WHEN 
            HERO_PROFESSIONS_GARDENING >= HERO_PROFESSIONS_MINING 
            AND HERO_PROFESSIONS_GARDENING >= HERO_PROFESSIONS_FORAGING
            AND HERO_PROFESSIONS_GARDENING >= HERO_PROFESSIONS_FISHING
  			AND HERO_PROFESSIONS_GARDENING != 0 THEN 'gardening'
          WHEN 
            HERO_PROFESSIONS_FORAGING >= HERO_PROFESSIONS_MINING 
            AND HERO_PROFESSIONS_FORAGING >= HERO_PROFESSIONS_GARDENING
            AND HERO_PROFESSIONS_FORAGING >= HERO_PROFESSIONS_FISHING 
  			AND HERO_PROFESSIONS_FORAGING != 0 THEN 'foraging'
          WHEN 
             HERO_PROFESSIONS_FISHING >= HERO_PROFESSIONS_MINING 
            AND HERO_PROFESSIONS_FISHING >= HERO_PROFESSIONS_GARDENING
            AND HERO_PROFESSIONS_FISHING >= HERO_PROFESSIONS_FORAGING
 			AND HERO_PROFESSIONS_FISHING != 0 THEN 'fishing'
         else 'professionless'
       END AS category   
  FROM mdao_harmony.dfk_quest_rewards qr JOIN hero_info hi ON hi.tx_hash[0] = qr.TX_HASH  
  group by HERO_STATE_LEVEL, CATEGORY 



	SELECT DATEDIFF(week, date_trunc('WEEK', block_timestamp), date_trunc('WEEK', CURRENT_TIMESTAMP)) AS weeks_ago
	, date_trunc('WEEK', block_timestamp) AS date
	, COUNT(1) AS n_quests
	, COUNT(DISTINCT native_from_address) as n_questers
	FROM mdao_harmony.txs
	WHERE native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
		AND data like '0x528be0a9%'
	GROUP BY 1, 2
	ORDER BY 1



WITH 
--Grabs all paying quests 
buckets AS (
  SELECT AS_VARCHAR(to_address) AS to_address,
  week(date_trunc('day', block_timestamp)) AS quest_week
  FROM mdao_harmony.dfk_quest_rewards
  WHERE block_timestamp < '2022-01-03'
  GROUP BY to_address, quest_week),
  
--Grabs the first week for paying quest  
first_week as (
  SELECT
  AS_VARCHAR(to_address) as to_address,
  min(week(date_trunc('day', block_timestamp))) AS first_quest_week
  FROM mdao_harmony.dfk_quest_rewards
  where block_timestamp < '2022-01-03'
  GROUP BY to_address
), 

--Grabs quest that pays out no rewards 
no_reward_buckets AS (
  SELECT from_address AS to_address, week(date_trunc('day', block_timestamp)) AS quest_week
  FROM mdao_harmony.txs
  WHERE native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
  AND data like '0x528be0a9%'
  AND tx_hash not in (SELECT tx_hash FROM mdao_harmony.dfk_quest_rewards)
  AND block_timestamp < '2022-01-03'
    --Only grab valid tx
  AND status = TRUE),
  
--Grabs the first week for non-paying quest 
first_week_no_rewards as (
  SELECT from_address as to_address, min(week(date_trunc('day', block_timestamp))) AS first_quest_week
  FROM mdao_harmony.txs
  WHERE native_to_address = 'one12yqt6vdcygm3zz9q7c7uldjefwv3n6h5trltq4'
  AND data like '0x528be0a9%'
  AND tx_hash not in (SELECT tx_hash FROM mdao_harmony.dfk_quest_rewards)
  AND block_timestamp < '2022-01-03'
  AND status = TRUE
  GROUP BY 1),
  
--Merges the two tables for first week  
merge_first_week AS (
  SELECT * FROM first_week_no_rewards 
  UNION ALL
  SELECT * FROM first_week ),
  
--If duplicate addresses, picks the one with a lower first week  
fiter_first_week AS (
    SELECT to_address, min(first_quest_week) AS first_quest_week
    FROM merge_first_week 
    GROUP BY to_address
    ORDER BY to_address),
    merge_buckets as (
    select * from buckets 
    union all
    select * from no_reward_buckets 
    ),  
    
--Creates table of user, week they quested, and week of first quest    
merge_login_first AS (
  SELECT a.to_address, 
  a.quest_week,
  b.first_quest_week AS first_quest 
  FROM merge_buckets a, fiter_first_week b
  WHERE a.to_address = b.to_address
  
), week_num AS (
  SELECT a.to_address, a.quest_week, b.first_quest_week AS first_quest, 
  a.quest_week - first_quest AS week_number 
  FROM buckets a, first_week b
  WHERE a.to_address = b.to_address
) SELECT first_quest AS first_week,
     SUM(CASE WHEN week_number = 0 THEN 1 ELSE 0 END) AS week_0,
       SUM(CASE WHEN week_number = 1 THEN 1 ELSE 0 END) AS week_1,
       SUM(CASE WHEN week_number = 2 THEN 1 ELSE 0 END) AS week_2,
       SUM(CASE WHEN week_number = 3 THEN 1 ELSE 0 END) AS week_3,
       SUM(CASE WHEN week_number = 4 THEN 1 ELSE 0 END) AS week_4,
       SUM(CASE WHEN week_number = 5 THEN 1 ELSE 0 END) AS week_5
       FROM week_num GROUP BY first_week ORDER BY first_week


AlgoFi
ALGO / OPUL
Add LP
Examples
gb/8+oHUk+3G6l7UGycJA+huSAQhycl5E4eYxsG5XXI=
ik0azydJLpeQi8qsRbt0zxDfmJDzX7V9cQ7mEzszOCo=
app_id = 613210847
Swap
Examples
yHcHFpy31C9CHHGm815PR4qRNL4STsJq4RElnEoDLjo=
c+X1QfLHuqpGbiecxYlUM7c74eThALxKl5Bp04sojOg=
gb/8+oHUk+3G6l7UGycJA+huSAQhycl5E4eYxsG5XXI=
xLHcgXj43Ts3h6FcgJGkwUZ3R05fDt16u13Jfb/eYGg=

app_id = 613210847

Pact.Fi
ALGO / USDC
app_id = 620995314

WITH orders AS (
	SELECT tx_id
	, block_timestamp AS sale_date
	, msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:token_id AS token_id
	, msg_value:execute_msg:execute_order:order:order:maker_asset:info:nft:contract_addr::string AS contract
	, msg_value:execute_msg:execute_order:order:order:taker_asset:amount::decimal/pow(10,6) AS price
	FROM terra.msgs 
	WHERE msg_value:contract::string = 'terra1eek0ymmhyzja60830xhzm7k7jkrk99a60q2z2t' 
	AND tx_status = 'SUCCEEDED'
	AND msg_value:execute_msg:execute_order IS NOT NULL
	AND contract IN ( '{}' )
), Lorders AS (
	SELECT tx_id
	, block_timestamp AS sale_date
	, msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:maker_asset:info:nft:token_id AS token_id
	, msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:maker_asset:info:nft:contract_addr::string AS contract
	, msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:taker_asset:amount::decimal/pow(10,6) AS price
	FROM terra.msgs 
	WHERE msg_value:contract::string = 'terra1eek0ymmhyzja60830xhzm7k7jkrk99a60q2z2t' 
	AND tx_status = 'SUCCEEDED'
	AND msg_value:execute_msg:ledger_proxy:msg:execute_order IS NOT NULL
	AND contract IN ( '{}' )
), unioned AS (
	SELECT * FROM orders
	UNION ALL 
	SELECT * FROM Lorders
)
SELECT CASE 
	WHEN contract = 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' THEN 'Levana Dust'
	WHEN contract = 'terra1chrdxaef0y2feynkpq63mve0sqeg09acjnp55v' THEN 'Levana Meteors'
	WHEN contract = 'terra1k0y373yxqne22pc9g7jvnr4qclpsxtafevtrpg' THEN 'Levana Dragon Eggs'
	WHEN contract = 'terra1trn7mhgc9e2wfkm5mhr65p3eu7a2lc526uwny2' THEN 'LunaBulls'
	WHEN contract = 'terra103z9cnqm8psy0nyxqtugg6m7xnwvlkqdzm4s4k' THEN 'Galactic Punks'
	ELSE 'Other' 
END AS collection
, sale_date
, token_id
, tx_id
, price
FROM unioned