1. Build in Public (Bravery)
2. Zero assumptions

When will you be out for?
Should we help out that 1st rock at all?
Thought leadership for NFTs as a utility
Why do we not have partners on Ethereum outside of Sushi?
Whats our sales pipeline look like?
Buillding stuff instead of analysis

VGX*dup.umt!rum4bqx
SELECT * FROM information_schema.tables

ProAd DLU
Chat + data for Michele to highlight the powers of the Flipside program for chains + analysts
Version 1.0 of THORChain ecosystem map
Chatted with Katana and built SDK demo for them
THORChain bounty GP reviewing
Launched flash bounty for ATOM launch
FYI ATOM launch has been a dud - they had to pause swaps for the last few days due to bug
Working with analytics to fix some data issues

Can we send ideas to you (Eric)?
Is the focus more on L2s or on Ethereum itself?

WITH sc1 AS (
  SELECT *
  , date_trunc('month', block_timestamp) AS month
  , rune_price
  , ROW_NUMBER() OVER (PARTITION BY month ORDER BY rune_liquidity DESC) AS rn
  FROM thorchain.daily_pool_stats
  WHERE pool_name IN ('BNB.BUSD-BD1','ETH.DAI-0X6B175474E89094C44DA98B954EEDEAC495271D0F','ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48')
)
SELECT * 
FROM sc1
WHERE rn = 1


Is solana too dependent on Serum
They use their transaction flows
Planning stage of expanding into other chains

Transactions for each month - report on network performance
From the start of 2022 - touch on the outages
Improvements in the network when they implemented quick
How many failed transactions from the same address (from top 10 addresses)
Work on skeleton of the report - do they want to collaborate on it
What caused the network outage
TPS / number of transactions
How to get the explorer involved
Detail + labeling in transaction logs (e.g. Program IDs are labeled)
We provide some of the charts, they provide context
"State of Solana" every quarter


WITH base AS (
  SELECT DATE_TRUNC('week', block_timestamp) AS date
  , signers[0]::string AS address
  , COUNT(1) AS n
  FROM solana.fact_transactions
  WHERE block_timestamp::date >= '2022-01-01'
  AND succeeded = FALSE
  GROUP BY 1, 2
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY date ORDER BY n DESC) AS rn
  FROM base
), b3 AS (
  SELECT date
  , CASE
    WHEN rn <= 10 THEN 'Top 10'
    WHEN rn <= 100 THEN 'Top 100'
    ELSE 'Other' END AS address_type
  , SUM(n) AS n
  FROM b2
  WHERE rn <= 10
  GROUP BY 1, 2
)
SELECT *
, n / (SUM(n) OVER ()) AS pct
FROM b3

SELECT *
FROM thorchain.swaps
WHERE pool_name LIKE 'GAIA.ATOM%'
ORDER BY block_timestamp
LIMIT 100

WITH base AS (
  SELECT native_to_address
  , COUNT(1) AS n_swaps
  , SUM(to_amount_usd) AS volume
  FROM thorchain.swaps
  WHERE to_asset LIKE 'ETH%'
  GROUP BY 1
  ORDER BY 3 DESC
  LIMIT 25
)

WITH base AS (
  SELECT signers[0]::string AS address
  , COUNT(1) AS n
  FROM solana.fact_transactions
  WHERE block_timestamp::date >= '2022-01-01'
  AND succeeded = FALSE
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 1000
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY date ORDER BY n DESC) AS rn
  FROM base
), b3 AS (
  SELECT date
  , CASE
    WHEN rn <= 10 THEN 'Top 10'
    WHEN rn <= 100 THEN 'Top 100'
    ELSE 'Other' END AS address_type
  , SUM(n) AS n
  FROM b2
  WHERE rn <= 10
  GROUP BY 1, 2
)
SELECT *
, n / (SUM(n) OVER ()) AS pct
FROM b3




SELECT s.block_timestamp::date AS date
, SUM(s.sales_amount) AS volume
FROM solana.core.fact_nft_sales s
JOIN solana.core.dim_nft_metadata m ON m.mint = s.mint AND m.collection = '${collection}'
WHERE s.block_timestamp::date < CURRENT_DATE::date
GROUP BY 1
ORDER BY 1


WITH base AS (
  SELECT day, date_trunc('month', day) AS month, SUM(asset_liquidity * asset_price_usd) AS non_rune_asset_value
  GROUP BY 1
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY month ORDER BY day DESC) AS rn
), b3 AS (
  SELECT month
  , non_rune_asset_value
  FROM b2 
  WHERE rn = 1
)
SELECT *
FROM b3

FROM FLIPSIDE_PROD_DB.THORCHAIN.DAILY_POOL_STATS
WHERE day = '2022-07-08'
ORDER BY non_rune_asset_value desc



SELECT *
, ARRAY_SIZE(t.instructions) AS arr_sz
FROM solana.core.fact_transactions t
WHERE block_timestamp >= '2022-07-03'
AND block_timestamp <= '2022-07-07'
AND tx_id = '3Gb17jSPuEpXDZjNLQ2VfoBEv7NCYz8dU9fjprouGCBzgKtqB7J4CQ5sX6UDxEd4rEBrDbWxyzHBcVJNp3uzwG2p'

SELECT *
FROM solana.core.dim_labels
WHERE label = '3d sniping demons'

WITH base AS (
  SELECT date_trunc('hour', block_timestamp) AS sacrifice_time
  , SUM(ARRAY_SIZE(t.instructions) / 2) AS n_burns
  FROM solana.core.fact_transactions t
  JOIN solana.core.dim_labels l ON l.address = t.instructions[0]:parsed:info:mint::string
  WHERE block_timestamp >= '2022-07-01'
  AND block_timestamp <= '2022-07-10'
  AND l.label = 'degentown'
  AND instructions[0]:parsed:type = 'burn'
  GROUP BY 1
)
SELECT *
, SUM(n_burns) OVER (ORDER BY sacrifice_time) AS cumu_burns
FROM base



with base as (select date_trunc('day', block_timestamp) as day,
  pool_name,
  CASE
WHEN CHARINDEX('-', pool_name) > 0 THEN LEFT(pool_name, CHARINDEX('-', pool_name)-1) ELSE pool_name END AS pool_names,
  case 
  when pool_name ilike 'BTC.%' then 'BTC'
  when pool_name ilike 'ETH.%' then 'ETH'
  when pool_name ilike 'BNB.%' then 'BNB'
  when pool_name ilike 'LTC.%' then 'LTC'
  when pool_name ilike 'DOGE.%' then 'DOGE'
  when pool_name ilike 'TERRA.%' then 'TERRA'
  when pool_name ilike 'BCH.%' then 'BCH'
  when pool_name ilike 'BNB.%' then 'BNB'
  else pool_name
  end as chain,
  avg(asset_amount_usd) as asset_liq,
  asset_liq * 2 as TVL,
  case 
  when chain = 'TERRA' and day >= '2022-05-09' then 0
  else TVL
  end as real_TVL
from thorchain.pool_block_balances
where asset_amount > 0 
  and date_trunc('day', block_timestamp) >= '2022-01-01'
group by 1,2
order by real_TVL desc),

base4 as (select day,
sum(real_TVL) as real_TVLz
from base
group by 1),

base5 as (select date_trunc('day', day) as day,
sum(total_value_bonded_usd) as total_value_bonded_usdz
from thorchain.daily_tvl
group by 1),

base2 as (select a.day,
((real_TVLz / 2) + total_value_bonded_usdz) / 301000000  as deterministic_price
from base4 a 
join base5 b 
on a.day = b.day),

base3 as (select date_trunc('day', block_timestamp) as day, 
avg(rune_usd) as daily_rune_price
from thorchain.prices
group by 1)
  
select a.day,
deterministic_price,
daily_rune_price,
daily_rune_price / deterministic_price as speculative_multiplier
from base2 a 
join base3 b 
on a.day = b.day



with degen as (SELECT address, label
FROM solana.core.dim_labels
WHERE label = 'degentown'),

discord_mods as (select purchaser, count(distinct mint) as number
from solana.core.fact_nft_mints mints
inner join degen on mints.mint = degen.address
group by purchaser
order by number desc),


mint_count as (select mint.purchaser as person, count(distinct mint) as mints
from solana.core.fact_nft_mints mint
inner join discord_mods on mint.purchaser = discord_mods.purchaser
  where mint.block_timestamp > CURRENT_DATE - 30
group by person
order by mints desc),


lisht as (select distinct purchaser as name
from solana.core.fact_nft_mints mints
inner join degen on mints.mint = degen.address)
,

rishi as ( 
  select *
  from solana.core.fact_nft_mints a
  inner join lisht on a.purchaser = lisht.name
  ),
  

too as (Select distinct purchaser,block_timestamp
  From (SELECT RANK() OVER (PARTITION BY purchaser order by block_timestamp asc) 
      as RN,purchaser,block_TIMESTAMP 
from rishi) as ST
  Where st.rn = 1
order by block_timestamp asc)
, first_tx AS (
  SELECT tx_to AS address, MIN(block_timestamp) AS mn
  FROM solana.core.fact_transfers
  GROUP BY 1
  UNION ALL
  SELECT tx_from AS address, MIN(block_timestamp) AS mn
  FROM solana.core.fact_transfers
  GROUP BY 1
), f2 AS (
  SELECT address, MIN(mn) AS first_tx_time
  FROM first_tx
  GROUP BY 1
)
, base AS (
select DISTINCT t.purchaser, CASE 
WHEN COALESCE(f2.first_tx_time, t.block_timestamp)::date >= '2022-06-30' AND t.block_timestamp::date = '2022-07-01' THEN 'Burner' 
  WHEN t.block_timestamp::date = '2022-07-01' THEN 'New Minter'
  ELSE 'Experienced Minter' END AS wallet_type
  from too t
  LEFT JOIN f2 ON f2.address = t.purchaser
)
select 
case when COALESCE(m.mints, 0) <= 1 then '1 - Normie'
when m.mints  < 10 then '2 - NFT Enthusiast'
 when m.mints  < 50 then '3 - Degen'
when m.mints  < 100 then '4 - High Power Degen'
else '5 - GOD MODE DEGEN' end as power_level, count(*) as numba
FROM base b 
LEFT JOIN  mint_count m ON b.purchaser = m.person
  where wallet_type = 'Experienced Minter'
group by power_level
order by numba desc







with degen as (SELECT address, label
FROM solana.core.dim_labels
WHERE label = 'degentown'),

lisht as (select distinct purchaser as name
from solana.core.fact_nft_mints mints
inner join degen on mints.mint = degen.address)
,

rishi as ( 
  select *
  from solana.core.fact_nft_mints a
  inner join lisht on a.purchaser = lisht.name
  ),
  

too as (Select distinct purchaser,block_timestamp
  From (SELECT RANK() OVER (PARTITION BY purchaser order by block_timestamp asc) 
      as RN,purchaser,block_TIMESTAMP 
from rishi) as ST
  Where st.rn = 1
order by block_timestamp asc)
, first_tx AS (
  SELECT tx_to AS address, MIN(block_timestamp) AS mn
  FROM solana.core.fact_transfers
  GROUP BY 1
  UNION ALL
  SELECT tx_from AS address, MIN(block_timestamp) AS mn
  FROM solana.core.fact_transfers
  GROUP BY 1
), f2 AS (
  SELECT address, MIN(mn) AS first_tx_time
  FROM first_tx
  GROUP BY 1
)
, base AS (
select CASE 
WHEN COALESCE(f2.first_tx_time, t.block_timestamp)::date >= '2022-06-30' AND t.block_timestamp::date = '2022-07-01' THEN 'Burner' 
  WHEN t.block_timestamp::date = '2022-07-01' THEN 'New Minter'
  ELSE 'Non-Burner' END AS wallet_type
  , block_timestamp::date AS date
  , count(1) AS n_wallets
  from too t
  LEFT JOIN f2 ON f2.address = t.purchaser
group by 1, 2 
order by 2 desc
)
SELECT *
, SUM(n_wallets) OVER () AS total_wallets
FROM base










[
  {
    "parsed": {
      "info": {
        "account": "3XRvFfcjoqXeTT4t9DLnqJBgok7tepMcvgG6sNPXLpW9",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "CKz4MabakdGnJ5icKXv9VdBAVjCcodWVA6D2Zop1ztz8"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "3XRvFfcjoqXeTT4t9DLnqJBgok7tepMcvgG6sNPXLpW9",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "8YDvXGdN7jxEh5A3TsS3KgHpaxEkjhJgXrzRpRKUwTMt",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "3XLdZCEpkZnrNpw3qv2AcRdQGgbk85T1QN2HBiRVXqUB"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "8YDvXGdN7jxEh5A3TsS3KgHpaxEkjhJgXrzRpRKUwTMt",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "36DZPspQzx61sti7porCqvHkiUf6zZPgKgoV1XdCevQj",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "7wShpK34PBwR8ys4oC4mR2zMYFTnB6ZhVQWxP2auo1dh"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "36DZPspQzx61sti7porCqvHkiUf6zZPgKgoV1XdCevQj",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "59JVeBTm4vtkWwjvmB7raB2nyW3wKj6NbV9ZgXzUutQv",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "EcaTM9LqBRqce9dLNu2kdWHm6kbW3cA4EjcZrWMeN1hm"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "59JVeBTm4vtkWwjvmB7raB2nyW3wKj6NbV9ZgXzUutQv",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "9Nfso2ao2E57KDjeFSsWGXQmEtDBT7WUnRGNAAof8vbH",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "6YAbarAamgwVggsKVfcK8VxaKfd33JB9sbnu5xrvXGyX"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "9Nfso2ao2E57KDjeFSsWGXQmEtDBT7WUnRGNAAof8vbH",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "Nbp9GwLWoCaVGJfqfiMpMXbTWaiAZt42kPtkPVrmxEQ",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "GtzGYEERyzAkXweXdzoSmUStMqhwS4y27q9DbHxDjfhV"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "Nbp9GwLWoCaVGJfqfiMpMXbTWaiAZt42kPtkPVrmxEQ",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "12xmesVmUBWeE4VTBJEQf8tJmxxGrdZzwBozMSptqKqF",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "91SAd85gQeCYNKHun94RDhYG8hMJmJzF2jAY6WisFHri"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "12xmesVmUBWeE4VTBJEQf8tJmxxGrdZzwBozMSptqKqF",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "HpNxFhnAfo4xCw1Rhf4wSf6A77VP3ySPXKHJSe9DavDA",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "GuFyccsRnc8JPhab5hyr3vv8k7WvoVShdDxGW9RHhJsh"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "HpNxFhnAfo4xCw1Rhf4wSf6A77VP3ySPXKHJSe9DavDA",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "Es36Bvuroth81DntriTDnNkkGdVAGMTrHf1DwYWame1M",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "9UcrGSbihtpMZQC1beAjM2WY6H9GC7PkkmDAxEsqeWqK"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "Es36Bvuroth81DntriTDnNkkGdVAGMTrHf1DwYWame1M",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "4Cjt4voswT1bJ3FWjUcx1bpUdyafciG9QRxe7UD7oHnG",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "5a8sxfwUUWkjZ6BgHWT6NZjZAjYQkiywFu2wYsu6YzVJ"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "4Cjt4voswT1bJ3FWjUcx1bpUdyafciG9QRxe7UD7oHnG",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "Etah1rY8SkS43V15kSi1YaxgZ63REiLuZwARrrHp4YRv",
        "amount": "1",
        "authority": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "mint": "H4W2WozmMcLgCfoe5EERgS57wcKupEwmhjr4CDr1viqF"
      },
      "type": "burn"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  },
  {
    "parsed": {
      "info": {
        "account": "Etah1rY8SkS43V15kSi1YaxgZ63REiLuZwARrrHp4YRv",
        "destination": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "multisigOwner": "835dotUpwzNnvWuHQW5DKpFjrvZp4zWJN2D2VmbPpz79",
        "signers": [
          "burn68h9dS2tvZwtCFMt79SyaEgvqtcZZWJphizQxgt"
        ]
      },
      "type": "closeAccount"
    },
    "program": "spl-token",
    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  }
]

Some idears...
"Flipside API in Python or R." -> Carlos or me? 
"EVM conceptual deep dive" -> Austin or Jim? 
"ShroomDK deep dive." -> Jim or Chris or Don?



WITH base AS (
  SELECT INITCAP(l.label) AS collection
  , COUNT(DISTINCT l.address) AS n_tokens
  , COUNT(DISTINCT m.mint) AS n_mints
  , AVG(m.mint_price) AS avg_mint_price
  , MIN(m.block_timestamp::date) AS mint_date
  , MIN(date_trunc('month', m.block_timestamp)) AS mint_month
  FROM solana.core.dim_labels l
  LEFT JOIN solana.core.fact_nft_mints m ON m.mint = l.address AND m.mint_price > 0 AND m.mint_price < 10
  WHERE label_subtype = 'nf_token_contract'
  GROUP BY 1
), volume AS (
  SELECT INITCAP(l.label) AS collection
  , COUNT(1) AS n_sales
  , COUNT(DISTINCT block_timestamp::date) AS n_days
  , SUM(sales_amount) AS volume
  , volume / n_days AS daily_volume
  FROM solana.core.fact_nft_sales s
  JOIN solana.core.dim_labels l ON l.address = s.mint
  GROUP BY 1
)
SELECT b.*
, COALESCE(b.avg_mint_price, 0) * b.n_tokens AS mint_volume
, v.n_sales
, v.n_days
, v.volume
, v.daily_volume
FROM base b
JOIN volume v ON v.collection = b.collection
ORDER BY mint_month DESC, mint_volume DESC

SELECT *
FROM solana.core.dim_labels
WHERE label ilike 'DegenTown'

SELECT *
FROM solana.core.fact_transactions t
JOIN solana.core.dim_labels l ON l.address = t.instructions[8]:parsed:info:mint::string
WHERE block_timestamp >= '2022-07-01'
AND instructions[0]:parsed:type = 'burn'
AND tx_id = '4RtsBQJ8EnDL2kh7cLQfuJrY4sMQVBUagRkHjvivzs1mcWGvdrgmqJ8yKUcF3r911XqeSGq5uswzD5ouWRBJCjGz'






with data_sol as (
  select
    instruction:accounts[0]::string as address
    , MIN(block_timestamp) AS wormhole_time
  from
    solana.core.fact_events
  where
  block_timestamp::date >= CURRENT_DATE - 30
  and
    program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
  and
    instruction:data = '4' -- retrieve from other chains
  and
    (
      inner_instruction:instructions[1]:parsed:type = 'mintTo' -- have careated associated token account
      or
      inner_instruction:instructions[3]:parsed:type = 'mintTo' -- not yet created associated token account
    )
  and
    succeeded = true
  group by 1
)
, data_user_programs as (
select
    b.address,
  case when label is null then COALESCE(program_id, 'None') else label end as labeling,
  ROW_NUMBER() OVER (PARTITION BY b.address ORDER BY block_timestamp) AS rn
from
  data_sol b
  LEFT JOIN 
  solana.core.fact_events a on a.block_timestamp > b.wormhole_time AND a.block_timestamp <= DATEADD('days', 4, b.wormhole_time) AND 
    b.address IN (
      instruction:accounts[0]
      , instruction:accounts[1]
      , instruction:accounts[2]
      , instruction:accounts[3]
      , instruction:accounts[4]
      , instruction:accounts[5]
      , instruction:accounts[6]
      , instruction:accounts[7]
      , instruction:accounts[8]
      , instruction:accounts[9]
    )
left join solana.core.dim_labels c on a.program_id = c.address
where
  a.block_timestamp::date >= CURRENT_DATE - 30
  and
    succeeded = true
  and
    program_id not in (
  'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb', -- exclude wormhole
  'worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth', -- exclude wormhole
  'DeJBGdMFa1uynnnKiwrVioatTuHmNLpyFKnmB5kaFdzQ', -- Phantom wallet program id for trasnfer https://docs.phantom.app/resources/faq
  '4MNPdKu9wFMvEeZBMt3Eipfs5ovVWTJb31pEXDJAAxX5' -- transfer token program
  ) -- exclude wormhole program id
  -- and
  --  block_timestamp::date >= CURRENT_DATE - 90
  and labeling != 'solana'
-- group by 1, 2 
and ROW_NUMBER() OVER (PARTITION BY b.address ORDER BY block_timestamp) <= 3
)
select 
  CASE
    when labeling = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' then 'Magic Eden V2'
    when labeling = '3Katmm9dhvLQijAvomteYMo6rfVbY5NaCRNq9ZBqBgr6' then 'Francium Lend Reward Program'
    when labeling = 'QMNeHCGYnLVDn1icRAfQZpjPLBNkfGbSKRB83G5d8KB' then 'Quarry Protocol'
    when labeling = 'VoLT1mJz1sbnxwq5Fv2SXjdVDgPXrb9tJyC8WpMDkSp' then 'Friktion Protocol'
    when labeling = 'hausS13jsjafwWwGqZTUQRmWyvyxn9EQpqMwV1PBBmk' then 'Opensea'
    when labeling = '2nAAsYdXF3eTQzaeUQS3fr4o782dDg8L28mX39Wr5j8N' then 'lyfRaydiumProgramID'
  else labeling end as label, 
  count(*) as total 
  from data_user_programs
  where label != 'solana' -- remove program id that dedicated to solana
  and rn <=3
  
  group by 1 order by 2 
  desc
//  limit 10


https://twitter.com/pinehearst_/status/1542532946632265728

Updates / Accomplishments
Ran our first THORChain flash bounty this week, done by Pinehearst
THORChain analysis on Terra LPs
Added Famous Foxes and Primates to NFT Deal Score
Helped Jack Forlines with some queries for wormhole + Michael with some stuff for Metaplex report
Finished the Solana NFT Metadata update script
Added THORNames + slash_points tables and updates to switch events tables 
Solana data sleuthing - worked with Jessica + Desmond to fix nft_mints + nft_sales tables
Problems Encountered
Wormhole query took a REALLY long time to run
Pretty clean week
Priorities
Get Solana NFT Metadata update script running on server
Chat with Eric about doing that cool wallet unique identifier thingy
Concerns
Nothing at the moment



  SELECT block_timestamp, SPLIT(to_asset, '-')[0] AS asset, from_address, to_amount AS usd_amount
  FROM flipside_prod_db.thorchain.swaps
  WHERE (to_asset ILIKE '%/BUSD%' OR to_asset = 'ETH/USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48') AND 
        block_timestamp >= TO_DATE('2022-01-01')

  SELECT block_timestamp, SPLIT(to_asset, '-')[0] AS asset, from_address, to_amount AS usd_amount
  FROM flipside_prod_db.thorchain.swaps
  WHERE (to_asset ILIKE '%/BUSD%' OR to_asset = 'ETH/USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48') AND 
        block_timestamp >= TO_DATE('2022-01-01')


SELECT SUM(to_e8) * POWER(10, -8) AS amt
FROM midgard.swap_events
WHERE to_asset ILIKE '%/BUSD%'

SELECT SUM(to_e8) * POWER(10, -8) AS amt
FROM thorchain.swap_events
WHERE to_asset ILIKE '%/BUSD%'

WITH base AS (
SELECT from_address
, SUM(from_amount_usd) AS volume_usd
FROM thorchain.swaps
WHERE pool_name LIKE '%BUSD%'
AND block_timestamp >= '2022-04-01'
GROUP BY 1
ORDER BY 2 DESC 
LIMIT 10
)
SELECT *
FROM base
ORDER BY 2 

WITH base AS (
  SELECT SPLIT(pool_name, '-')[0] AS pool_name
  , COUNT(1) AS n_swaps
  , ROUND(SUM(from_amount_usd)) AS usd_volume
  FROM thorchain.swaps
  WHERE from_address = 'thor1yd6m7pek6h6zh7p32d5xt20xcegz96pml0n208'
  OR native_to_address = 'thor1yd6m7pek6h6zh7p32d5xt20xcegz96pml0n208'
  GROUP BY 1
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY usd_volume DESC) AS rn
  FROM base
)
SELECT *
, CONCAT(CASE WHEN rn < 10 THEN '0', ELSE '' END, rn::varchar, '. ', pool_name) AS label
FROM b2


SELECT DISTINCT date_trunc('hour', block_timestamp) AS hour
FROM thorchain.UPDATE_NODE_ACCOUNT_STATUS_EVENTS
ORDER BY 1 DESC

SELECT COUNT(1) AS n, MAX(block_timestamp)
FROM thorchain.swaps

189779216.728215 - 188233930.596686
1,545,286

SELECT CASE WHEN (
  from_address IN ('thor1dhlfhswuqs64s0askzmkq3nqutaajunjj5n6wn','thor170e2xv7atjepzr8cjsjlpyq4fah6ykdagg6c2u','bnb1z3s7lqa96pw0netgzgge476xy8wp5eavg5vxv5')
  OR native_to_address IN ('thor1dhlfhswuqs64s0askzmkq3nqutaajunjj5n6wn','thor170e2xv7atjepzr8cjsjlpyq4fah6ykdagg6c2u','bnb1z3s7lqa96pw0netgzgge476xy8wp5eavg5vxv5')
) THEN 'Synth Hoarders' ELSE 'Others' END AS swapper
, COUNT(1) AS n_swaps
, ROUND(SUM(from_amount_usd)) AS usd_volume
FROM thorchain.swaps
WHERE pool_name LIKE '%.BUSD%'
AND block_timestamp::date >= '2022-04-25'
GROUP BY 1
ORDER BY 3 DESC

SELECT REPLACE(SPLIT(pool_name, '-')[0], '/', '.') AS poolname
, SUM(CASE WHEN from_asset LIKE '%/%' THEN from_amount_usd ELSE 0 END) AS synth_swap_volume
, SUM(CASE WHEN from_asset LIKE '%/%' THEN 0 ELSE from_amount_usd END) AS non_synth_swap_volume
, SUM(CASE WHEN from_asset LIKE '%/%' THEN liq_fee_rune_usd + liq_fee_asset_usd ELSE 0 END) AS synth_swap_fees
, SUM(CASE WHEN from_asset LIKE '%/%' THEN 0 ELSE liq_fee_rune_usd + liq_fee_asset_usd END) AS non_synth_swap_fees
FROM thorchain.swaps
WHERE block_timestamp::date >= '2022-04-25'
GROUP BY 1

WITH base AS (
  SELECT CASE WHEN pool_name LIKE '%BUSD%' THEN 'BUSD' ELSE 'Others' AS pool
  , SUM(CASE WHEN from_asset LIKE '%/%' THEN from_amount_usd ELSE 0 END) AS synth_swap_volume
  , SUM(CASE WHEN from_asset LIKE '%/%' THEN 0 ELSE from_amount_usd END) AS non_synth_swap_volume
  , SUM(CASE WHEN from_asset LIKE '%/%' THEN liq_fee_rune_usd + liq_fee_asset_usd ELSE 0 END) AS synth_swap_fees
  , SUM(CASE WHEN from_asset LIKE '%/%' THEN 0 ELSE liq_fee_rune_usd + liq_fee_asset_usd END) AS non_synth_swap_fees
  FROM thorchain.swaps
  WHERE block_timestamp::date >= '2022-04-25'
  GROUP BY 1
)
SELECT *
, synth_swap_fees / non_synth_swap_volume AS synth_swap_fees_vs_non_synth_swap_volume_ratio
FROM base



WITH base AS (
  SELECT CASE WHEN pool_name LIKE '%BUSD%' THEN 'BUSD' ELSE 'Others' END AS pool
  , SUM(CASE WHEN from_asset LIKE '%/%' THEN from_amount_usd ELSE 0 END) AS synth_swap_volume
  , SUM(CASE WHEN from_asset LIKE '%/%' THEN 0 ELSE from_amount_usd END) AS non_synth_swap_volume
  , SUM(CASE WHEN from_asset LIKE '%/%' THEN liq_fee_rune_usd + liq_fee_asset_usd ELSE 0 END) AS synth_swap_fees
  , SUM(CASE WHEN from_asset LIKE '%/%' THEN 0 ELSE liq_fee_rune_usd + liq_fee_asset_usd END) AS non_synth_swap_fees
  FROM thorchain.swaps
  WHERE block_timestamp::date >= '2022-04-25'
  GROUP BY 1
), b2 AS (
  SELECT *
  , synth_swap_fees / non_synth_swap_volume AS synth_swap_fees_vs_non_synth_swap_volume_ratio
  FROM base b
)
SELECT b2.*
, b3.synth_swap_fees_vs_non_synth_swap_volume_ratio * b2.synth_swap_volume AS expected_synth_swap_fees
, b2.synth_swap_fees - expected_synth_swap_fees AS difference
FROM b2
JOIN b2 b3 ON b3.pool = 'Others'




WITH synths AS (
  SELECT CASE WHEN native_to_address = '' THEN from_address ELSE native_to_address END AS address
  , SUM(to_amount) AS to_amount
  , COUNT(1) AS n_synths
  FROM thorchain.swaps
  WHERE to_asset LIKE '%/BUSD%'
  GROUP BY 1
  ORDER BY 2 DESC
), burns AS (
  SELECT from_address AS address
  , SUM(from_amount) AS from_amount
  , COUNT(1) AS n_burns
  FROM thorchain.swaps
  WHERE from_asset LIKE '%/BUSD%'
  GROUP BY 1
  ORDER BY 2 DESC
), base AS (
  SELECT COALESCE(s.address, b.address) AS address
  , COALESCE(s.to_amount, 0) AS synth_amount
  , COALESCE(b.from_amount, 0) AS burn_amount
  , COALESCE(s.n_synths, 0) AS n_synths
  , COALESCE(b.n_burns, 0) AS n_burns
  , synth_amount - burn_amount AS net_synth_amount
  , ROW_NUMBER() OVER (ORDER BY net_synth_amount DESC) AS rn
  FROM synths s
  FULL OUTER JOIN burns b on b.address = s.address
)
SELECT * FROM base

SELECT *
FROM thorchain.swaps
WHERE (
  from_address = 'thor163jhxz3tyf4qpgmsqjnmmdv80z40yr3hn6e6f9'
  OR to_pool_address = 'thor163jhxz3tyf4qpgmsqjnmmdv80z40yr3hn6e6f9'
  OR native_to_address = 'thor163jhxz3tyf4qpgmsqjnmmdv80z40yr3hn6e6f9'
) AND (
  from_asset LIKE '%/BUSD%'
  OR to_asset LIKE '%/BUSD%'
)


SELECT SUM(to_amount) AS to_amount
FROM thorchain.swaps
WHERE to_asset LIKE '%/BUSD%'
GROUP BY 1
ORDER BY 2 DESC

SELECT SUM(from_amount) AS from_amount
FROM thorchain.swaps
WHERE from_asset LIKE '%/BUSD%'
GROUP BY 1
ORDER BY 2 DESC

), burns AS (
SELECT from_address
, SUM(from_amount) AS from_amount
FROM thorchain.swaps
WHERE from_asset LIKE '%/BUSD%'
GROUP BY 1
ORDER BY 2 DESC


WITH synths AS (
  SELECT DATE_TRUNC('week', block_timestamp) AS week
  , SUM(to_amount) AS to_amount
  FROM thorchain.swaps
  WHERE to_asset LIKE '%/BUSD%'
  AND from_address = 'thor1yd6m7pek6h6zh7p32d5xt20xcegz96pml0n208'
  GROUP BY 1
  ORDER BY 2 DESC
), burns AS (
  SELECT  DATE_TRUNC('week', block_timestamp) AS week
  , SUM(from_amount) AS from_amount
  FROM thorchain.swaps
  WHERE from_asset LIKE '%/BUSD%'
  AND from_address = 'thor1yd6m7pek6h6zh7p32d5xt20xcegz96pml0n208'
  GROUP BY 1
  ORDER BY 2 DESC
)
SELECT COALESCE(s.week, b.week) AS week
, COALESCE(s.to_amount, 0) AS synth_amount
, COALESCE(b.from_amount, 0) AS burn_amount
, synth_amount - burn_amount AS net_synth_amount
, SUM(net_synth_amount) OVER (ORDER BY COALESCE(s.week, b.week)) AS cumu_net_synth_amount
FROM synths s
FULL OUTER JOIN burns b on b.week = s.week


SELECT from_address
, to_address
, from_asset
, to_asset
, COUNT(1) AS n_swaps
, SUM(from_amount_usd) AS swaps_volume
FROM thorchain.swaps
WHERE from_address = 'thor1yd6m7pek6h6zh7p32d5xt20xcegz96pml0n208'
OR to_address = 'thor1yd6m7pek6h6zh7p32d5xt20xcegz96pml0n208'
GROUP BY 1, 2, 3, 4
ORDER BY 6 DESC



WITH synths AS (
  SELECT from_address
  , SUM(to_amount) AS to_amount
  FROM thorchain.swaps
  WHERE to_asset LIKE '%/BUSD%'
  GROUP BY 1
  ORDER BY 2 DESC
), burns AS (
  SELECT from_address
  , SUM(from_amount) AS from_amount
  FROM thorchain.swaps
  WHERE from_asset LIKE '%/BUSD%'
  GROUP BY 1
  ORDER BY 2 DESC
), base AS (
SELECT COALESCE(s.from_address, b.from_address) AS from_address
, COALESCE(s.to_amount, 0) AS synth_amount
, COALESCE(b.from_amount, 0) AS burn_amount
, synth_amount - burn_amount AS net_synth_amount
, ROW_NUMBER() OVER (ORDER BY net_synth_amount DESC) AS rn
FROM synths s
FULL OUTER JOIN burns b on b.from_address = s.from_address
WHERE net_synth_amount > 0
ORDER BY net_synth_amount DESC
)
SELECT * 
, CASE WHEN rn < 10 THEN CONCAT('0', rn::varchar, '. ', from_address)
ELSE '10. Others' END AS label
FROM base







WITH to_synths AS (
  SELECT  DATE_TRUNC({{timeframe}}, block_timestamp) AS time, SPLIT(to_asset, '-')[0] AS asset, SUM(to_amount) AS to_amount, SUM(to_amount_usd) AS to_amount_usd
  FROM flipside_prod_db.thorchain.swaps
  WHERE block_timestamp >= TO_DATE('2022-03-11') 
        AND (to_asset ILIKE '%/%') 
        GROUP BY 1, 2
),
from_synths AS (
  SELECT DATE_TRUNC({{timeframe}}, block_timestamp) AS time, SPLIT(from_asset, '-')[0] AS asset, SUM(from_amount) AS from_amount, SUM(from_amount_usd) AS from_amount_usd
  FROM flipside_prod_db.thorchain.swaps
  WHERE block_timestamp >= TO_DATE('2022-03-11') 
        AND (from_asset ILIKE '%/%') 
        GROUP BY 1, 2
),
base AS (
  SELECT COALESCE(t.asset, f.asset) AS asset
  , COALESCE(t.time, f.time) AS time
  , COALESCE(to_amount, 0) AS to_amount
  , COALESCE(to_amount_usd, 0) AS to_amount_usd
  , COALESCE(from_amount, 0) AS from_amount
  , COALESCE(from_amount_usd, 0) AS from_amount_usd
  FROM to_synths t
  FULL OUTER JOIN from_synths f ON f.time = t.time AND f.asset = t.asset
)
SELECT *
, CASE WHEN from_amount = 0 THEN NULL ELSE to_amount * 1.0 / from_amount END AS amount_ratio
, to_amount_usd - from_amount_usd AS net_mint_amount_usd
, CASE WHEN from_amount_usd = 0 THEN NULL ELSE to_amount_usd * 1.0 / from_amount_usd END AS usd_ratio
, CASE WHEN to_amount = 0 THEN NULL ELSE from_amount * 1.0 / to_amount END AS amount_ratio_2
, CASE WHEN to_amount_usd = 0 THEN NULL ELSE from_amount_usd * 1.0 / to_amount_usd END AS usd_ratio_2
FROM base
WHERE (from_amount_usd + to_amount_usd) > 10000




Lean into IBC 
What is topical
Reach out directly on Twitter or on Discord
Getting retweets from big accounts
Use Reddit + Discord
Knowing what does well - adding NFT communities
Scanning mentions

Do we skip over regulatory?
Do we use money?

DraftKings + FanDuel were saying it's a skill game
Spin it up as a skill game?
VC funding comes first
From a legal standpoint that's huge
Shifting to real money is a high priority
Invite Manny to git

https://linktr.ee/multichain




Baller gonna ball in any market.

Holla at a Balla. 


SELECT *
FROM information_schema.tables

Got the most airdrops

Have we run any bounties on the number of the number of unique program ids?
Interactions bby programId
Solana dApp Store
FTX is positioning itself as a way to pay in crypto by converting on their platform
The benefit of having a bunch of Qualcomm guys running the show
Stocks are coming to FTX
I think FTX might replace Robinhood for me
I think SBF might be one of my favorite people - had to slow down video
The Orca girl is incredible
Mobile focus is bullish for props


with
-- We are interested in knowing how many users are currently in the ecosystem. 
-- In this case, how many are holding $RUNE in their wallet or have an open LP position. 
-- What is this number currently, and how has it trended over time?
  t1 as (
  SELECT
distinct from_address as user,
  min(block_timestamp) as debut
from flipside_prod_db.thorchain.bond_events
  group by 1
  ),
t2 as (
  SELECT
  distinct from_address as user ,
  min(block_timestamp) as debut
  from flipside_prod_db.thorchain.message_events
  group by 1
  ),
  t3 as (
  SELECT
  distinct rune_address as user ,
  min(block_timestamp) as debut
  from flipside_prod_db.thorchain.stake_events
  group by 1
  ),
  t4 as (
  SELECT
  distinct from_address as user,
  min(block_timestamp) as debut
  from flipside_prod_db.thorchain.swap_events
  group by 1
  ),
t5 as (
  SELECT
  distinct from_address as user,
  min(block_timestamp) as debut
  from flipside_prod_db.thorchain.transfer_events
  group by 1
  ),
  final as (
  select * from t1
  UNION
  select * from t2
  UNION
  select * from t3 
  UNION
  select * from t4
  UNION
  select * from t5
  ),
  final2 as (
  SELECT
  distinct user,
  min(debut) as final_debut
  from final
  group by 1
  ),
  total_users as (
SELECT
trunc(final_debut,'day') as date,
count(distinct user) as new_users,--,
sum(new_users) over (order by date) as total_users
from final2
group by 1
order by 1 asc
  ),
  t6 as (
  SELECT
  trunc(block_timestamp,'day') as date,
  from_address as user,
  sum(rune_amount) as rune_out,
  sum(rune_out) over (order by date) as cum_rune_out
FROM flipside_prod_db.thorchain.transfers 
  group by 1,2
),
  t7 as (
SELECT
  trunc(block_timestamp,'day') as date, 
  to_address as user,
  sum(rune_amount) as rune_in,
  sum(rune_in) over (order by date) as cum_rune_in
FROM flipside_prod_db.thorchain.transfers
  group by 1,2
  ),
  final_rune_holders as (
  SELECT
  t7.date,
  t7.user,
  cum_rune_in-cum_rune_out as net_rune
  from t7
  left join t6 on t7.user=t6.user and t7.date=t6.date
  ),
  rune_holders as (
  select date, count(distinct user) as new_users, sum(new_users) over (order by date) as total_users from final_rune_holders where net_rune>0 group by 1
  ),
t8 as (
  SELECT
  trunc(block_timestamp,'day') as date,
  from_address as user,
  pool_name,
  sum(asset_amount) as staked,
  sum(staked) over (partition by user,pool_name order by date) as cum_staked
FROM flipside_prod_db.thorchain.liquidity_actions
  group by 1,2,3
),
t9 as (
  SELECT
  trunc(block_timestamp,'day') as date, 
  to_address as user,
  pool_name,
  sum(asset_amount) as unstaked,
  sum(unstaked) over (partition by user,pool_name order by date) as cum_unstaked
FROM flipside_prod_db.thorchain.liquidity_actions
  group by 1,2,3
),
  final_loopers as (
  SELECT
  t8.date,
  t8.user,
  t8.pool_name,
  cum_staked-cum_unstaked as net_staked
  from t8
  left join t9 on t8.user=t9.user and t8.pool_name=t9.pool_name
  ),
  final_loopers2 as (
  SELECT
  date,user,sum(net_staked) as total_staked
  from final_loopers
  group by 1,2
  ),
  loopers as (
  select date, count(distinct user) as new_users, sum(new_users) over (order by date) as total_users from final_loopers2 where total_staked>0 group by 1
  )
select 'RUNE holders' as type,* from rune_holders
UNION
select 'Loopers' as type,* from loopers
  UNION
  select 'All users' as type,* from total_users




SELECT project_name, COUNT(DISTINCT mint) AS n_mint
FROM solana.dim_nft_metadata
GROUP BY 1
ORDER BY 1

WITH base AS (
  SELECT project_name AS collection
  , COUNT(DISTINCT mint) AS n_mint
  FROM solana.dim_nft_metadata
  GROUP BY 1
), b2 AS (
  SELECT CAST(((n_mint - 1) / 1000.0) AS INT) AS n_mint_grp
  , COUNT(1) AS n
  FROM base
  GROUP BY 1
)
SELECT CASE WHEN n_mint_grp >= 10 THEN '[10k, +)'
ELSE CONCAT('[',n_mint_grp,'k, ',n_mint_grp + 1,'k)') END AS n_mint_grp_2
, SUM(n) AS n
FROM b2
GROUP BY 1
ORDER BY 1



WITH base AS (
  SELECT m.project_name AS collection
  , SUM(sales_amount) AS sales_amount
  FROM solana.fact_nft_sales s
  JOIN solana.dim_nft_metadata m ON m.mint = s.mint
  WHERE s.block_timestamp >= '2022-01-01'
  GROUP BY 1
), b2 AS (
  SELECT collection
  , ROW_NUMBER() OVER (ORDER BY sales_amount DESC) AS rn
  FROM base
), b3 AS (
  SELECT m.project_name AS collection
  , date_trunc('month', s.block_timestamp) AS month
  , SUM(sales_amount) AS sales_amount
  FROM solana.fact_nft_sales s
  JOIN solana.dim_nft_metadata m ON m.mint = s.mint
  JOIN b2 ON b2.collection = m.project_name AND b2.rn <= 10
  WHERE s.block_timestamp >= '2022-01-01'
  GROUP BY 1, 2
)
SELECT *
, sales_amount / SUM(sales_amount) OVER (PARTITION BY month) AS pct
FROM b3


SELECT *
FROM solana.dim_nft_metadata
WHERE LOWER(project_name) IN (
  'cets on creck'
  , 'astrals'
  , 'solstein'
  , 'solgods'
  , 'okay bears'
  , 'meerkat millionaires'
  , 'catalina whale mixer'
  , 'citizens by solsteads'
  , 'defi pirates'
)


SELECT *
FROM solana.dim_nft_metadata
WHERE LOWER(project_name) IN (
  'astrals'
)



WITH mult_currency AS(
  SELECT DISTINCT l.label
  FROM solana_dev.silver.nft_mints m
  JOIN solana.dim_labels l ON m.mint = l.address
  WHERE m.mint_currency <> 'So11111111111111111111111111111111111111111'
), sales AS (
  SELECT l.label
  , COUNT(1) AS n_sales
  , SUM(s.sales_amount) AS volume
  , COUNT(DISTINCT s.mint) AS n_mints
  , MIN(block_timestamp::date) AS start_date
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l on s.mint = l.address
  GROUP BY 1
), mints AS (
  SELECT l.label
  , CASE WHEN e.label IS NULL THEN 0 ELSE 1 END AS is_mult_currency
  , COUNT(1) AS n_mints
  , COUNT(DISTINCT m.mint) AS n_unique_mints
  , SUM(m.mint_price) AS sol_mint_volume
  FROM solana_dev.silver.nft_mints
  JOIN solana.dim_labels l ON m.mint = l.address
  LEFT JOIN mult_currency e ON e.label = l.label
  GROUP BY 1, 2
)
SELECT s.*
, m.is_mult_currency
, m.n_mints
, m.n_unique_mints
, m.sol_mint_volume
, CASE WHEN s.n_mints > COALESCE(m.n_unique_mints, 0) THEN 1 ELSE 0 END AS is_missing_mint_events
FROM sales s
LEFT JOIN mints m ON m.label = s.label
WHERE s.start_date >= '2022-01-01'
ORDER BY s.volume DESC




WITH mult_currency AS(
  SELECT DISTINCT l.label
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON m.mint = l.address
  WHERE m.mint_currency <> 'So11111111111111111111111111111111111111111'
), sales AS (
  SELECT l.label
  , COUNT(1) AS n_sales
  , SUM(s.sales_amount) AS volume
  , COUNT(DISTINCT s.mint) AS n_mints
  , MIN(block_timestamp::date) AS start_date
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l on s.mint = l.address
  GROUP BY 1
), mints AS (
  SELECT l.label
  , CASE WHEN e.label IS NULL THEN 0 ELSE 1 END AS is_mult_currency
  , COUNT(1) AS n_mints
  , COUNT(DISTINCT m.mint) AS n_unique_mints
  , SUM(m.mint_price) AS sol_mint_volume
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON m.mint = l.address
  LEFT JOIN mult_currency e ON e.label = l.label
  GROUP BY 1, 2
)
SELECT s.*
, m.is_mult_currency
, m.n_mints
, m.n_unique_mints
, m.sol_mint_volume
, CASE WHEN s.n_mints > COALESCE(m.n_unique_mints, 0) THEN 1 ELSE 0 END AS is_missing_mint_events
FROM sales s
LEFT JOIN mints m ON m.label = s.label
WHERE s.start_date >= '2022-01-01'
ORDER BY s.volume DESC

SELECT *
FROM solana.fact_nft_mints
WHERE block_timestamp >= '2022-04-24'
AND  block_timestamp <= '2022-04-28'
AND mint = 'EJDtiPkuoRt9nNrQXhQDhtYuAGdV5JzQL9ZAVsRkEdJe'

SELECT

WITH sales AS (
  -- get all the sales
  SELECT DISTINCT l.label
  , s.mint
  , s.tx_id
  , s.block_timestamp::date AS date
  , s.sales_amount
  , MIN(date) OVER (PARTITION BY l.label) AS mn_date
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l on s.mint = l.address
), mints AS (
  -- get all the mints
  SELECT DISTINCT l.label
  , m.mint
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON m.mint = l.address
), base AS (
  -- check which sales do not have a corresponding mint
  SELECT s.*
  , ROW_NUMBER() OVER (PARTITION BY s.label ORDER BY s.sales_amount DESC) AS rn
  FROM sales s
  LEFT JOIN mints m ON m.mint = s.mint
  WHERE m.mint IS NULL AND mn_date >= '2022-02-01'
), b2 AS (
  -- take the top 3 from each collection
  SELECT *
  FROM base
  WHERE rn <= 3
)
-- select only the top 500 sales missing a mint
SELECT *
FROM b2 
ORDER BY sales_amount DESC
LIMIT 500

@poolprops I think were close

#1: $PROPS used to place bets
#2: Liquidity pools ensure ease of enter and exit
#3: 🤔
#4: $PROPS stakers earn % of transaction fees
#5: Contributors rewarded with $PROPS based on DAO vote
#6: Protocol sustained by small tx fees
#7: https://kellen-blumberg.gitbook.io/props/

SELECT l.label
, COUNT(1) AS n
, COUNT(DISTINCT m.mint) AS n_mints
FROM solana.fact_nft_mints m
JOIN solana.dim_labels l ON l.address = m.mint
GROUP BY 1


SELECT l.label
, m.mint
, COUNT(1) AS n_mints
, MAX(tx_id) AS mx_tx
, MIN(tx_id) AS mn_tx
FROM solana.fact_nft_mints m
JOIN solana.dim_labels l ON l.address = m.mint
GROUP BY 1, 2
HAVING COUNT(1) > 1
ORDER BY 3 DESC


WITH base AS (
  SELECT l.label
  , m.mint
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON l.address = m.mint
  GROUP BY 1, 2
  HAVING COUNT(1) > 1
)
SELECT *
FROM solana.fact_nft_mints m
JOIN base b ON b.mint = m.mint
ORDER BY block_timestamp DESC, m.mint, tx_id

WITH base AS (
  SELECT COALESCE(l.label, 'Unknownn')
  , date_trunc('month')
  , SUM(sales_amount) AS volume

)


WITH non_sol_mint AS(
  SELECT DISTINCT l.label
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON m.mint = l.address
  WHERE m.mint_currency <> 'So11111111111111111111111111111111111111111'
), mult_mints AS (
  SELECT l.label, m.mint
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON m.mint = l.address
  WHERE mint_price > 0
  GROUP BY 1, 2
  HAVING COUNT(1) > 1
), times AS(
  SELECT l.label
  , m.mint
  , m.mint_price
  , m.block_timestamp
  , m.mint_currency
  , m.tx_id
  , ROW_NUMBER() OVER (PARTITION BY l.label ORDER BY m.block_timestamp DESC) AS rn
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON m.mint = l.address
  LEFT JOIN non_sol_mint n ON n.label = l.label
  LEFT JOIN mult_mints mm ON mm.label = l.label
  WHERE n.label IS NULL
  AND mm.label IS NULL
  AND mint_price > 0
), rishi AS (
  SELECT label
  , MIN(block_timestamp) as mn_date
  , MAX(block_timestamp) as mx_date
  , AVG(mint_price) as avg_price
  , MIN(mint_price) as mn_price
  , MAX(mint_price) as mx_price
  , COUNT(DISTINCT ROUND(mint_price, 3)) as n_mint_prices
  , COUNT(DISTINCT mint) as n_mints
  FROM times
  GROUP BY 1
), sales AS (
  SELECT RANK() OVER (PARTITION BY l.label ORDER BY s.block_timestamp desc) AS rn
  , l.label
  , s.block_TIMESTAMP
  , s.marketplace
  , s.sales_amount
  , s.mint
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l on s.mint = l.address
), s2 AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY label ORDER BY sales_amount ASC) AS rn2
  FROM sales
  WHERE rn <= 20
), cur_floor AS (
  SELECT label
  , MIN(CASE WHEN rn2 = 4 THEN sales_amount ELSE NULL END) AS cur_floor
  , MEDIAN(sales_amount) AS cur_floor_md
  FROM s2
  GROUP BY 1
), total_sales AS (
  SELECT label
  , COUNT(1) AS n_sales
  , SUM(sales_amount) AS volume
  , COUNT(DISTINCT mint) as n_unique_sales
  FROM sales
  GROUP BY 1
), base AS (
  SELECT t.label
  , t.n_sales
  , n_mints
  , n_unique_sales
  , t.volume
  , f.cur_floor
  , f.cur_floor_md
  , r.mn_date
  , r.mx_date
  , DATEDIFF('days', mn_date, mx_date) AS dff
  , avg_price
  , mn_price
  , mx_price
  , mx_price - mn_price AS range
  , n_mint_prices
  , cur_floor - avg_price AS profit
  , cur_floor / avg_price AS return
  FROM total_sales t
  JOIN cur_floor f ON f.label = t.label
  JOIN rishi r ON r.label = t.label
  WHERE dff <= 10
  AND n_sales >= 30
  AND n_mints > 50
  AND NOT t.label IN ('rithfu','degen games: dice game','solchicks','vs-y1-22')
  AND mn_date >= '2022-01-01'
  AND mn_date < '2022-06-01'
  ORDER BY profit DESC
), b2 AS (
  SELECT t.label
  , t.mint
  , COUNT(1) AS n
  , COUNT(DISTINCT t.tx_id) AS n_tx
  FROM times t
  JOIN base b ON b.label = t.label
  GROUP BY 1, 2
  HAVING COUNT(1) > 1
  ORDER BY 3 DESC
)
SELECT t.label
, t.mint
, t.mint_price
, t.block_timestamp
, t.mint_currency
, t.tx_id
FROM times t 
JOIN b2 ON b2.label = t.label AND b2.mint = t.mint
ORDER BY t.block_timestamp DESC, t.label, t.mint, t.mint_price, t.tx_id


WITH non_sol_mint AS(
  SELECT DISTINCT l.label
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON m.mint = l.address
  WHERE m.mint_currency <> 'So11111111111111111111111111111111111111111'
), times AS(
  SELECT l.label
  , m.mint
  , m.mint_price
  , m.block_timestamp
  , m.mint_currency
  , ROW_NUMBER() OVER (PARTITION BY l.label ORDER BY m.block_timestamp DESC) AS rn
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON m.mint = l.address
  LEFT JOIN non_sol_mint e ON e.label = l.label
  WHERE e.label IS NULL
  AND mint_price > 0
), rishi AS (
  SELECT label
  , MIN(block_timestamp) as mn_date
  , MAX(block_timestamp) as mx_date
  , AVG(mint_price) as avg_price
  , MIN(mint_price) as mn_price
  , MAX(mint_price) as mx_price
  , COUNT(DISTINCT ROUND(mint_price, 3)) as n_mint_prices
  , COUNT(DISTINCT mint) as n_mints
  FROM times
  GROUP BY 1
), sales AS (
  SELECT RANK() OVER (PARTITION BY l.label ORDER BY s.block_timestamp desc) AS rn
  , l.label
  , s.block_TIMESTAMP
  , s.marketplace
  , s.sales_amount
  , s.mint
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l on s.mint = l.address
), s2 AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY label ORDER BY sales_amount ASC) AS rn2
  FROM sales
  WHERE rn <= 20
), cur_floor AS (
  SELECT label
  , MIN(CASE WHEN rn2 = 4 THEN sales_amount ELSE NULL END) AS cur_floor
  , MEDIAN(sales_amount) AS cur_floor_md
  FROM s2
  GROUP BY 1
), total_sales AS (
  SELECT label
  , COUNT(1) AS n_sales
  , SUM(sales_amount) AS volume
  , COUNT(DISTINCT mint) as n_unique_sales
  FROM sales
  GROUP BY 1
), base AS (
  SELECT t.label
  , t.n_sales
  , n_mints
  , n_unique_sales
  , t.volume
  , f.cur_floor
  , f.cur_floor_md
  , r.mn_date
  , r.mx_date
  , DATEDIFF('days', mn_date, mx_date) AS dff
  , avg_price
  , mn_price
  , mx_price
  , mx_price - mn_price AS range
  , n_mint_prices
  , cur_floor - avg_price AS profit
  , cur_floor / avg_price AS return
  FROM total_sales t
  JOIN cur_floor f ON f.label = t.label
  JOIN rishi r ON r.label = t.label
  WHERE dff <= 10
  AND n_sales >= 30
  AND n_mints > 50
  AND NOT t.label IN ('rithfu','degen games: dice game','solchicks')
  AND mn_date >= '2022-01-01'
  AND mn_date < '2022-06-01'
  ORDER BY profit DESC
), b2 AS (
  SELECT CASE WHEN avg_price < 0.25 THEN '1: (0, 0.5) $SOL'
    WHEN avg_price < 0.5 THEN '2: [0.5, 1.0) $SOL'
    WHEN avg_price < 2 THEN '3: [1.0, 2.0) $SOL'
    WHEN avg_price < 10 THEN '4: [2.0, 5.0) $SOL'
    ELSE '5: 5+ $SOL' END AS mint_price
  , CASE WHEN cur_floor < avg_price THEN '1: Unprofitable'
    WHEN cur_floor - avg_price < 0.5 THEN '2: [0, 0.5) $SOL Profit'
    WHEN cur_floor - avg_price < 2 THEN '3: [0.5, 2.0) $SOL Profit'
    WHEN cur_floor - avg_price < 10 THEN '4: [2.0, 10.0) $SOL Profit'
    ELSE '5: 10+ $SOL Profit' END AS profit_group
  , COUNT(1) AS n_collections
  FROM base 
  GROUP BY 1, 2
)
SELECT *
, n_collections * 100 / SUM(n_collections) OVER (PARTITION BY mint_price) AS pct
FROM b2
ORDER BY mint_price, profit_group



SELECT m.*
FROM solana.fact_nft_mints m
JOIN solana.dim_labels l ON l.address = m.mint
WHERE l.label = 'jelly rascals'


secondtable AS (
  SELECT DISTINCT label
  ,sales_amount
  ,marketplace
  ,block_timestamp
  FROM (SELECT RANK() OVER (PARTITION BY label order by block_timestamp desc) 
      as RN,label,block_TIMESTAMP,marketplace,sales_amount //as date 
      from solana.core.fact_nft_sales join solana.core.dim_labels on solana.core.fact_nft_sales.mint = solana.core.dim_labels.address) as ST
Where ST.RN = 1),

profit_table as(Select rishi.label,rishi.price_of_mint,secondtable.sales_amount, Case when rishi.price_of_mint = 0 then 99999 else secondtable.sales_amount/rishi.price_of_mint end as profit 
from rishi
Inner join secondtable on rishi.label = secondtable.label
order by profit desc)



SELECT CASE WHEN profit < 1 THEN '1: Less than 1x Returns'
 WHEN profit >= 1 and profit < 2 THEN '2: 1-2x Returns'  
 WHEN profit >=2 and profit < 5  THEN '3: 2-5x Returns' 
 WHEN  profit >= 5 and profit < 10 THEN '4: 5-10x Returns'  
 WHEN profit >= 10 and profit < 50 THEN '5: 10-50x Returns'  
 WHEN profit > 50 THEN '6: More than 50x Returns' 
 END AS profitability, count(label) as collections
 From profit_table
GROUP by profitability
Order by profitability







SELECT CASE WHEN profit < 1 THEN '1: Less than 1x Returns'
 WHEN profit >= 1 and profit < 2 THEN '2: 1-2x Returns'  
 WHEN profit >=2 and profit < 5  THEN '3: 2-5x Returns' 
 WHEN  profit >= 5 and profit < 10 THEN '4: 5-10x Returns'  
 WHEN profit >= 10 and profit < 50 THEN '5: 10-50x Returns'  
 WHEN profit > 50 THEN '6: More than 50x Returns' 
 END AS profitability, count(label) as collections
 From profit_table
GROUP by profitability
Order by profitability





https://howrare.is/vbagame
https://howrare.is/junglecats
https://howrare.is/meerkatmillionaires
https://howrare.is/nakedmeerkats
https://howrare.is/chroniclesnft
https://howrare.is/hellopantha
https://howrare.is/soldierxsolvivorsolvivor
https://howrare.is/gangstagators
https://howrare.is/foxyfennecsgang
https://howrare.is/dangervalleyducks

https://twitter.com/cheesybobas
https://www.pixiv.net/en/users/36919471


WITH base AS (
  SELECT f.asset
  , f.asset_e8
  , f.pool_deduct
  , f.block_timestamp
  , f.tx
  , p.cnt
  , ROW_NUMBER() OVER (PARTITION BY f.asset, f.asset_e8, f.pool_deduct, f.block_timestamp, f.tx ORDER BY p.cnt) AS rn
  FROM bronze_midgard_2_6_9_20220405.midgard_fee_events f
  JOIN bronze_midgard_2_6_9_20220405.fee_events_pk_count p
  ON p.asset = f.asset 
    AND p.asset_e8 = f.asset_e8
    AND p.pool_deduct = f.pool_deduct
    AND p.block_timestamp = f.block_timestamp
    AND p.tx = f.tx
)
SELECT asset
, asset_e8
, pool_deduct
, block_timestamp
, tx
FROM base
WHERE rn <= cnt

SELECT tx
, asset
, asset_e8
, pool_deduct
, block_timestamp
, ROW_NUMBER() OVER (ORDER BY block_timestamp, tx) AS event_id
FROM midgard.fee_events


curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | bash
ssh -i "Users/kellenblumberg/git/props/aws/props-aws.pem" ubuntu@ec2-34-204-49-162.compute-1.amazonaws.com
ssh -i "~/git/props/aws/props-aws.pem" ubuntu@34.204.49.162
pm2 start npm -- start
sudo reboot


API Key: IioFYwv2t7KEsWjbh8xsEKd8d
API Secret Key: 0650aF7Vo3gz2H7z1o10ws4ojXgjbl8UWk3N8kqVUtqvgIBv26
Bearer Token: AAAAAAAAAAAAAAAAAAAAAF%2BRdAEAAAAAQ2emwRpqU1B7Cj22LUxfJ%2Btrhck%3DpFpWm4G4Wd5dNLJjP2Ru5EuVVHEx6lroYfkiEAlPQZ0HQvqwsn

WITH base AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY project_name ORDER BY insert_date DESC) AS rn
  FROM crosschain.address_labels
  WHERE blockchain = 'solana'
  AND label_subtype = 'nf_token_contract'
)
SELECT *
FROM base WHERE rn = 1


With secondtable as (Select DISTINCT label,marketplace,block_timestamp
From (SELECT RANK() OVER (PARTITION BY label order by block_timestamp) 
      as RN,label,block_TIMESTAMP,marketplace //as date 
      from solana.core.fact_nft_sales join solana.core.dim_labels on solana.core.fact_nft_sales.mint = solana.core.dim_labels.address) as ST
Where ST.RN = 1)

SELECT block_timestamp::date AS date
, COUNT(1) AS n_sales
, AVG( CASE WHEN l.label IS NULL THEN 0 ELSE 1 END ) AS pct_labeled
, SUM( CASE WHEN l.label IS NULL THEN 0 ELSE sales_amount END ) / SUM(sales_amount) AS pct_labeled_volume
FROM solana.core.fact_nft_sales s
LEFT JOIN solana.dim_labels l on s.mint = l.address
WHERE marketplace in ('magic eden v1', 'magic eden v2') 
AND block_timestamp >= '2022-01-01' 
GROUP BY 1
ORDER BY 1


SELECT *
FROM bi_analytics.bronze.hevo_grades g
JOIN bi_analytics.bronze.hevo_submissions s ON s.id = g.submission_id
JOIN bi_analytics.bronze.hevo_users u ON u.id = s.created_by_id
LIMIT 10




WITH base AS (
  SELECT from_address AS address
  , date_trunc('month', block_timestamp) AS month
  , SUM(-rune_amount) AS net_rune_amount
  FROM thorchain.transfers
  GROUP BY 1, 2
  UNION
  SELECT to_address AS address
  , date_trunc('month', block_timestamp) AS month
  , SUM(rune_amount) AS net_rune_amount
  FROM thorchain.transfers
  GROUP BY 1, 2
), b2 AS (
  SELECT address
  , MIN(month) AS month
  , SUM(net_rune_amount) AS net_rune_amount
  FROM base
  GROUP BY 1
), l AS (
  SELECT
  from_address AS address
  , pool_name
  , sum(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) as net_stake_units
  FROM thorchain.liquidity_actions
  WHERE lp_action IN ('add_liquidity','remove_liquidity')
  AND address LIKE 'thor%'
  GROUP BY 1, 2
), l2 AS (
  SELECT DISTINCT address
  FROM l 
  WHERE net_stake_units > 0
), f AS (
  SELECT DISTINCT address
  FROM silver_crosschain.ntr
  WHERE address LIKE 'thor%'
), tot_pool AS (
  SELECT pool_name
  , SUM(net_stake_units) AS tot_stake_units
  FROM l
  WHERE net_stake_units > 0
  GROUP BY 1
), pct_pool AS (
  SELECT address
  , l.pool_name
  , net_stake_units
  , tot_stake_units
  , net_stake_units / tot_stake_units AS pct_pool
  FROM l
  JOIN tot_pool t ON t.pool_name = l.pool_name
  WHERE net_stake_units > 0
), p AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY pool_name ORDER BY block_timestamp DESC) AS rn
  FROM thorchain.pool_block_balances
  WHERE block_timestamp >= CURRENT_DATE - 3
), pool_val AS (
  SELECT pool_name
  , rune_amount_usd + asset_amount_usd AS tvl
  FROM p
  WHERE rn = 1
), user_val AS (
  SELECT pp.address
  , pct_pool * tvl AS lp_val
  FROM pct_pool pp
  JOIN pool_val v ON v.pool_name = pp.pool_name
)
SELECT 
-- date_trunc('year', month) AS month
CASE WHEN f.address IS NULL THEN 0 ELSE 1 END AS is_flipside
, COUNT(1) AS n
, AVG(CASE WHEN net_rune_amount > 0 OR l2.address IS NOT NULL THEN 1 ELSE 0 END) AS pct_hold
, AVG(CASE WHEN net_rune_amount > 0 AND l2.address IS NULL THEN 1 ELSE 0 END) AS pct_rune_only
, AVG(CASE WHEN net_rune_amount <= 0 AND l2.address IS NOT NULL THEN 1 ELSE 0 END) AS pct_lp_only
, AVG(CASE WHEN net_rune_amount > 0 AND l2.address IS NOT NULL THEN 1 ELSE 0 END) AS pct_lp_and_rune
, SUM( COALESCE(lp_val, 0)) AS lp_val
FROM b2
LEFT JOIN l2 ON l2.address = b2.address
LEFT JOIN f ON f.address = b2.address
LEFT JOIN user_val u ON u.address = b2.address
GROUP BY 1
SELECT v.collection, v.mint, v.token_id, v.deal_score_rank, v.rarity_rank, v.fair_market_price , m.image_url
FROM flipside_prod_db.crosschain.nft_fair_market_value v
LEFT JOIN solana.core.dim_nft_metadata m ON m.mint = v.mint
WHERE v.mint = '3hDW84qQdbh8TcCvJWwDQ1SXbqWmG6QxX84JCq7ZP4Dp'
ORDER BY CASE WHEN COALESCE(m.image_url, 'None') = 'None' THEN 1 ELSE 0 END
LIMIT 1


When I run it the first time, it doesn't work


SELECT v.collection, v.mint, v.token_id, v.deal_score_rank, v.rarity_rank, v.fair_market_price, '' AS image_url
FROM flipside_prod_db.crosschain.nft_fair_market_value v
WHERE v.mint = 'FWPW8ddEZYbpaQ8b6qiU5yMbFan3Myg4z55oEN5qTpsE'
LIMIT 1


SELECT
    block_timestamp,
    'Deposit' as action,
    'SOL Covered Call' as vault,
    '8vyTqVVPmJfqFexRcMBGDAHoSCyZ52RC5sRVhYzbfU4j' as vault_address,
    inner_instruction:index as index,
    tx_id,
    inner_instruction:instructions [0] :parsed:info:authority as user_wallet,
    inner_instruction:instructions [0] :parsed:info:amount / 1e9 as amount
FROM
    solana.core.fact_events
WHERE
    program_id = '1349iiGjWC7ZTbu6otFmJwztms122jEEnShKgpVnNewy'
    and inner_instruction:instructions [0] :parsed:info:destination = '8vyTqVVPmJfqFexRcMBGDAHoSCyZ52RC5sRVhYzbfU4j'
    and inner_instruction:instructions [0] :parsed:type = 'transfer'
    and SUCCEEDED = 'TRUE'
UNION
SELECT
    e.block_timestamp,
    'Withdraw' as action,
    'SOL Covered Call' as vault,
    '8vyTqVVPmJfqFexRcMBGDAHoSCyZ52RC5sRVhYzbfU4j' as vault_address,
    inner_instruction:index as index,
    e.tx_id,
    case when array_size(instruction:accounts) > 12 then 
        instruction:accounts [1]
        when array_size(instruction:accounts) < 10 then 
        instruction:accounts [6]
        else
        instruction:accounts [9]
    end as user_wallet,
    case when inner_instruction:instructions [1] :parsed:type = 'transfer' then
        inner_instruction:instructions [1] :parsed:info:amount / 1e9
    else
        inner_instruction:instructions [0] :parsed:info:amount / 1e9
    end as amount
FROM
    solana.core.fact_events e
WHERE
    program_id = '1349iiGjWC7ZTbu6otFmJwztms122jEEnShKgpVnNewy'
    AND (
        (
            inner_instruction:instructions [1] :parsed:info:source = '8vyTqVVPmJfqFexRcMBGDAHoSCyZ52RC5sRVhYzbfU4j'
            AND inner_instruction:instructions [1] :parsed:type = 'transfer'
        )
        OR (
            inner_instruction:instructions [0] :parsed:info:source = '8vyTqVVPmJfqFexRcMBGDAHoSCyZ52RC5sRVhYzbfU4j'
            AND inner_instruction:instructions [0] :parsed:type = 'transfer'
        )
    ) and SUCCEEDED = 'TRUE'
  and user_wallet = 'BEmgLNjcUJx8ibdoryYxJDZVEovADSZnWCccs1HLgt37'
UNION
SELECT
    block_timestamp,
    'Initiate Withdraw' as action,
    'SOL Covered Call' as vault,
    'DRLcUXwMcFf8itWJy7NdzKuWrZep1HceLaTgRRDs51SH' as vault_address,
    inner_instruction:index as index,
    tx_id,
    inner_instruction:instructions [0] :parsed:info:authority as user_wallet,
    inner_instruction:instructions [0] :parsed:info:amount / 1e9 as amount
FROM
    solana.core.fact_events
WHERE
    program_id = '1349iiGjWC7ZTbu6otFmJwztms122jEEnShKgpVnNewy'
    and inner_instruction:instructions [0] :parsed:info:destination = 'DRLcUXwMcFf8itWJy7NdzKuWrZep1HceLaTgRRDs51SH'
    and inner_instruction:instructions [0] :parsed:type = 'transfer'
  and SUCCEEDED = 'TRUE'
  and user_wallet = 'BEmgLNjcUJx8ibdoryYxJDZVEovADSZnWCccs1HLgt37'




Education on the site
Andy8052
BetDex taking on risk

SELECT *
FROM silver_crosschain.address_labels
WHERE address = '6z8T7je8BZfCbUaEpX3uNVGB1rsjXdQSRTashfArJjDP'

SELECT *
FROM solana.dim_labels
WHERE mint = '6z8T7je8BZfCbUaEpX3uNVGB1rsjXdQSRTashfArJjDP'

SELECT m.mint
, m.block_timestamp
, m.tx_id
, m.mint_price
FROM solana.fact_nft_mints m
WHERE tx_id = '4VzwY68wQ7shxyxFtxexWufe76ojZaPEpT1EYEKBQvZroVdEkSdx58m3sBpYAaJpFV7neCxRUoHCUx9XQTmwXCDv'


SELECT l.address
, m.block_timestamp
, m.tx_id
, m.mint_price
FROM flipside_prod_db.solana.dim_labels l
JOIN solana.fact_nft_mints m ON m.mint = l.address
WHERE label_type = 'nft'
AND label = 'bulldog doghouses'


SELECT label, address, COUNT(1) AS n
FROM solana.dim_labels
GROUP BY 1, 2
HAVING COUNT(1) > 1
ORDER BY 3 DESC


SELECT cp.project_name
, cp.title
, cp.slug
, b.title AS bounty_title
, b.difficulty
, u.discord_handle
, g.created_at AS grade_time
, grade_time::date AS grade_date
, total_score
, result_url
FROM bi_analytics.bronze.hevo_grades g
JOIN bi_analytics.bronze.hevo_submissions s ON s.id = g.submission_id
JOIN bi_analytics.bronze.hevo_users u ON u.id = s.created_by_id
JOIN bi_analytics.bronze.hevo_claims c ON c.id = s.claim_id
JOIN bi_analytics.bronze.hevo_bounties b ON b.id = c.bounty_id
JOIN bi_analytics.bronze.hevo_campaigns cp ON cp.id = b.campaign_id
WHERE total_score IS NOT NULL
AND cp.project_name ilike '%thor%'
ORDER BY grade_time DESC
LIMIT 1000


WITH base AS (
  SELECT label
  , MIN(m.block_timestamp::date) AS mn_date
  , MAX(m.block_timestamp::date) AS mx_date
  , MIN(m.mint_price) AS mn_price
  , MAX(m.mint_price) AS mx_price
  , COUNT(1) AS n_mints
  FROM solana.core.fact_nft_mints m
  JOIN solana.dim_labels l on m.mint = l.address
  AND block_timestamp >= '2022-01-01' 
  GROUP BY 1
  ORDER BY 1
), filter AS (
  SELECT DISTINCT label
  FROM solana.core.fact_nft_sales s
  LEFT JOIN solana.dim_labels l on s.mint = l.address
  WHERE marketplace in ('magic eden v1', 'magic eden v2') 
  AND block_timestamp >= '2022-01-01' 
)
SELECT b.*
, datediff('days', mn_date, mx_date) AS mint_days
FROM base b
JOIN filter f ON f.label = b.label
ORDER BY mint_days DESC

WITH t AS (
  SELECT DISTINCT tx_id
  FROM thorchain.swaps
  WHERE block_timestamp >= CURRENT_DATE - 1
  LIMIT 10
)
SELECT *
FROM thorchain.swaps s
JOIN t ON t.tx_id = s.tx_id
ORDER BY tx_id



WITH t AS (
  SELECT tx_id, COUNT(1) AS n
  FROM thorchain.swaps
  WHERE block_timestamp >= CURRENT_DATE - 1
  GROUP BY 1
)
SELECT s.*, CASE WHEN t.n = 1 THEN 0 ELSE 1 END AS two_sided_swap_via_rune
FROM thorchain.swaps s
JOIN t ON t.tx_id = s.tx_id
WHERE block_timestamp >= CURRENT_DATE - 1


https://docs.google.com/presentation/d/1u1f41MsaEpTdCtBnR-lrX6yoIJeh2gOZDK6_54E70y0/edit#slide=id.g132b60abcc1_0_152


SELECT ARRAY_SIZE(inner_instructions[0]:instructions) AS sz
, COUNT(1) AS n
FROM solana.fact_transactions 
WHERE block_timestamp >= CURRENT_DATE - 30
AND instructions[0]:programId = 'GrcZwT9hSByY1QrUTaRPp6zs5KxAA5QYuqEhjT1wihbm'
GROUP BY 1

With times as
(
  Select solana.core.fact_nft_mints.BLOCK_TIMESTAMP, Solana.core.dim_labels.LABEL
  From Solana.core.fact_nft_mints
  INNER JOIN solana.core.dim_labels on solana.core.fact_nft_mints.MINT = solana.core.dim_labels.ADDRESS
),
//this is for the combination of mint and labels tabel where it connects the adress and gives the timestamp and english label
second_table as (Select DISTINCT label,block_timestamp
From (SELECT RANK() OVER (PARTITION BY label order by block_timestamp)
      as RN,label,block_TIMESTAMP //as date
      from times) as ST
Where ST.RN = 1),
//this is creating the min times for each collection
min_mint_table as(
Select label,block_timestamp as date
From second_table
), first_sale AS (
  SELECT l.label, MIN(block_timestamp) AS first_sale_date
  FROM solana.fact_nft_sales s
  INNER JOIN solana.dim_labels l on l.address = s.mint
),
//just renaming blocktimestamp as date to seprate in combined table
combined_mint_table as(
Select t.block_timestamp
,t.label
, m.date
, f.first_sale_date
,TIMESTAMPDIFF('day', t.block_timestamp, first_sale_date) AS sale_difference
,TIMESTAMPDIFF('day', m.date, t.block_timestamp) AS difference
from times t
JOIN first_sale f ON f.label = t.label
JOIN min_mint_table m ON m.label = t.label
), n_mints AS (
  SELECT label, COUNT(DISTINCT address) AS n
  FROM solana.core.dim_labels
  GROUP BY 1
)
Select t.label, n_mints, MAX(difference) AS mx, AVG(difference) as avg
From combined_mint_table t
JOIN n_mints n ON n.label = t.label
GROUP BY 1, 2
ORDER BY 3 DESC

WITH base AS (
  SELECT t.from AS address
  , hash
  , RIGHT(CONCAT(REPEAT('0', 64), substr(CAST(l.data AS VARCHAR),3,64)), 64) as n
  , l.data
  , 4294967296::decimal as exp0 -- 16^8
  , 72057594037927936::decimal as exp1 -- 16^14
  , t.block_time
  FROM optimism.transactions t
  INNER JOIN optimism.logs l
      ON t.hash = l.tx_hash
  AND t.to = '\xfedfaf1a10335448b7fa0268f56d2b44dbd357de'
  AND l.topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND t.block_time > '2022-05-25'
  AND t.block_time <= '2022-06-03 22:57'
  AND success
), b2 AS (
  SELECT address
  , MIN(block_time)
  , SUM(
      CONCAT('x00',CAST((substring(n, 1, 14)) AS VARCHAR))::bit(64)::bigint * exp0 * exp1 * exp1 * exp1
      + CONCAT('x00',CAST((substring(n, 15, 14)) AS VARCHAR))::bit(64)::bigint * exp0 * exp1 * exp1
      + CONCAT('x00',CAST((substring(n, 29, 14)) AS VARCHAR))::bit(64)::bigint * exp0 * exp1
      + CONCAT('x00',CAST((substring(n, 43, 14)) AS VARCHAR))::bit(64)::bigint * exp0
      + CONCAT('x00',CAST((substring(n, 57, 14)) AS VARCHAR))::bit(64)::bigint
  ) / POWER(10,18) AS claim_amount 
  FROM base
  GROUP BY 1, 2
), swaps AS (
  SELECT b2.address
  , SUM(
    CASE WHEN d.trader_a = b2.address
  )
  FROM b2
  JOIN dex.trades d ON d.block_time >= b2.block_time AND (
    (d.trader_a = b2.address AND token_a_address = '\x4200000000000000000000000000000000000042')
    OR (d.trader_b = b2.address AND token_b_address = '\x4200000000000000000000000000000000000042')
  )
  AND (
    token_a_address = '\x4200000000000000000000000000000000000042'
    OR token_b_address = '\x4200000000000000000000000000000000000042'
  )
  AND usd_amount > 0 
  AND token_a_amount_raw>0
  AND token_b_amount_raw>0
)
SELECT SUM(claim_amount) AS claim_amount, MAX(block_time) AS mx
 FROM b2




WITH base AS (
  SELECT from_address AS address
  , date_trunc('month', block_timestamp) AS month
  , SUM(-rune_amount) AS net_rune_amount
  FROM thorchain.transfers
  GROUP BY 1, 2
  UNION
  SELECT to_address AS address
  , date_trunc('month', block_timestamp) AS month
  , SUM(rune_amount) AS net_rune_amount
  FROM thorchain.transfers
  GROUP BY 1, 2
), b2 AS (
  SELECT address
  , MIN(month) AS month
  , SUM(net_rune_amount) AS net_rune_amount
  FROM base
  GROUP BY 1
), l AS (
  SELECT
  from_address AS address
  , pool_name
  , sum(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) as net_stake_units
  FROM thorchain.liquidity_actions
  WHERE lp_action IN ('add_liquidity','remove_liquidity')
  AND address LIKE 'thor%'
  GROUP BY 1, 2
), l2 AS (
  SELECT DISTINCT address
  FROM l 
  WHERE net_stake_units > 0
), f AS (
  SELECT DISTINCT address
  FROM silver_crosschain.ntr
  WHERE address LIKE 'thor%'
), tot_pool AS (
  SELECT pool_name
  , SUM(net_stake_units) AS tot_stake_units
  FROM l
  WHERE net_stake_units > 0
  GROUP BY 1
), pct_pool AS (
  SELECT address
  , l.pool_name
  , net_stake_units
  , tot_stake_units
  , net_stake_units / tot_stake_units AS pct_pool
  FROM l
  JOIN tot_pool t ON t.pool_name = l.pool_name
  WHERE net_stake_units > 0
), p AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY pool_name ORDER BY block_timestamp DESC) AS rn
  FROM thorchain.pool_block_balances
  WHERE block_timestamp >= CURRENT_DATE - 3
), pool_val AS (
  SELECT pool_name
  , rune_amount_usd + asset_amount_usd AS tvl
  FROM p
  WHERE rn = 1
), user_val AS (
  SELECT pp.address
  , pct_pool * tvl AS lp_val
  FROM pct_pool pp
  JOIN pool_val v ON v.pool_name = pp.pool_name
)
SELECT 
-- date_trunc('year', month) AS month
CASE WHEN f.address IS NULL THEN 0 ELSE 1 END AS is_flipside
, COUNT(1) AS n
, AVG(CASE WHEN net_rune_amount > 0 OR l2.address IS NOT NULL THEN 1 ELSE 0 END) AS pct_hold
, AVG(CASE WHEN net_rune_amount > 0 AND l2.address IS NULL THEN 1 ELSE 0 END) AS pct_rune_only
, AVG(CASE WHEN net_rune_amount <= 0 AND l2.address IS NOT NULL THEN 1 ELSE 0 END) AS pct_lp_only
, AVG(CASE WHEN net_rune_amount > 0 AND l2.address IS NOT NULL THEN 1 ELSE 0 END) AS pct_lp_and_rune
, SUM( COALESCE(lp_val, 0)) AS lp_val
FROM b2
LEFT JOIN l2 ON l2.address = b2.address
LEFT JOIN f ON f.address = b2.address
LEFT JOIN user_val u ON u.address = b2.address
GROUP BY 1



WITH op_claims AS (
SELECT `from`
, block_time
, hash
, (
CAST(conv(substring(n, 1, 14), 16, 10) AS STRING) * exp0 * exp1 * exp1 * exp1
+ CAST(conv(substring(n, 15, 14), 16, 10) AS STRING) * exp0 * exp1 * exp1
+ CAST(conv(substring(n, 29, 14), 16, 10) AS STRING) * exp0 * exp1
+ CAST(conv(substring(n, 43, 14), 16, 10) AS STRING) * exp0
+ CAST(conv(substring(n, 57, 14), 16, 10) AS STRING)
)/POWER(10,18) AS claim_amount
FROM (
    SELECT `from`
    , hash
    , lpad(substr(l.data,3,64), 64, '0') as n
    , '4294967296' as exp0 -- 16^8
    , '72057594037927936' as exp1 -- 16^14
    , t.block_time
    FROM optimism.transactions t
    INNER JOIN optimism.logs l
        ON t.hash = l.tx_hash
    WHERE `to` = '0xfedfaf1a10335448b7fa0268f56d2b44dbd357de'
    AND topic1 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND t.block_time > '2022-05-25'
    AND success
    ) a
  LIMIT 1000
)
SELECT c.*
, CASE WHEN d.trader_a = `from` THEN 1 ELSE 0 END AS is_trader_a
FROM op_claims
LEFT JOIN dex.trades d ON d.block_time >= c.block_time AND (d.trader_a = `from` OR d.trader_b = `from`)
AND (
  token_a_address = '\x4200000000000000000000000000000000000042'
  OR token_b_address = '\x4200000000000000000000000000000000000042'
)
AND usd_amount > 0 
AND token_a_amount_raw>0
AND token_b_amount_raw>0




SELECT *
FROM thorchain.prices
WHERE pool_name like '%.ETH%'
AND block_timestamp >= '2021-11-11'
ORDER BY block_timestamp, pool_name
LIMIT 10000


SELECT *
FROM solana.fact_transactions 
WHERE block_timestamp >= CURRENT_DATE - 2
AND tx_id = 'URjxfD1SFfYwySGRVaD9iC3PX4b8mheC4LYuik9fY9uQV2kfyfr7gXkGHeQY4RHSaRwn7mN9mPQ5J8Y8mE5xEtt'
AND instructions[0]:programId = 'GrcZwT9hSByY1QrUTaRPp6zs5KxAA5QYuqEhjT1wihbm'


SELECT MAX(block_timestamp)
FROM solana.fact_transactions 


Zac: CeVXxgDm6MR5biBzRqLW7U9uv47nfrZMYJ5BtTbGLPY3


thinking about the JJ idea a little more... what do you think about this... I have someone that I work closely with that is also deeply ingrained in the hip crypto news that fills this role with me. They are also data savvy but more importantly are very up to date with crypto.
The person that comes most to mind is Nhat (not sure what his bandwidth looks like), but Jessica could also fit the bill.

SELECT *
FROM THORCHAIN.LIQUIDITY_ACTIONS
LIMIT 100

SELECT


SELECT *
FROM thorchain.prices
WHERE 1=1
AND block_timestamp >= '2021-11-18 18:00:00'
AND block_timestamp <= '2021-11-18 20:00:00'
GROUP BY 1


WITH mints AS (
  SELECT DISTINCT LOWER(project_name) AS collection
  , address AS mint
  FROM crosschain.address_labels
  WHERE blockchain = 'solana'
  AND label_subtype = 'nf_token_contract'
  UNION 
  SELECT DISTINCT LOWER(project_name) AS collection
  , mint
  FROM solana.dim_nft_metadata
)
SELECT collection
, SUM(sales_amount) AS sales_amount
, MIN(block_timestamp::date) AS start_date
, MIN(CASE WHEN block_timestamp >= CURRENT_DATE - 3 THEN sales_amount ELSE NULL END) AS mm
, MAX(CASE WHEN block_timestamp >= CURRENT_DATE - 3 THEN sales_amount ELSE NULL END) AS mx
, COUNT(1) AS n_sales
FROM solana.fact_nft_sales s
JOIN mints m ON m.mint = s.mint
GROUP BY 1
ORDER BY 2 DESC
LIMIT 200


SELECT instructions[0]:programId AS programId, LEFT(instructions[0]:data::string, 4) AS data, COUNT(1) AS n
FROM solana.fact_transactions t
WHERE t.block_timestamp >= '2022-01-01'
AND t.block_timestamp <= '2022-01-02'
-- AND LEFT(instructions[0]:data::string, 4) IN ('ENwH','3GyW')
AND instructions[0]:programId like 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8'
AND 
GROUP BY 1, 2



SELECT instructions[0]:programId AS programId, LEFT(instructions[0]:data::string, 4) AS data, *
FROM solana.fact_transactions t
WHERE t.block_timestamp >= '2022-01-01'
AND t.block_timestamp <= '2022-01-02'
-- AND LEFT(instructions[0]:data::string, 4) IN ('ENwH','3GyW')
AND instructions[0]:programId like 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8'


SELECT *
FROM solana.fact_transactions t
WHERE t.block_timestamp >= CURRENT_DATE - 2
AND tx_id = '3xpAMpkcuhKb6grqfmFTcquAKBuFHnRAd2C3goSvVyabFX28GeoJyARXnMjPgYjPDMqf6HzYTH1etF4W9BsbsSSk'
-- AND LEFT(instructions[0]:data::string, 4) IN ('ENwH','3GyW')
AND instructions[0]:programId like 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
AND inner_instructions[0]:instructions[2]:parsed:info:destination::string = 'AoNVE2rKCE2YNA44V7NQt8N73JdPM7b6acZ2vzSpyPyi'



SELECT SPLIT(pool_name, '-')[0]::string AS pool_name
, COUNT(1) AS n_swaps
, AVG(from_amount_usd) AS avg_size
, SUM( CASE WHEN from_address = 'THOR.RUNE' THEN from_amount ELSE -to_amount END) AS net_rune
, SUM( CASE WHEN from_address = 'THOR.RUNE' THEN from_amount_usd ELSE -from_amount_usd END) AS net_rune_usd
FROM thorchain.swaps
WHERE 1=1
AND block_timestamp >= '2021-11-18 18:00:00'
AND block_timestamp <= '2021-11-18 20:00:00'
GROUP BY 1


SELECT MIN(block_timestamp)
FROM thorchain.swaps 
WHERE pool_name like 'ETH.ETH%'
AND block_timestamp: >= '2021-09-17'
ORDER BY block_timestamp
LIMIT 1000



WITH base AS (
  SELECT DISTINCT SPLIT(pool_name, '-')[0]::string AS pool_name
  , block_timestamp::date AS date
  , SUM(from_amount_usd) AS amt
  FROM thorchain.swaps
  GROUP BY 1, 2
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY pool_name ORDER BY date) AS rn
  FROM base
)
SELECT b.*, COALESCE(c.date, CURRENT_DATE) AS n_date, DATEDIFF('days', b.date, n_date) AS lag, c.amt AS n_amt
FROM b2 b
LEFT JOIN b2 c ON c.pool_name = b.pool_name AND c.rn = b.rn + 1



SELECT *
, ROW_NUMBER() OVER (PARTITION BY pool_name ORDER BY block_timestamp)
FROM thorchain.swaps

SELECT *
FROM thorchain.prices
WHERE pool_name like 'ETH.ETH%'
AND block_timestamp >= '2021-10-21'
//AND block_timestamp <= '2021-10-10'
//AND ROUND(asset_usd) <> 8007
ORDER BY block_timestamp
LIMIT 1000



WITH tc AS (
  SELECT height AS tc_block_id
  , TO_TIMESTAMP(CAST(timestamp * POWER(10, -9) AS INT)) AS tc_block_timestamp
  FROM thorchain_midgard_public.block_log
), base AS (
  SELECT t.block_id AS terra_block_id
  , t.block_timestamp AS terra_block_timestamp
  , tc.*
  , ROW_NUMBER() OVER (PARTITION BY t.block_id ORDER BY tc.tc_block_timestamp DESC) AS rn
  FROM terra.blocks t
  JOIN tc ON tc.tc_block_timestamp <= t.block_timestamp
  AND tc.tc_block_timestamp >= DATEADD('hours', -1, t.block_timestamp)
  WHERE t.block_id = 7544910
)
SELECT *
FROM base


WITH tc AS (
  -- get each THORChain block
  SELECT height AS tc_block_id
  , TO_TIMESTAMP(CAST(timestamp * POWER(10, -9) AS INT)) AS tc_block_timestamp
  FROM thorchain_midgard_public.block_log
), base AS (
  SELECT t.block_id AS terra_block_id
  , t.block_timestamp AS terra_block_timestamp
  , tc.*
  -- get the earliest THORChain block in the "Post-attack" timeframe
  , ROW_NUMBER() OVER (PARTITION BY t.block_id ORDER BY tc.tc_block_timestamp ASC) AS rn
  FROM terra.blocks t
  JOIN tc ON tc.tc_block_timestamp >= t.block_timestamp
  AND tc.tc_block_timestamp <= DATEADD('hours', 1, t.block_timestamp)
  WHERE t.block_id = 7790000
)
SELECT *
FROM base
WHERE rn = 1



SELECT *
FROM terra.blocks t
WHERE t.block_id = 7790000
-- WHERE t.block_id = 7544910

SELECT rune_address
, asset_address
, rune_e8 * POWER(10, -8) AS rune_amt
, asset_e8 * POWER(10, -8) AS asset_amt
, stake_units
FROM thorchain.stake_events
WHERE block_id <= 5461281
AND pool_name like 'TERRA%'
UNION ALL
SELECT rune_address
, asset_address
, -rune_e8 * POWER(10, -8) AS rune_amt
, -asset_e8 * POWER(10, -8) AS asset_amt
, -stake_units AS stake_units
FROM thorchain.unstake_events
WHERE block_id <= 5461281
AND pool_name like 'TERRA%'



SELECT *
, height AS block_id
, TO_TIMESTAMP(timestamp) AS block_timestamp
FROM thorchain_midgard_public.block_log
LIMIT 100

WITH mints AS (
    SELECT DISTINCT LOWER(project_name) AS collection
    , mint
    , token_id
    FROM solana.dim_nft_metadata
    WHERE token_id IS NOT NULL
), base AS (
  SELECT m.collection
  , m.token_id
  , s.tx_id
  , s.block_timestamp
  , s.mint
  , s.sales_amount AS price
  FROM solana.fact_nft_sales s
  JOIN mints m ON LOWER(m.mint) = LOWER(s.mint)
  WHERE succeeded = TRUE
)
SELECT *
FROM base


PATH=/Library/Frameworks/Python.framework/Versions/3.10/bin

/Users/kellenblumberg/.nvm/versions/node/v16.10.0/bin
/Users/kellenblumberg/.cargo/bin
/Users/kellenblumberg/.local/share/solana/install/active_release/bin
/Users/kellenblumberg/Documents/google-cloud-sdk/bin
/Users/kellenblumberg/opt/anaconda3/bin
/Users/kellenblumberg/opt/anaconda3/condabin
/usr/local/bin
/usr/bin
/bin
/usr/sbin
/sbin
/usr/local/share/dotnet
~/.dotnet/tools
/Library/Apple/usr/bin
/Library/Frameworks/Mono.framework/Versions/Current/Commands



SELECT DISTINCT inner_instructions[0]:instructions[0]:parsed:info:authority::string AS address
FROM solana.fact_transactions 
WHERE block_timestamp >= CURRENT_DATE - 7
AND instructions[0]:programId = 'GrcZwT9hSByY1QrUTaRPp6zs5KxAA5QYuqEhjT1wihbm'
AND ARRAY_SIZE(inner_instructions[0]:instructions) = 3

https://discordapp.com/api/oauth2/authorize?scope=bot&client_id=

PROPS_KEYPAIR=[79, 195, 45, 204, 181, 174, 129, 152, 73, 156, 16, 108, 98, 36, 250, 122, 218, 77, 24, 101, 32, 82, 39, 15, 58, 55, 163, 229, 102, 19, 190, 55 ]

% own RUNE before first bounty
% own RUNE currently



WITH base AS (
  SELECT from_address AS address
  , SUM(-rune_amount) AS net_rune
  FROM thorchain.transfers
  GROUP BY 1
  UNION ALL
  SELECT to_address AS address
  , SUM(rune_amount) AS net_rune
  FROM thorchain.transfers
  GROUP BY 1
)
SELECT address
, SUM(net_rune) AS net_rune
FROM base
GROUP BY 1


SELECT from_address AS address
, SPLIT(pool_name, '-')[0]::string AS pool_name
, SUM(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) AS net_stake_units
FROM thorchain.liquidity_actions
GROUP BY 1, 2


SELECT block_timestamp::date AS date
, COUNT(1) AS n
, SUM(RUNE_AMOUNT) AS RUNE_AMOUNT
FROM thorchain.liquidity_actions
WHERE lp_action = 'remove_liquidity'
AND pool_name LIKE 'TERRA%'
GROUP BY 1



ssh -i "~/git/props/aws/props-aws.pem" ubuntu@34.204.49.162

with add as (
  SELECT
  from_address as address
  , pool_name
  , block_timestamp::date AS date
  , sum(stake_units) as net_units
  , sum(rune_amount) as net_rune_amount
  , sum(asset_amount) as net_asset_amount
  FROM thorchain.liquidity_actions
  WHERE lp_action = 'add_liquidity'
  AND pool_name like '%TERRA%'
  AND address like 'thor%'
  GROUP BY 1, 2, 3
), remove AS (
  SELECT
  from_address as address
  , pool_name
  , block_timestamp::date AS date
  , sum(-stake_units) as net_units
  , sum(-rune_amount) as net_rune_amount
  , sum(-asset_amount) as net_asset_amount
  FROM thorchain.liquidity_actions
  WHERE lp_action = 'remove_liquidity'
  AND pool_name like '%TERRA%'
  AND address like 'thor%'
  GROUP BY 1, 2, 3
), b1 AS (
  SELECT *
  FROM add
  UNION ALL
  SELECT *
  FROM remove
), b2 AS (
  SELECT address
  , pool_name
  , date
  , SUM(net_units) AS net_units
  , SUM(net_rune_amount) AS net_rune_amount
  , SUM(net_asset_amount) AS net_asset_amount
  FROM b1
  GROUP BY 1, 2, 3
), b3 AS (
  SELECT *
  , SUM(net_units) OVER (PARTITION BY address, pool_name ORDER BY date) AS cumu_net_units
  , SUM(net_rune_amount) OVER (PARTITION BY address, pool_name ORDER BY date) AS cumu_net_rune_amount
  , SUM(net_asset_amount) OVER (PARTITION BY address, pool_name ORDER BY date) AS cumu_net_asset_amount
  FROM b2
), b4 AS (
  SELECT *
  , CASE WHEN cumu_net_units > 0 AND cumu_net_units = net_units THEN 1
    WHEN cumu_net_units <= 0 AND net_units < 0 THEN -1
    ELSE 0 END
    AS chg_in_lpers
  FROM b3
), b5 AS (
  SELECT pool_name
  , date
  , SUM(chg_in_lpers) AS net_lpers
  FROM b4
  GROUP BY 1, 2
)
SELECT *
, SUM(net_lpers) OVER (PARTITION BY pool_name ORDER BY date) AS cumu_lpers
FROM b5


SELECT
COALESCE(address, '') AS address
, pool_name
, sum(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) as net_stake_units
, sum(CASE WHEN lp_action = 'add_liquidity' THEN rune_amount ELSE -rune_amount END) as net_rune_amount
, sum(CASE WHEN lp_action = 'add_liquidity' THEN asset_amount ELSE -asset_amount END) as net_asset_amount
, sum(asset_amount) as net_asset_amount
FROM thorchain.liquidity_actions
WHERE lp_action IN ('add_liquidity','remove_liquidity')
AND pool_name like '%TERRA%'
GROUP BY 1, 2


  GROUP BY 1, 2, 3
), remove AS (
  SELECT
  from_address as address
  , pool_name
  , block_timestamp::date AS date
  , sum(-stake_units) as net_units
  , sum(-rune_amount) as net_rune_amount
  , sum(-asset_amount) as net_asset_amount
  FROM thorchain.liquidity_actions
  WHERE lp_action = 'remove_liquidity'
  AND pool_name like '%TERRA%'
  AND address like 'thor%'
  GROUP BY 1, 2, 3
)



SELECT MAX(block_timestamp)
FROM thorchain.liquidity_actions
WHERE lp_action = 'remove_liquidity'
AND pool_name like '%TERRA%'
AND address like 'thor%'


  combo as (select
  a.address as addresses,
  a.poolname as pools,
  a.units as add_units,
  b.units as remove_units,
  a.units - ifnull(b.units, 0) as net_add
  from add a 
  left join remove b
  on (a.address = b.address and a.poolname = b.poolname)
  where net_add > 0
  group by 1,2,3,4),
  
earliest_still_add as (select from_address,
min(date_trunc('month',block_timestamp)) as earliest_month
from thorchain.liquidity_actions
where from_address in (select addresses from combo)
and lp_action = 'add_liquidity'
group by 1),

  -- total_added
add_liq as (select
  from_address as address,
  tx_id,
  block_timestamp,
  pool_name as poolname,
  sum(stake_units) as units
from thorchain.liquidity_actions
  where lp_action = 'add_liquidity'
  and from_address like 'thor%'
  group by 1,2,3,4),

earliest_total_add as (select address,
min(date_trunc('month', block_timestamp)) as earliest_month
from add_liq
group by address)

select count(distinct(from_address)) as wallets_still_providing_liquidity, 'wallets_still_providing_liquidity'
from earliest_still_add
union 
select count(distinct(address)) - count(distinct(from_address)) as total_wallets_removed_liquidity, 'total_wallets_removed_liquidity'
from earliest_total_add, earliest_still_add

SELECT record_content[0]:collection AS collection
, COUNT(1) AS n
FROM flipside_prod_db.bronze.prod_data_science_uploads_1748940988
GROUP BY 1
ORDER BY 2 DESC

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
  JOIN solana.dim_nft_metadata m ON m.token_id = t.value:token_id AND m.project_name = t.value:collection
  WHERE m.mint = '${mint}'
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
), b7 AS (
  SELECT collection
  , mint
  , token_id
  , deal_score_rank
  , rarity_rank
  , new_floor AS floor_price
  , ROUND(CASE WHEN new_fair_market_price < floor_price THEN floor_price ELSE new_fair_market_price END, 2) AS fair_market_price
  , ROUND(CASE WHEN new_fair_market_price - cur_sd < floor_price * 0.975 THEN floor_price * 0.975 ELSE new_fair_market_price - cur_sd END, 2) AS price_low
  , ROUND(CASE WHEN new_fair_market_price + cur_sd < floor_price * 1.025 THEN floor_price * 1.025 ELSE new_fair_market_price + cur_sd END, 2) AS price_high
  FROM base6
)


WITH base AS (
  SELECT * 
  FROM flipside_prod_db.bronze.prod_data_science_uploads_1748940988
  WHERE record_content[0]:collection IS NOT NULL
  AND record_metadata:key like '%nft-deal-score-rankings-%'
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
), metadata AS (
  SELECT project_name AS collection
  , token_id
  , mint
  , ROW_NUMBER() OVER (PARTITION BY collection, token_id ORDER BY created_at_timestamp DESC) AS rn
  FROM solana.dim_nft_metadata
  WHERE mint IS NOT NULL
), base5 AS (
  SELECT b2.*
  , m.mint
  , b4.new_floor
  FROM base2 b2
  JOIN base4 b4 ON b2.collection = b4.collection AND b4.rn = 1
  LEFT JOIN metadata m ON m.token_id = b2.token_id AND m.collection = b2.collection AND m.rn = 1
  WHERE b2.rn = 1
), base6 AS (
  SELECT *
  , old_sd * new_floor / old_floor AS cur_sd
  , old_fair_market_price + ((new_floor - old_floor) * lin_coef) + (( new_floor - old_floor) * log_coef * old_fair_market_price / old_floor) AS new_fair_market_price
  FROM base5
), b7 AS (
  SELECT mint
  , COUNT(1) AS n
  FROM base6
  GROUP BY 1
)
SELECT collection
, b.mint
, n
, token_id
, deal_score_rank
, rarity_rank
, new_floor AS floor_price
, ROUND(CASE WHEN new_fair_market_price < floor_price THEN floor_price ELSE new_fair_market_price END, 2) AS fair_market_price
, ROUND(CASE WHEN new_fair_market_price - cur_sd < floor_price * 0.975 THEN floor_price * 0.975 ELSE new_fair_market_price - cur_sd END, 2) AS price_low
, ROUND(CASE WHEN new_fair_market_price + cur_sd < floor_price * 1.025 THEN floor_price * 1.025 ELSE new_fair_market_price + cur_sd END, 2) AS price_high
FROM base6 b
JOIN b7 ON b7.mint = b.mint



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


SELECT max(block_timestamp)
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


SELECT COUNT(1), MAX(block_timestamp)
FROM bronze_midgard_2_6_9_20220405.midgard_fee_events 

SELECT COUNT(1), MAX(block_timestamp)
FROM midgard.bond_events 
WHERE block_timestamp <= 1655955911000000000
WHERE block_timestamp <= 1655986950000000000

SELECT COUNT(1), MAX(block_timestamp)
FROM thorchain.bond_events



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




WITH a AS (
  SELECT DISTINCT tx_id
  FROM ethereum.events_emitted
  WHERE block_timestamp >= CURRENT_DATE - 1
  AND contract_address = LOWER('0xa5f2211B9b8170F694421f2046281775E8468044')
), b AS (
  SELECT DISTINCT e.tx_id
  FROM ethereum.events_emitted e
  JOIN a ON a.tx_id = e.tx_id
  WHERE block_timestamp >= CURRENT_DATE - 1
  AND tx_to_address = LOWER('0x815c23eca83261b6ec689b60cc4a58b54bc24d8d')
  AND event_name = 'Withdraw'
), c AS (
  SELECT DISTINCT e.tx_id
  FROM ethereum.events_emitted e
  JOIN b ON b.tx_id = e.tx_id
  WHERE block_timestamp >= CURRENT_DATE - 1
  AND tx_to_address = LOWER('0x815c23eca83261b6ec689b60cc4a58b54bc24d8d')
  AND event_name = 'DelegateVotesChanged'
)
SELECT block_timestamp::date AS date
, SUM(CASE WHEN e.event_name = 'Transfer' 
  AND contract_address = '0xa5f2211b9b8170f694421f2046281775e8468044' 
  AND LENGTH(event_inputs:value::string) <= 25
  THEN CAST(event_inputs:value::string AS INT) * POWER(10, -18)
  ELSE 0 END) AS thor_amt
FROM ETHEREUM.events_emitted e
JOIN c ON c.tx_id = e.tx_id
GROUP BY 1




WITH a AS (
  SELECT DISTINCT tx_id
  FROM ethereum.events_emitted
  WHERE block_timestamp >= CURRENT_DATE - 18
  AND contract_address = LOWER('0xa5f2211B9b8170F694421f2046281775E8468044')
  LIMIT 1000
), b AS (
  SELECT DISTINCT e.tx_id
  FROM ethereum.events_emitted e
  JOIN a ON a.tx_id = e.tx_id
  WHERE block_timestamp >= CURRENT_DATE - 18
  AND tx_to_address = LOWER('0x815c23eca83261b6ec689b60cc4a58b54bc24d8d')
  AND event_name = 'Deposit'
), c AS (
  SELECT DISTINCT e.tx_id
  FROM ethereum.events_emitted e
  JOIN b ON b.tx_id = e.tx_id
  WHERE block_timestamp >= CURRENT_DATE - 18
  AND tx_to_address = LOWER('0x815c23eca83261b6ec689b60cc4a58b54bc24d8d')
  AND event_name = 'DelegateVotesChanged'
)
SELECT block_timestamp::date AS date
, event_inputs:value AS thor_amt
FROM ETHEREUM.events_emitted e
JOIN c ON c.tx_id = e.tx_id
WHERE block_timestamp >= CURRENT_DATE - 18



SELECT *
FROM ETHEREUM_CORE.FACT_TOKEN_TRANSFERS
WHERE contract_address = '0xa5f2211B9b8170F694421f2046281775E8468044'
LIMIT 100

SELECT *
FROM ethereum.events_emitted
WHERE contract_address = '0xa5f2211b9b8170f694421f2046281775e8468044'
LIMIT 100

106. [Hard] New User Onboarding
When a new wallet gets created, where are they first getting their $RUNE? Is it from a direct transfer? Or from a centralized exchange? From a $RUNE upgrade from BNB.RUNE or ETH.RUNE? Or from a swap from another chain's asset to $RUNE? If it's a swap, which chain / asset are they coming from? Are there any trends over time you can see?


conn = psycopg2.connect(
    host="vic5o0tw1w-repl.twtim97jsb.tsdb.cloud.timescale.com",
    user="tsdbadmin",
    password="yP4wU5bL0tI0kP3k"
)

-- onboarding new users
-- we have run scavenger hunts specifically designed to onboard new users
-- show weekly new users and color when they come from Flipside
-- CAC vs block rewards

-- what % were new to the ecosystem in the last 3 weeks

  -- show new LPers and color when they come from flipside

-- show twitter stats

-- show Rune holder retention from Flipside
-- show wallet and when they first get their rune and if they are still a participant in the ecosystem
-- stay involved in the ecosystem

 l AS (
  SELECT
  COALESCE(address, '') AS address
  , pool_name
  , sum(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) as net_stake_units
  FROM thorchain.liquidity_actions
  WHERE lp_action IN ('add_liquidity','remove_liquidity')
  GROUP BY 1, 2
), l2 AS (
  SELECT *
  , SUM(net_stake_units) OVER (PARTITION BY address, pool_name ORDER BY month) AS cumu_stake_units
  FROM l
),


WITH base AS (
  SELECT from_address AS address
  , date_trunc('month', block_timestamp) AS month
  , SUM(-rune_amount) AS net_rune_amount
  FROM thorchain.transfers
  GROUP BY 1, 2
  UNION
  SELECT to_address AS address
  , date_trunc('month', block_timestamp) AS month
  , SUM(rune_amount) AS net_rune_amount
  FROM thorchain.transfers
  GROUP BY 1, 2
), b2 AS (
  SELECT address
  , MIN(month) AS month
  , SUM(net_rune_amount) AS net_rune_amount
  FROM base
  GROUP BY 1
), l AS (
  SELECT
  from_address AS address
  , pool_name
  , sum(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) as net_stake_units
  FROM thorchain.liquidity_actions
  WHERE lp_action IN ('add_liquidity','remove_liquidity')
  AND address LIKE 'thor%'
  GROUP BY 1, 2
), l2 AS (
  SELECT DISTINCT address
  FROM l 
  WHERE net_stake_units > 0
), f AS (
  SELECT DISTINCT address
  FROM silver_crosschain.ntr
  WHERE address LIKE 'thor%'
)
SELECT 
-- date_trunc('year', month) AS month
CASE WHEN f.address IS NULL THEN 0 ELSE 1 END AS is_flipside
, COUNT(1) AS n
, AVG(CASE WHEN net_rune_amount > 0 OR l2.address IS NOT NULL THEN 1 ELSE 0 END) AS pct_hold
, AVG(CASE WHEN net_rune_amount > 0 AND l2.address IS NULL THEN 1 ELSE 0 END) AS pct_rune_only
, AVG(CASE WHEN net_rune_amount <= 0 AND l2.address IS NOT NULL THEN 1 ELSE 0 END) AS pct_lp_only
, AVG(CASE WHEN net_rune_amount > 0 AND l2.address IS NOT NULL THEN 1 ELSE 0 END) AS pct_lp_and_rune
FROM b2
LEFT JOIN l2 ON l2.address = b2.address
LEFT JOIN f ON f.address = b2.address
GROUP BY 1

SELECT * 
FROM silver_crosschain.ntr
WHERE symbol = 'RUNE'
LIMIT 10


SELECT * FROM flipside_dev_db.silver_crosschain.nft_fair_market_value LIMIT 100

SELECT * FROM flipside_prod_db.silver_crosschain.nft_fair_market_value LIMIT 100

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
FROM dex.trades
WHERE (
  token_a_address = '\x4200000000000000000000000000000000000042'
  OR token_b_address = '\x4200000000000000000000000000000000000042'
)
AND usd_amount > 0 
AND token_a_amount_raw>0
AND token_b_amount_raw>0
LIMIT 100


SELECT payment_currency
, SUM(payment_amount) AS payment_amount
, COUNT(1) AS n
FROM BI_ANALYTICS.BRONZE.HEVO_PAYMENTS
GROUP BY 1

SELECT block_timestamp::date AS date
, COUNT(1) AS n
, COUNT(1) AS n2
FROM thorchain.transfers
WHERE block_timestamp >= '2022-05-01'
GROUP BY 1

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

SELECT DISTINCT project_name
FROM crosschain.address_labels
WHERE blockchain = 'solana'
AND label_subtype = 'nf_token_contract'



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

SELECT DISTINCT project_name
FROM solana.dim_nft_metadata




SELECT COUNT(1)
FROM thorchain.bond_events

SELECT COUNT(1)
FROM bronze_midgard_2_6_9_20220405.midgard_bond_events


with aa as (
  select from_address,
  pool_name,
  SUM(COALESCE(il_protection_usd, 0)) as il_protection,
  MIN(block_timestamp) as min_date,
  MAX(block_timestamp) as max_date
  from thorchain.liquidity_actions 
  group by 1, 2
)
select 
CAST(DATEDIFF('days', min_date, max_date) AS INT) AS weeks_in_pool
, SUM(il_protection) AS il_protection
from aa 
group by 1 


SELECT *
FROM thorchain.liquidity_actions
WHERE from_address = 'thor1zfef8qe3xcxnwwn9ntd2thzyw6n7a4m9xuyr56'

SELECT project_name AS collection
, mint
, token_id
FROM solana.dim_nft_metadata
LIMIT 10


SELECT * 
FROM solana.dim_nft_metadata m
JOIN flipside_prod_db.bronze.prod_data_science_uploads_1748940988 u 
  ON m.token_id = u.token_id
  AND m.token_id = u.token_id
WHERE record_content[0]:collection IS NOT NULL
AND record_metadata:key like '%nft-deal-score-rankings-%'
AND record_content[0]:collection = 'Solana Monkey Business'

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
  JOIN solana.dim_nft_metadata m ON m.token_id = t.value:token_id AND m.project_name = t.value:collection
  WHERE m.mint = 'B4GxvM6EjBKUUvQ3bhrs1UK8qhbNXVm6Q2ewZNM7hcgB'
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



with add as (select
  from_address as address,
  pool_name as poolname,
  sum(stake_units) as units
from thorchain.liquidity_actions
  where lp_action = 'add_liquidity'
  group by 1,2),

  remove as (select
  from_address as address,
  pool_name as poolname,
  sum(stake_units) as units
  from thorchain.liquidity_actions
  where lp_action = 'remove_liquidity'
  group by 1,2),

  combo as (select
  a.address as addresses,
  a.poolname as pools,
  a.units as add_units,
  b.units as remove_units,
  a.units - ifnull(b.units, 0) as net
  from add a
  left join remove b
  on (a.address = b.address and a.poolname = b.poolname)
  --where addresses = 'thor1tecpk5zmd225ec4ma56q20j0xejd7ay7xmtajl'
  where net > 0
  group by 1,2,3,4,5),
  
table1 as (select addresses,
a.pools,
date_trunc('day',block_timestamp) as day
from combo a 
join thorchain.liquidity_actions b
on a.addresses = b.from_address and a.pools = b.pool_name
where lp_action = 'add_liquidity'),

table2 as (select addresses,
pools,
min(day) as earliest_add
from table1
group by 1,2),

active AS (
  SELECT DISTINCT split(pool_name, '-')[0]
  FROM thorchain.swaps WHERE block_timestamp >= CURRENT_DATE - 3
)

base as (select addresses, --ifnull(count(addresses),0) as addresses,
  pools,
  split(pools, '-')[0] as pool_namez,
  datediff(day, earliest_add, current_date) as lp_period
from table2
order by 3 asc)

select pool_namez,
avg(lp_period)
from base b
JOIN active a ON a.pool_name = b.pool_namez
group by 1




SELECT block_timestamp::date AS date
, tx_id
, ARRAY_SIZE(inner_instructions[0]:instructions) AS sz
, inner_instructions[0]:instructions[0]:parsed:info:amount::int * POWER(10, -8) AS amt_pay
, inner_instructions[0]:instructions[0]:parsed:info:authority::string AS address
, inner_instructions[0]:instructions[1]:parsed:info:amount::int * POWER(10, -8) AS amt_fee
, inner_instructions[0]:instructions[2]:parsed:info:amount::int * POWER(10, -8) AS amt_buy
, inner_instructions[0]:instructions[2]:parsed:info:mint::string AS mint
FROM solana.fact_transactions 
WHERE block_timestamp >= CURRENT_DATE - 4
AND instructions[0]:programId = 'GrcZwT9hSByY1QrUTaRPp6zs5KxAA5QYuqEhjT1wihbm'
AND ARRAY_SIZE(inner_instructions[0]:instructions) = 3


WITH mint AS (
  SELECT block_timestamp::date AS date
  , tx_id
  , ARRAY_SIZE(inner_instructions[0]:instructions) AS sz
  , inner_instructions[0]:instructions[0]:parsed:info:amount::int * POWER(10, -8) AS amt_pay
  , inner_instructions[0]:instructions[0]:parsed:info:authority::string AS address
  , inner_instructions[0]:instructions[1]:parsed:info:amount::int * POWER(10, -8) AS amt_fee
  , inner_instructions[0]:instructions[2]:parsed:info:amount::int * POWER(10, -8) AS amt_buy
  , inner_instructions[0]:instructions[2]:parsed:info:mint::string AS mint
  FROM solana.fact_transactions 
  WHERE block_timestamp >= CURRENT_DATE - 7
  AND instructions[0]:programId = 'GrcZwT9hSByY1QrUTaRPp6zs5KxAA5QYuqEhjT1wihbm'
  AND ARRAY_SIZE(inner_instructions[0]:instructions) = 3
  AND mint IN ('Fgf39CUcAN5CLduHCswTzTj42TRTLV8511iCnSEnkdpJ','9e99vFWTaxDewCUv5RpMp45G5wjMpygtpAY3NXuWDk3A')
), burn AS (
  SELECT block_timestamp::date AS date
  , tx_id
  , ARRAY_SIZE(inner_instructions[0]:instructions) AS sz
  , inner_instructions[0]:instructions[0]:parsed:info:amount::int * POWER(10, -8) AS amt_brn
  , inner_instructions[0]:instructions[0]:parsed:info:authority::string AS address
  , inner_instructions[0]:instructions[0]:parsed:info:mint::string AS mint
  , inner_instructions[0]:instructions[1]:parsed:info:amount::int * POWER(10, -8) AS amt_pbt
  , inner_instructions[0]:instructions[2]:parsed:info:amount::int * POWER(10, -8) AS amt_props
  FROM solana.fact_transactions 
  WHERE block_timestamp >= CURRENT_DATE - 7
  AND instructions[0]:programId = 'GrcZwT9hSByY1QrUTaRPp6zs5KxAA5QYuqEhjT1wihbm'
  AND ARRAY_SIZE(inner_instructions[0]:instructions) = 4
  AND mint IN ('Fgf39CUcAN5CLduHCswTzTj42TRTLV8511iCnSEnkdpJ','9e99vFWTaxDewCUv5RpMp45G5wjMpygtpAY3NXuWDk3A')
), m AS (
  SELECT address
  , mint
  , SUM(amt_pay) AS amt_pay
  , SUM(amt_fee) AS amt_fee
  , SUM(amt_buy) AS amt_buy
  FROM mint
  GROUP BY 1, 2
), b AS (
  SELECT address
  , mint
  , SUM(amt_brn) AS amt_brn
  , SUM(amt_pbt) AS amt_pbt
  , SUM(amt_props) AS amt_props
  FROM burn
  GROUP BY 1, 2
), reward AS (
  SELECT m.mint
  , SUM(amt_pbt) / SUM(amt_buy) AS pbt_per_token
  , SUM(amt_props) / SUM(amt_buy) AS props_per_token
  FROM m
  JOIN b ON b.address = m.address AND b.mint = m.mint
  WHERE amt_pbt > 0 AND amt_props > 0
  GROUP BY 1
)
SELECT COALESCE(m.address, b.address) AS address
, COALESCE(m.mint, b.mint) AS mint
, ROUND(COALESCE(m.amt_pay, 0), 4) AS amt_pay
, ROUND(COALESCE(m.amt_fee, 0), 4) AS amt_fee
, ROUND(COALESCE(m.amt_buy, 0), 4) AS amt_buy
, ROUND(amt_pay + amt_fee, 4) AS cost
, ROUND(cost / amt_buy, 4) AS price
, ROUND(COALESCE(b.amt_brn, 0), 4) AS amt_brn
, ROUND(COALESCE(b.amt_pbt, 0), 4) AS amt_pbt_1
, ROUND(COALESCE(b.amt_props, 0), 4) AS amt_props_1
, ROUND(COALESCE(m.amt_buy * pbt_per_token, 0), 4) AS amt_pbt
, ROUND(COALESCE(m.amt_buy * props_per_token, 0), 4) AS amt_props
FROM m
FULL OUTER JOIN b ON b.address = m.address AND b.mint = m.mint
LEFT JOIN reward r ON r.mint = m.mint




-- 8: initialize the pool
-- 5: burn pool
-- 4: sell team tokens
-- 3: buy team token



with aa as (
  select from_address,
  pool_name,
  SUM(COALESCE(il_protection_usd, 0)) as il_protection,
  MIN(CASE WHEN lp_action = 'add_liquidity' THEN block_timestamp ELSE NULL END) as min_date,
  MIN(CASE WHEN lp_action = 'remove_liquidity' THEN block_timestamp ELSE NULL END) as max_date,
  from thorchain.liquidity_actions 
  group by 1, 2
)
select 
CAST(DATEDIFF('days', min_date, max_date) AS INT) + 1 AS weeks_in_pool
, SUM(il_protection) AS il_protection
from aa 
WHERE max_date IS NOT NULL AND min_date IS NOT NULL
group by 1 



SELECT COUNT(1)
FROM thorchain.bond_actions




WITH mints AS (
  SELECT DISTINCT project_name
  , mint
  , token_id
  FROM solana.dim_nft_metadata
  WHERE project_name = 'Okay Bears'
), rem AS (
  SELECT pre_token_balances[0]:mint::string AS mint
  , m.project_name
  , m.token_id
  , t.tx_id AS remove_tx
  , t.block_timestamp AS remove_time
  , LEFT(instructions[0]:data::string, 4) AS remove_data
  , CASE WHEN s.tx_id IS NULL THEN 0 ELSE 1 END AS is_sale
  , s.sales_amount
  , ROW_NUMBER() OVER (PARTITION BY m.mint ORDER BY t.block_timestamp DESC) AS rn
  FROM solana.fact_transactions t
  JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
  LEFT JOIN solana.fact_nft_sales s ON s.tx_id = t.tx_id
  WHERE t.block_timestamp >= CURRENT_DATE - 3
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
, DATEDIFF('hours', a.listing_time, r.remove_time) AS time_to_sell
, r.is_sale
, r.sales_amount
, CASE WHEN r.remove_time IS NULL OR a.listing_time > r.remove_time THEN 1 ELSE 0 END AS is_listed
FROM add a
LEFT JOIN rem r ON r.mint = a.mint AND r.rn = 1 AND a.listing_time <= r.remove_time
WHERE a.rn = 1 AND is_sale = 1

WITH mints AS (
  SELECT DISTINCT LOWER(project_name) AS project_name
  , address AS mint
  FROM crosschain.address_labels
  WHERE blockchain = 'solana'
  AND label_subtype = 'nf_token_contract'
  UNION 
  SELECT DISTINCT LOWER(project_name) AS project_name
  , mint
  FROM solana.dim_nft_metadata
)
SELECT project_name, mint, COUNT(1) AS n
FROM mints
GROUP BY 1, 2
ORDER BY 3 DESC







WITH mints AS (
  SELECT DISTINCT LOWER(project_name) AS project_name
  , address AS mint
  FROM crosschain.address_labels
  WHERE blockchain = 'solana'
  AND label_subtype = 'nf_token_contract'
  UNION 
  SELECT DISTINCT LOWER(project_name) AS project_name
  , mint
  FROM solana.dim_nft_metadata
), rem AS (
  SELECT pre_token_balances[0]:mint::string AS mint
  , m.project_name
  , t.tx_id AS remove_tx
  , t.block_timestamp AS remove_time
  , CASE WHEN s.tx_id IS NULL THEN 0 ELSE 1 END AS is_sale
  , s.sales_amount
  , ROW_NUMBER() OVER (PARTITION BY m.mint ORDER BY t.block_timestamp DESC) AS rn
  FROM solana.fact_transactions t
  JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
  LEFT JOIN solana.fact_nft_sales s ON s.tx_id = t.tx_id
  WHERE t.block_timestamp >= CURRENT_DATE - 13
  AND LEFT(instructions[0]:data::string, 4) IN ('ENwH','3GyW')
  AND instructions[0]:programId = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
), add AS (
  SELECT pre_token_balances[0]:mint::string AS mint
  , m.project_name
  , t.tx_id AS listing_tx
  , block_timestamp AS listing_time
  , ROW_NUMBER() OVER (PARTITION BY mint ORDER BY block_timestamp DESC) AS rn
  FROM solana.fact_transactions t
  JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
  WHERE block_timestamp >= CURRENT_DATE - 13
  AND LEFT(instructions[0]:data::string, 4) IN ('2B3v')
  AND instructions[0]:programId = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
  AND succeeded = TRUE
), base AS (
  SELECT a.*
  , r.remove_tx
  , r.remove_time
  , date_trunc('day', a.listing_time) AS listing_day
  , date_trunc('day', r.remove_time) AS remove_day
  , r.is_sale
  , r.sales_amount
  , CASE WHEN r.remove_time IS NULL OR a.listing_time > r.remove_time THEN 1 ELSE 0 END AS is_listed
  , ROW_NUMBER() OVER (PARTITION BY a.listing_tx ORDER BY r.remove_time ASC) AS rn2
  FROM add a
  LEFT JOIN rem r ON r.mint = a.mint AND a.listing_time <= r.remove_time
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY remove_tx ORDER BY listing_time DESC) AS rn3
  FROM base 
  WHERE rn2 = 1
), b3 AS (
  SELECT *
  FROM b2
  WHERE rn3 = 1
), days AS (
  SELECT DISTINCT listing_day AS day
  FROM b3
  UNION
  SELECT DISTINCT remove_day AS day
  FROM b3
), filter AS (
  SELECT project_name
  , SUM(sales_amount) AS amt
  FROM base
  WHERE remove_day >= CURRENT_DATE - 14
  AND sales_amount > 0
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 100
)
SELECT h.day
, b.project_name
, MIN(sales_amount) AS floor
FROM days h
JOIN b3 b ON b.listing_day <= h.day AND COALESCE(b.remove_day, h.day) >= h.day
JOIN filter f ON f.project_name = b.project_name
WHERE COALESCE(b.sales_amount, 0) > 0
GROUP BY 1, 2
ORDER BY 1








WITH mints AS (
  SELECT DISTINCT project_name
  , mint
  , token_id
  FROM solana.dim_nft_metadata
  WHERE project_name IN ('Okay Bears','Catalina Whale Mixer')
), rem AS (
  SELECT pre_token_balances[0]:mint::string AS mint
  , m.project_name
  , m.token_id
  , t.tx_id AS remove_tx
  , t.block_timestamp AS remove_time
  , CASE WHEN s.tx_id IS NULL THEN 0 ELSE 1 END AS is_sale
  , s.sales_amount
  , ROW_NUMBER() OVER (PARTITION BY m.mint ORDER BY t.block_timestamp DESC) AS rn
  FROM solana.fact_transactions t
  JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
  LEFT JOIN solana.fact_nft_sales s ON s.tx_id = t.tx_id
  WHERE t.block_timestamp >= CURRENT_DATE - 3
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
), base AS (
  SELECT a.*
  , r.remove_tx
  , r.remove_time
  , date_trunc('hour', a.listing_time) AS listing_hour
  , date_trunc('hour', r.remove_time) AS remove_hour
  , r.is_sale
  , r.sales_amount
  , CASE WHEN r.remove_time IS NULL OR a.listing_time > r.remove_time THEN 1 ELSE 0 END AS is_listed
  , ROW_NUMBER() OVER (PARTITION BY a.listing_tx ORDER BY r.remove_time ASC) AS rn2
  FROM add a
  LEFT JOIN rem r ON r.mint = a.mint AND a.listing_time <= r.remove_time
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY remove_tx ORDER BY listing_time DESC) AS rn3
  FROM base 
  WHERE rn2 = 1
), b3 AS (
  SELECT *
  FROM b2
  WHERE rn3 = 1
), hours AS (
  SELECT DISTINCT listing_hour AS hour
  FROM b3
  UNION
  SELECT DISTINCT remove_hour AS hour
  FROM b3
)
SELECT h.hour
, project_name
, MIN(sales_amount) AS floor
FROM hours h
JOIN b3 b ON b.listing_hour <= h.hour AND COALESCE(b.remove_hour, h.hour) >= h.hour
WHERE COALESCE(b.sales_amount, 0) > 0
GROUP BY 1, 2
ORDER BY 1




WITH mints AS (
  SELECT DISTINCT project_name
  , mint
  , token_id
  FROM solana.dim_nft_metadata
  WHERE project_name = 'Okay Bears'
), rem AS (
  SELECT pre_token_balances[0]:mint::string AS mint
  , m.project_name
  , m.token_id
  , t.tx_id AS remove_tx
  , t.block_timestamp AS remove_time
  , CASE WHEN s.tx_id IS NULL THEN 0 ELSE 1 END AS is_sale
  , s.sales_amount
  , ROW_NUMBER() OVER (PARTITION BY m.mint ORDER BY t.block_timestamp DESC) AS rn
  FROM solana.fact_transactions t
  JOIN mints m ON m.mint = t.pre_token_balances[0]:mint::string
  LEFT JOIN solana.fact_nft_sales s ON s.tx_id = t.tx_id
  WHERE t.block_timestamp >= CURRENT_DATE - 3
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
), base AS (
  SELECT a.*
  , r.remove_tx
  , r.remove_time
  , date_trunc('hour', a.listing_time) AS listing_hour
  , date_trunc('hour', r.remove_time) AS remove_hour
  , r.is_sale
  , r.sales_amount
  , CASE WHEN r.remove_time IS NULL OR a.listing_time > r.remove_time THEN 1 ELSE 0 END AS is_listed
  FROM add a
  LEFT JOIN rem r ON r.mint = a.mint AND r.rn = 1
  WHERE a.rn = 1
), hours AS (
  SELECT DISTINCT listing_hour AS hour
  FROM base
  UNION
  SELECT DISTINCT remove_hour AS hour
  FROM base
)
SELECT h.hour
, MIN(sales_amount) AS floor
FROM hours h
JOIN base b ON b.listing_hour <= h.hour AND COALESCE(b.remove_hour, h.hour) >= h.hour
WHERE COALESCE(b.sales_amount, 0) > 0
GROUP BY 1
ORDER BY 1


SELECT *
FROM thorchain.fee_events
WHERE block_id = 5687427
QUALIFY COUNT(tx_id) OVER(PARTITION BY tx_id) <> 3
ORDER BY pool_deduct

SELECT * FROM midgard.fee_events
LIMIT 100

Chat with 
Central wallets tranferring into new wallets


WITH base AS (
  SELECT INITCAP(l.label) AS collection, s.purchaser, SUM(s.sales_amount) AS volume
  FROM solana.core.fact_nft_sales s
  JOIN solana.core.dim_labels l ON l.address = s.mint
  GROUP BY 1, 2
), b2 AS (
  SELECT collection, purchaser, volume, SUM(volume) OVER (PARTITION BY collection) AS tot_volume
  , ROW_NUMBER() OVER (PARTITION BY collection ORDER BY volume DESC) AS rn
  FROM base
), b3 AS (
  SELECT collection, tot_volume, SUM(volume) AS top_vol
  , ROW_NUMBER() OVER (ORDER BY tot_volume DESC) AS rn_2
  FROM b2
  WHERE rn <= 50
  GROUP BY 1, 2
) 
SELECT *, top_vol / tot_volume AS pct_top
FROM b3
WHERE rn_2 <= 25
ORDER BY pct_top DESC


SELECT COUNT(tx)
, COUNT(asset)
, COUNT(asset_e8)
, COUNT(pool_deduct)
, COUNT(block_timestamp)
, COUNT(1) AS n
FROM midgard.fee_events

Using NFT artist from in-house
Build out the brand
Get onto Magic Eden launchpad
Hit things quick


Outfit_Astronaut Light
Outfit_Astronaut Orange
Head_Crown
Attribute count_1 7
Edition_Alien
Outfit_Armor Light


SELECT block_timestamp::date AS date
, COUNT(1) AS n
FROM solana.core.fact_nft_sales
WHERE sales_amount = 0
GROUP BY 1
ORDER BY 1

SELECT *
FROM bronze_midgard_2_6_9_20220405.switch_events
LIMIT 100


SELECT COUNT(1) AS n
FROM midgard.fee_events
WHERE block_timestamp <= 1656360639000000000
  AND block_timestamp <  1654041600000000000

SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
, MAX(block_timestamp) AS mxx
FROM thorchain.fee_events

SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
, MAX(block_timestamp) AS mxx
FROM thorchain.bond_events





WITH base AS (
  SELECT tx
  , asset
  , asset_e8
  , pool_deduct
  , block_timestamp
  , COUNT(1) AS n
  FROM midgard.fee_events
  GROUP BY 1, 2, 3, 4, 5
  HAVING COUNT(1) > 1
)
SELECT MAX(block_timestamp) AS mx
, COUNT(1) AS n
, COUNT(1) AS nn
FROM base

fee_events
bond_events
switch_events
transfer_events


WITH base AS (
  SELECT tx
  , asset
  , asset_e8
  , pool_deduct
  , block_timestamp
  , COUNT(1) AS n
  FROM bronze_midgard_2_6_9_20220405.midgard_fee_events
  GROUP BY 1, 2, 3, 4, 5
  HAVING COUNT(1) > 1
)
SELECT MAX(block_timestamp) AS mx
, COUNT(1) AS n
, COUNT(1) AS nn
FROM base

WITH base AS (
  SELECT tx_id
  , asset
  , asset_e8
  , pool_deduct
  , block_timestamp
  , COUNT(1) AS n
  FROM thorchain.fee_events
  GROUP BY 1, 2, 3, 4, 5
  HAVING COUNT(1) > 1
)
SELECT MAX(block_timestamp) AS mx
, COUNT(1) AS n
, COUNT(1) AS nn
FROM base



WITH base AS (
  SELECT tx_id
  , asset
  , asset_e8
  , pool_deduct
  , block_timestamp
  , COUNT(1) AS n
  FROM thorchain.fee_events
  GROUP BY 1, 2, 3, 4, 5
  HAVING COUNT(1) > 1
)
SELECT MAX(block_timestamp) AS mx
, COUNT(1) AS n
, COUNT(1) AS nn
FROM base

WITH base AS (
  SELECT from_addr
  , to_addr
  , asset
  , amount_e8
  , block_timestamp
  , COUNT(1) AS cnt
  FROM midgard.transfer_events
  WHERE block_timestamp >= 1650092800000000000
  AND block_timestamp < 1654041600000000000
  GROUP BY 1, 2, 3, 4, 5
  HAVING COUNT(1) > 1
)
SELECT asset
, COUNT(1) AS n
FROM base
GROUP BY 1



SELECT tx
, chain
, from_addr
, to_addr
, asset
, asset_e8
, memo
, bond_type
, e8
, block_timestamp
, COUNT(1) AS cnt
FROM FLIPSIDE_PROD_DB.BRONZE_MIDGARD_2_6_9_20220405.BOND_EVENTS_PK_COUNT
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10


SELECT CASE WHEN tx IS NULL THEN 'null' ELSE tx END AS clean_tx
, COUNT(1) AS cnt
FROM midgard.bond_events
WHERE tx IS NULL OR tx = ''
GROUP BY 1


SELECT CASE WHEN chain IS NULL THEN 'null' ELSE chain END AS chain_tx
, COUNT(1) AS cnt
FROM midgard.bond_events
WHERE tx IS NULL OR chain = ''
GROUP BY 1

SELECT project_name
, mint
, token_id
, token_metadata:"Accessories"::string AS "Accessories"
, token_metadata:"Attribute Count"::int AS "Attribute Count"
, token_metadata:"Background"::string AS "Background"
, token_metadata:"Big Drip"::string AS "Body"
, token_metadata:"Coat"::string AS "Coat"
, token_metadata:"Eyes"::string AS "Eyes"
, token_metadata:"Head"::string AS "Head"
, token_metadata:"Mouth"::string AS "Mouth"
, token_metadata:"Shoe"::string AS "Shoe"
FROM solana.dim_nft_metadata
WHERE project_name = 'Bubblegoose Ballers'
LIMIT 10

SELECT DISTINCT mint, project_name AS collection
FROM solana.dim_nft_metadata m 
LEFT JOIN solana.dim_labels l ON l.address = m.mint
WHERE l.address IS NULL

WITH sales AS (
  SELECT l.label AS collection, SUM(sales_amount) AS volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON l.address = s.mint
  GROUP BY 1
), base AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY volume DESC) AS volume_rank
  FROM sales
  ORDER BY volume DESC
  LIMIT 50
), b2 AS (
  SELECT DISTINCT collection, purchaser, mint
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON l.address = s.mint
  JOIN base b ON b.collection = l.label
  UNION 
  SELECT DISTINCT collection, purchaser, mint
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON l.address = m.mint
  JOIN base b ON b.collection = l.label
)
SELECT DISTINCT collection, purchaser, mint
FROM b2


WITH a AS (
  SELECT DISTINCT mint
  FROM solana.fact_nft_sales s
), b AS (
  SELECT l.label
  , SUM(s.mint_price) AS mint_volume
  , SUM(s.mint_price) / COUNT(DISTINCT s.mint) AS mint_price
  FROM solana.fact_nft_mints s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE mint_currency = 'So11111111111111111111111111111111111111111'
  AND block_timestamp >= '2022-03-01'
  GROUP BY 1
), c AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY mint_volume DESC) AS mint_volume_rank
  FROM b
), d AS (
  SELECT l.label
  , SUM(s.sales_amount) AS sales_volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON l.address = s.mint
  GROUP BY 1
), e AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY sales_volume DESC) AS sales_volume_rank
  FROM d
)
SELECT c.label AS collection
, c.mint_volume
, c.mint_volume_rank
, e.sales_volume
, e.sales_volume_rank
, c.mint_price
, COUNT(DISTINCT m.mint) AS n_mints
, COUNT(DISTINCT a.mint) AS n_sell
, 1 - (n_sell / n_mints) AS pct_hold
FROM solana.fact_nft_mints m
JOIN solana.dim_labels l ON l.address = m.mint
JOIN c ON c.label = l.label
JOIN e ON e.label = l.label
LEFT JOIN a ON a.mint = m.mint
GROUP BY 1, 2, 3, 4, 5, 6


SELECT *
FROM solana.dim_labels
WHERE label = 'lizard'



WITH sales AS (
  SELECT INITCAP(l.label) AS collection
  , SUM(s.sales_amount) AS volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE date_trunc('month', s.block_timestamp) = '2022-05-01'
  GROUP BY 1
), base AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY volume DESC) AS rn
  FROM sales
), b2 AS (
  SELECT *
  , CASE WHEN rn < 10 THEN CONCAT('0', rn::varchar, '. ', collection) ELSE '10. Other' END AS label
  FROM base
), b3 AS (
  SELECT label
  , SUM(volume) AS volume
  FROM b2
  GROUP BY 1
  ORDER BY 1
)
SELECT *
, volume / SUM(volume) OVER () AS pct
FROM b3


WITH sales AS (
  SELECT INITCAP(l.label) AS collection
  , date_trunc('month', s.block_timestamp) AS month
  , SUM(s.sales_amount) AS volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE date_trunc('month', s.block_timestamp) >= '2022-01-01'
  GROUP BY 1, 2
), base AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY month ORDER BY volume DESC) AS rn
  FROM sales
), b2 AS (
  SELECT *
  , CONCAT(LEFT(month::varchar, 2), '. ', TO_CHAR(month, 'month'), ' - ', collection) AS label
  FROM base
  WHERE rn = 1
)
SELECT *
FROM b2

SELECT L1_LABEL, COUNT(1) AS n
FROM FLIPSIDE_PROD_DB.SILVER_CROSSCHAIN.ADDRESS_LABELS
GROUP BY 1
ORDER BY 2 DESC


SELECT L1_LABEL, COUNT(1) AS n
FROM FLIPSIDE_PROD_DB.SILVER_CROSSCHAIN.ADDRESS_LABELS
//WHERE blockchain = 'solana'
//ORDER BY insert_date desc
LIMIT 100

WITH prices AS (
  SELECT recorded_at::date AS date
  , AVG(price) AS sol_price
  FROM silver.prices_v2
  WHERE asset_id = 'solana'
  GROUP BY 1
), mints AS (
  SELECT INITCAP(l.label) AS collection
  , SUM(mint_price) AS sol_volume
  , COUNT(DISTINCT m.mint) AS n_mints
  , MIN(block_timestamp::date) AS mn_date
  , MAX(block_timestamp::date) AS mx_date
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON l.address = m.mint
  GROUP BY 1
), base AS (
  SELECT m.*
  , p.sol_price
  , p.sol_price * sol_volume AS usd_volume
  , sol_volume / n_mints AS mint_price
  , ROW_NUMBER() OVER (ORDER BY usd_volume DESC) AS usd_volume_rank
  FROM mints m
  JOIN prices p ON p.date = m.mn_date
  WHERE mint_price <= 10
  ORDER BY usd_volume DESC
)
SELECT * 
FROM base
ORDER BY usd_volume DESC

WITH a AS (
  SELECT DISTINCT mint
  FROM solana.fact_nft_sales s
), b AS (
  SELECT CASE WHEN l.label = 'lizard' THEN 'Reptilian Renegade' ELSE l.label END AS label
  , SUM(s.mint_price) AS mint_volume
  , SUM(s.mint_price) / COUNT(DISTINCT s.mint) AS mint_price
  FROM solana.fact_nft_mints s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE mint_currency = 'So11111111111111111111111111111111111111111'
  AND block_timestamp >= '2022-03-01'
  -- data issue, need to look into this
  AND NOT l.label IN ( 'degen trash pandas' )
  GROUP BY 1
), c AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY mint_volume DESC) AS mint_volume_rank
  FROM b
), d AS (
  SELECT l.label
  , SUM(s.sales_amount) AS sales_volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE block_timestamp >= '2022-03-01'
  GROUP BY 1
), e AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY sales_volume DESC) AS sales_volume_rank
  FROM d
), base AS (
  SELECT INITCAP(c.label) AS collection
  , c.mint_volume
  , c.mint_volume_rank
  , e.sales_volume
  , e.sales_volume_rank
  , c.mint_price
  , COUNT(DISTINCT m.mint) AS n_mints
  , COUNT(DISTINCT a.mint) AS n_sell
  , 100 * (1 - (n_sell / n_mints)) AS pct_hold
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON l.address = m.mint
  JOIN c ON c.label = l.label
  JOIN e ON e.label = l.label
  LEFT JOIN a ON a.mint = m.mint
  GROUP BY 1, 2, 3, 4, 5, 6
  ORDER BY sales_volume_rank
  LIMIT 35
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY pct_hold DESC) AS rn
  FROM base
)
SELECT *
, CASE WHEN rn < 10 THEN CONCAT('0', rn::varchar) ELSE rn::varchar END AS clean_rn
, CONCAT(clean_rn, '. ', collection) AS label
FROM b2 
ORDER BY label


WITH base AS (
  SELECT token_metadata:Fur::string AS Fur
  , AVG(sales_amount) AS avg_price
  , MEDIAN(sales_amount) AS med_price
  FROM solana.dim_nft_metadata m
  JOIN solana.fact_nft_sales s ON s.mint = m.mint
  WHERE project_name = 'Okay Bears'
  GROUP BY 1
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY avg_price DESC) AS rn
  FROM base
)
SELECT *
, CASE WHEN rn <= 10 THEN CONCAT('0', rn::varchar) ELSE rn END AS clean_rn
, CONCAT(clean_rn, '. ', Fur) AS value
FROM b2
ORDER BY value


SELECT CASE WHEN s.block_timestamp::date <= '2022-05-04' THEN 'Apr 26 - May 4' ELSE 'After May 4' END AS sale_date
, CASE WHEN token_metadata:"Attribute Count"::string = '4' THEN '4 Att' ELSE 'Others' END AS bear_type
, CASE WHEN sale_date = 'Apr 26 - May 4' AND bear_type = '4 Att' THEN '1. Apr 26 - May 4: 4 Att'
WHEN sale_date = 'Apr 26 - May 4'  THEN '2. Apr 26 - May 4: Others'
WHEN bear_type = '4 Att' THEN '3. After May 4: 4 Att'
ELSE '4. After May 4: Others' END AS sale_type
, AVG(s.sales_amount) AS avg_price
, MEDIAN(s.sales_amount) AS med_price
FROM solana.dim_nft_metadata m
JOIN solana.fact_nft_sales s ON s.mint = m.mint
WHERE project_name = 'Okay Bears'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3


SELECT CASE WHEN s.block_timestamp::date <= '2022-05-04' THEN 'Apr 26 - May 4' ELSE 'After May 4' END AS sale_date
, CASE WHEN token_metadata:"Attribute Count"::string = '4' THEN '4 Att' ELSE 'Others' END AS bear_type
, CASE WHEN sale_date = 'Apr 26 - May 4' AND bear_type = '4 Att' THEN '1. Apr 26 - May 4: 4 Att'
WHEN sale_date = 'Apr 26 - May 4'  THEN '2. Apr 26 - May 4: Others'
WHEN bear_type = '4 Att' THEN '3. After May 4: 4 Att'
ELSE '4. After May 4: Others' END AS sale_type
, AVG(s.sales_amount) AS avg_price
, MEDIAN(s.sales_amount) AS med_price
FROM solana.dim_nft_metadata m
JOIN solana.fact_nft_sales s ON s.mint = m.mint
WHERE project_name = 'Okay Bears'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3


-- transfer_events_pk_count
SELECT from_addr
, to_addr
, asset
, amount_e8
, block_timestamp
, COUNT(1) AS cnt
FROM midgard.transfer_events
WHERE block_timestamp >= 1655294400000000000
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(1) > 1


WITH base AS (
  SELECT from_addr
  , to_addr
  , asset
  , amount_e8
  , block_timestamp
  , COUNT(1) AS cnt
  FROM bronze_midgard_2_6_9_20220405.midgard_transfer_events
  WHERE block_timestamp >= 1655294400000000000
  AND block_timestamp <= 1655906524979260126
  GROUP BY 1, 2, 3, 4, 5
  HAVING COUNT(1) > 1
)
SELECT COUNT(1) AS n
, MAX(block_timestamp)
FROM base


SELECT from_addr
, to_addr
, asset
, amount_e8
, block_timestamp
FROM midgard.transfer_events
WHERE from_addr IS NULL
OR to_addr IS NULL
OR asset IS NULL
OR amount_e8 IS NULL
OR block_timestamp IS NULL


WITH base AS (
  SELECT from_addr
  , to_addr
  , asset
  , amount_e8
  , block_timestamp
  , COUNT(1) AS cnt
  FROM midgard.transfer_events
  WHERE block_timestamp >= 1655294400000000000
  AND block_timestamp <= 1655906524979260126
  GROUP BY 1, 2, 3, 4, 5
  HAVING COUNT(1) > 1
)
SELECT COUNT(1) AS n
, MAX(block_timestamp)
FROM base


WITH base AS (
SELECT from_addr
, to_addr
, asset
, amount_e8
, block_timestamp
, COUNT(1) AS cnt
FROM midgard.transfer_events
-- WHERE block_timestamp < 1620299757083512012
WHERE block_timestamp < 1620299000000000000
WHERE block_timestamp < 1640995200000000000
WHERE block_timestamp < 1637301700000000000
WHERE block_timestamp < 1633046400000000000
WHERE block_timestamp >= 1640995200000000000
AND block_timestamp <    1654041600000000000
AND block_timestamp <    1655294400000000000
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(1) > 1
)
SELECT COUNT(1) AS n
FROM base

-- bond_events_pk_count
SELECT tx
, chain
, from_addr
, to_addr
, asset
, asset_e8
, memo
, bond_type
, e8
, block_timestamp
, COUNT(1) AS cnt
FROM midgard.bond_events
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10


SELECT tx
, COALESCE(chain, '') AS chain
, COALESCE(from_addr, '') AS from_addr
, COALESCE(to_addr, '') AS to_addr
, COALESCE(asset, '') AS asset
, asset_e8
, COALESCE(memo, '') AS memo
, bond_type
, e8
, block_timestamp
, COUNT(1) AS cnt
FROM midgard.bond_events


SELECT COUNT(tx) AS tx
, COUNT(chain) AS cahin
, COUNT(from_addr) As from_addr
, COUNT(to_addr) as to_address
, COUNT(asset) as asset
, COUNT(asset_e8) as asset_e8
, COUNT(memo) as memo
, COUNT(bond_type) as bond_type
, COUNT(e8) as 38
, COUNT(block_timestamp) as block_timestamp
, COUNT(1) AS cnt
FROM midgard.bond_events

SELECT COUNT(tx) AS tx
, COUNT(chain) AS cahin
, COUNT(from_addr) As from_addr
, COUNT(to_addr) as to_address
, COUNT(asset) as asset
, COUNT(asset_e8) as asset_e8
, COUNT(memo) as memo
, COUNT(bond_type) as bond_type
, COUNT(e8) as 38
, COUNT(block_timestamp) as block_timestamp
, COUNT(1) AS cnt
FROM bronze_midgard_2_6_9_20220405.midgard_bond_events

SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
FROM flipside_dev_db.thorchain.bond_actions

SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
FROM flipside_dev_db.bronze_midgard_2_6_9_20220405.bond_events

SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
FROM midgard.bond_events
WHERE block_timestamp <= 1655880505782611860


-- fee_events
SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
FROM flipside_dev_db.thorchain.fee_events

SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
FROM flipside_dev_db.bronze_midgard_2_6_9_20220405.midgard_fee_events

SELECT INITCAP(l.label) AS collection
, SUM(s.sales_amount) AS volume
, AVG(s.sales_amount) AS avg_price
, MEDIAN(s.sales_amount) AS med_price
FROM solana.fact_nft_sales s
JOIN solana.dim_labels l ON l.address = s.mint
ORDER BY 2 DESC

SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
FROM midgard.fee_events
WHERE block_timestamp <= 1655330913000000000
WHERE block_timestamp <= 1655885723960048067


-- switch_events
SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
FROM flipside_dev_db.thorchain.switch_events

SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
FROM flipside_dev_db.bronze_midgard_2_6_9_20220405.midgard_switch_events

SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
FROM midgard.switch_events
WHERE block_timestamp <= 1655884351385908318


-- switch_events_pk_count
SELECT tx
, from_addr
, to_addr
, burn_asset
, burn_e8
, mint_e8
, block_timestamp
, COUNT(1) AS cnt
FROM midgard.switch_events
GROUP BY 1, 2, 3, 4, 5, 6, 7

-- transfer_events_pk_count
SELECT from_addr
, to_addr
, asset
, amount_e8
, block_timestamp
, COUNT(1) AS cnt
FROM midgard.transfer_events
GROUP BY 1, 2, 3, 4, 5




WITH base AS (
  SELECT from_addr
  , to_addr
  , asset
  , amount_e8
  , block_timestamp
  , COUNT(1) AS n
  FROM midgard.transfer_events
  WHERE block_timestamp < 16540416000000000000
  AND block_timestamp >=  16513632000000000000
  GROUP BY 1, 2, 3, 4, 5
  HAVING COUNT(1) > 1
)
SELECT MAX(block_timestamp) AS mx
, COUNT(1) AS n
, COUNT(1) AS nn
FROM base


WITH base AS (
  SELECT from_addr
  , to_addr
  , asset
  , amount_e8
  , block_timestamp
  , COUNT(1) AS n
  FROM bronze_midgard_2_6_9_20220405.midgard_transfer_events
  WHERE block_timestamp >= 1653048000000000000
  GROUP BY 1, 2, 3, 4, 5
  HAVING COUNT(1) > 1
)
SELECT MAX(block_timestamp) AS mx
, COUNT(1) AS n
, COUNT(1) AS nn
FROM base



SELECT *
FROM bronze_midgard_2_6_9_20220405.midgard_fee_events
LIMIT 10



SELECT DISTINCT LOWER(project_name) AS lower_collection
FROM solana.dim_nft_metadata


Solana thread on NFT Metadata
THORChain bounty question + grading
Met w/ 9R to discuss expectations
Discussing future bounty qs w/ community in Discord


WITH base AS (
  SELECT s.block_timestamp::date AS date
  , INITCAP(m.project_name) AS collection
  , COUNT(1) AS n
  , SUM(sales_amount) AS volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_nft_metadata m ON m.mint = s.mint
  WHERE s.block_timestamp::date >= CURRENT_DATE - 7
  GROUP BY 1, 2
  ORDER BY 1, 2
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY date ORDER BY volume DESC) AS rn
  FROM base
)
SELECT *
FROM b2
WHERE rn <= 10

WITH base AS (
  SELECT s.block_timestamp::date AS date
  , INITCAP(m.project_name) AS collection
  , COUNT(1) AS n
  , SUM(sales_amount) AS volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_nft_metadata m ON m.mint = s.mint
  WHERE s.block_timestamp::date >= CURRENT_DATE - 20
  GROUP BY 1, 2
  ORDER BY 1, 2
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (PARTITION BY date ORDER BY volume DESC) AS rn
  FROM base
)
SELECT *
FROM b2
WHERE rn <= 1

WITH base AS (
  SELECT s.mint, s.purchaser, s.block_timestamp AS date, s.sales_amount AS price
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE l.label = 'okay bears' AND s.mint = '2E9NM1zugqH253SKiUDN22xnaVgH5vZz44Pv1e4V3PZq'
  UNION
  SELECT s.mint, s.purchaser, s.block_timestamp AS date, s.mint_price AS price
  FROM solana.fact_nft_mints s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE l.label = 'okay bears' AND s.mint = '2E9NM1zugqH253SKiUDN22xnaVgH5vZz44Pv1e4V3PZq'
)
SELECT * FROM base ORDER BY date
SELECT s.mint, s.purchaser, s.block_timestamp AS date, s.sales_amount AS price
FROM solana.fact_nft_sales s
JOIN solana.dim_labels l ON l.address = s.mint
WHERE l.label = 'okay bears' AND s.mint = '2E9NM1zugqH253SKiUDN22xnaVgH5vZz44Pv1e4V3PZq'
UNION
SELECT s.mint, s.purchaser, s.block_timestamp AS date, s.mint_price AS price
FROM solana.fact_nft_mints s
JOIN solana.dim_labels l ON l.address = s.mint
WHERE l.label = 'okay bears' AND s.mint = '2E9NM1zugqH253SKiUDN22xnaVgH5vZz44Pv1e4V3PZq'

WITH base AS (
  SELECT s.mint, s.purchaser
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE l.label = 'solana monkey Business'
  UNION
  SELECT s.mint, s.purchaser
  FROM solana.fact_nft_mints s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE l.label = 'solana monkey Business'
), b2 AS (
  SELECT mint, COUNT(DISTINCT purchaser) AS n_owners
  FROM base
  GROUP BY 1
)
SELECT n_owners
, COUNT (1) AS n_mints
FROM b2
GROUP BY 1



WITH base AS (
  SELECT INITCAP(l.label) AS collection
  , date_trunc('week', s.block_timestamp) AS month
  , SUM(sales_amount) AS volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE s.block_timestamp >= '2022-01-01'
  GROUP BY 1, 2
), b2 AS (
  SELECT collection
  , COUNT(1) AS n_weeks
  FROM base
  WHERE volume >= 2000
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 10
), b3 AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY n_weeks DESC, collection) AS rn
  FROM b2
)
SELECT *
, CONCAT(CASE WHEN rn < 10 THEN CONCAT('0', rn::varchar) ELSE rn::varchar END, '. ', collection) AS label
, CASE WHEN collection = 'Famous Fox Federation' THEN 'Famous Fox Federation' ELSE 'Others' END AS is_fff
FROM b3
ORDER BY label





WITH base AS (
  SELECT INITCAP(l.label) AS collection
  , date_trunc('week', s.block_timestamp) AS month
  , SUM(sales_amount) AS volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON m.address = s.mint
  WHERE m.project_name ilike 'Famous Fox Federation'
  AND s.block_timestamp >= '2021-12-01'
  GROUP BY 1, 2
)
SELECT collection
, COUNT(1) AS n_weeks
FROM base
WHERE volume >= 1000
GROUP BY 1
ORDER BY 2 DESC

WITH base AS (
  SELECT INITCAP(l.label) AS collection
  , date_trunc('week', s.block_timestamp) AS month
  , SUM(sales_amount) AS volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_nft_metadata m ON m.address = s.mint
  WHERE s.block_timestamp >= '2021-12-01'
  GROUP BY 1, 2
)
SELECT collection
, COUNT(1) AS n_weeks
FROM base
WHERE volume >= 1000
GROUP BY 1
ORDER BY 2 DESC

In terms of stuff that's been done before I think Meerkat Millionaires is closest to what we're going for, something friendly but with some swagger (and ours would be more like jocks than rich people). For other examples, Okay Bears + Catalina Whales also in this category.
A little different, but also loved the art from the Cyber Sam Gen 2 collection.
I'd also be open to something a little different, like what VBA game did. This pic that Michael found would be very intriguing.

Something like Meerkat Millionaires / Okay Bears / Catalina Whales

GP Review - how to get to Cantina
How to surface top analyst

Put into snowflake
Documentation over time
Weighting by difficulty
Breaking it into tiers
Check with Sam + Dan
Variable based on score - What does the data say about what 

100,000
10,000 / day
User queries to compute user portfolio info in vaults


BI for blockchains. 

Understand the nuances of the data - the impact of synths on liquidity providers
The impact of adding a new chain to the protocol








VBA Game



data-science
DKiZEzE-CZ+o59}L
boatpartydotbiz

sudo cp /rstudio-data/algorand_console_data.RData ~/
sudo cp ~/ntr_data.RData /rstudio-data/
sudo cp ~/nft_deal_score_data.RData /rstudio-data/
sudo cp ~/nft_deal_score_listings_data.RData /rstudio-data/
sudo cp ~/nft_deal_score_sales_data.RData /rstudio-data/
sudo cp ~/nft_deal_score_sales.csv /rstudio-data/

Coat_Black Jag
Big Drip_Spaced Out
Big Drip_Midnight

Head_King
Head_Brainfreeze
Head_Sahara Dropout
Mouth_Big Smoke
Mouth_Diamond Tasting

Body_Porcelain
Shoe_Froggies
Head_Smoothie Dropout
Accessories_Gold Bgb Chain

Solana NFT showcase

1. Banbannard
2. Zook
3. Multipartite
4. hernandezngronk
5. adriaparcerisas

SELECT COUNT(1) AS n
FROM midgard.slash_amounts

SELECT COUNT(1) AS n
FROM bronze_midgard_2_6_9_20220405.midgard_slash_amounts

SELECT pool
, asset
, asset_e8
, block_timestamp
, COUNT(1) AS n
, COUNT(1) AS n2
FROM bronze_midgard_2_6_9_20220405.midgard_slash_amounts
GROUP BY 1, 2, 3, 4

SELECT 
node_address
, slash_points
, reason
, block_timestamp
, COUNT(1) AS n
FROM midgard.slash_points
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC

SELECT pool
, asset
, asset_e8
, block_timestamp
, COUNT(1) AS n
, COUNT(1) AS n2
FROM midgard.slash_amounts
GROUP BY 1, 2, 3, 4


SELECT 
COUNT(1) AS n
FROM thorchain.slash_amounts


SELECT 
COUNT(1) AS n
, COUNT(1) AS n2
FROM midgard.slash_amounts

sudo chmod 774 nft_deal_score_data.RData
ls -l

SELECT *
FROM thorchain.liquidity_actions
WHERE pool_name ilike '%vthor%'
LIMIT 100


SELECT pool, COUNT(1) as n
FROM bronze_midgard_2_6_9_20220405.midgard_stake_events
WHERE pool ilike '%thor%'
GROUP BY 1
LIMIT 100

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


WITH f AS (
  SELECT l.label
  , MIN(block_timestamp) AS start_time
  , MIN(block_timestamp::date) AS start_date
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON l.address = s.mint
  WHERE marketplace LIKE 'magic%'
  AND block_timestamp >= '2022-05-01'
  GROUP BY 1
)
SELECT start_time::date AS date
, l.label
, COUNT(1) AS n_sales
, SUM(sales_amount) AS volume
FROM solana.fact_nft_sales s
JOIN solana.dim_labels l ON l.address = s.mint
JOIN f ON DATEADD('minute', 60 * 12, start_time) >= block_timestamp AND l.label = f.label
JOIN m ON m.start_date = f.start_date AND l.label = m.label
WHERE date >= '2022-05-10'
GROUP BY 1, 2
ORDER BY 3 DESC


SELECT l.label
, (block_timestamp::date) AS start_date
, COUNT(1) AS n
FROM solana.fact_nft_mints s
JOIN solana.dim_labels l ON l.address = s.mint
WHERE block_timestamp >= '2022-05-01'
AND l.label = 'fine fillies'
GROUP BY 1, 2

Blocksmith Labs
Cets on Creck
Citizens by Solsteads
Dazed Duck
Degen Trash Pandas
Famous Foxes
Galactic Gecko
Generous Robots
Ghostface Gen 2
Marinade Chefs
Portals
Psyker Hideouts
Smoke Heads
Solstein
The Orcs
The Remnants
The Tower
The Vaultx DAO
Visionary Studios

SELECT COALESCE(tx) as tx
, COUNT(from_addr) as from_addr
, COUNT(to_addr) as to_addr
, COUNT(burn_asset) AS burn_asset
, COUNT(burn_e8) AS burn_e8
, COUNT(mint_e8) AS mint_e8
, COUNT(block_timestamp) AS block_timestamp
, COUNT(1) AS cnt
FROM midgard.switch_events

-- check upgrades to make sure it has mint amount
-- check switch events to make sure number of rows is right


SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
, MAX(block_timestamp) AS mxx
FROM flipside_dev_db.thorchain.upgrades

SELECT COUNT(1) AS n
, MAX(block_timestamp) AS mx
, MAX(block_timestamp) AS mxx
FROM midgard.switch_events
WHERE block_timestamp <= 1656483014000000000


goes as profile.yml in ~/.dbt/
dbt deps to install dependencies
dbt test 
dbt run --full-refresh -s models/thorchain
dbt run --full-refresh -s models/crosschain
dbt run --full-refresh -s models/thorchain/dbt/thorchain_dbt__fee_events_pk_count.sql
dbt test -s models/thorchain
dbt run --full-refresh -s models/crosschain
dbt test -s models/crosschain

WITH first_sale AS (
  SELECT l.label
  , MIN(s.block_timestamp) AS mn_date
  FROM solana.core.fact_nft_sales s
  JOIN solana.core.dim_labels l ON l.address = s.mint
  HAVING mn_date >= CURRENT_DATE - 7
)
SELECT INITCAP(l.label) AS collection
, SUM(s.sales_amount) AS volume
, AVG(s.sales_amount) AS avg_price
FROM solana.core.fact_nft_sales s
JOIN solana.core.dim_labels l ON l.address = s.mint
JOIN first_sale f ON f.label = l.label
WHERE s.sales_amount > 0
ORDER BY 2 DESC


WITH first_sale AS (
  SELECT l.label
  , MIN(s.block_timestamp) AS mn_date
  FROM solana.core.fact_nft_sales s
  JOIN solana.core.dim_labels l ON l.address = s.mint
  GROUP BY 1
  HAVING mn_date::date = '2022-06-28'
)
SELECT INITCAP(l.label) AS collection
, SUM(s.sales_amount) AS volume
, AVG(s.sales_amount) AS avg_price
FROM solana.core.fact_nft_sales s
JOIN solana.core.dim_labels l ON l.address = s.mint
JOIN first_sale f ON f.label = l.label
WHERE s.sales_amount > 0
GROUP BY 1
ORDER BY 2 DESC

WITH base AS (
  SELECT node_address
  , slash_points
  , reason
  , block_timestamp
  , COUNT(1) AS n
  FROM midgard.slash_points
  GROUP BY 1, 2, 3, 4
  HAVING COUNT(1) > 1
)
SELECT COUNT(1)
FROM base



with data_token_transfer as (
  select 
    substring(input_data, 202, 1),  tx_hash 
  from 
    ethereum.core.fact_transactions 
  where 
  to_address = '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
  and
  origin_function_signature = '0x0f5287b0' -- tokenTransfer
  and
  status = 'SUCCESS'
  and
  substring(input_data, 202, 1)::string = '1' -- solana
)
, data_token as (
  select
    block_timestamp::date as date,
    symbol,
    sum(amount_usd) as total,
    sum(amount) as total_tokens
  from
    ethereum.core.ez_token_transfers 
  where
    tx_hash in (select tx_hash from data_token_transfer)
  and
    to_address = '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
  and
    symbol is not null
  and
    amount_usd > 0
  group by 1, 2
)
, eth_price as (
  select
    hour,
    price
  from
    ethereum.core.fact_hourly_token_prices
  where
    token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
)
, data_eth as (
  select
    date_trunc('hour', block_timestamp) as hour,
    sum(eth_value) as total_eth
  from
    ethereum.core.fact_transactions
  where
    substring(input_data, 74, 1)::string = '1' -- solana
  and
    to_address = '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
  and
    status = 'SUCCESS'
  and
    origin_function_signature = '0x9981509f' -- wrapAndTransfer
  group by 1
)
, data_token_eth as (
  select hour::date as date, 'ETH' as symbol, sum(total) as total, sum(total_eth) as total_tokens from (
    select 
      a.hour, total_eth*price as total, total_eth
    from data_eth a join eth_price b on a.hour = b.hour
    ) dd group by 1
)
, data_tokens as (
  select * from (
  select 
    date_trunc('month', date) as months, 
    symbol,
    sum(total) as total_volumes,
    sum(total_tokens) as total_volumes_tokens,
    row_number() over (partition by months order by total_volumes desc) as nomor
  from (
  select * from data_token
  union all
  select * from data_token_eth ) ee group by 1, 2 ) ff where nomor = 1
)
select * from data_tokens






with data_token_transfer as (
  select 
    substring(input_data, 202, 1),  tx_hash 
  from 
    ethereum.core.fact_transactions 
  where 
  to_address = '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
  and
  origin_function_signature = '0x0f5287b0' -- tokenTransfer
  and
  status = 'SUCCESS'
  and
  substring(input_data, 202, 1)::string = '1' -- solana
)
, data_token as (
  select
    block_timestamp::date as date,
    symbol,
    sum(amount_usd) as total,
    sum(amount) as total_tokens
  from
    ethereum.core.ez_token_transfers 
  where
    tx_hash in (select tx_hash from data_token_transfer)
  and
    to_address = '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
  and
    symbol is not null
  and
    amount_usd > 0
  group by 1, 2
)
, eth_price as (
  select
    hour,
    price
  from
    ethereum.core.fact_hourly_token_prices
  where
    token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
)
, data_eth as (
  select
    date_trunc('hour', block_timestamp) as hour,
    sum(eth_value) as total_eth
  from
    ethereum.core.fact_transactions
  where
    substring(input_data, 74, 1)::string = '1' -- solana
  and
    to_address = '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
  and
    status = 'SUCCESS'
  and
    origin_function_signature = '0x9981509f' -- wrapAndTransfer
  group by 1
)
, data_token_eth as (
  select hour::date as date, 'ETH' as symbol, sum(total) as total, sum(total_eth) as total_tokens from (
    select 
      a.hour, total_eth*price as total, total_eth
    from data_eth a join eth_price b on a.hour = b.hour
    ) dd group by 1
)
, data_tokens as (
  select * from (
  select 
    date_trunc('month', date) as months, 
    symbol,
    sum(total) as total_volumes,
    sum(total_tokens) as total_volumes_tokens,
    row_number() over (partition by months order by total_volumes desc) as nomor
  from (
  select * from data_token
  union all
  select * from data_token_eth ) ee group by 1, 2 ) ff where nomor = 1
)
select * from data_tokens



SELECT node_address
, slash_points
, reason
, block_timestamp
, COUNT(1) AS n
FROM midgard.slash_points
GROUP BY 1, 2, 3, 4
HAVING n > 1

SELECT chain, COUNT(1) AS n
, MIN(block_timestamp) as mn
FROM BRONZE_MIDGARD_2_6_9_20220405.MIDGARD_ADD_EVENTS
GROUP BY 1



SELECT COUNT(1) AS N
, MAX(block_timestamp) as mx
, MAX(block_timestamp) as mx2
FROM THORCHAIN.THORNAME_CHANGE_EVENTS


SELECT *
FROM solana.core.dim_nft_metadata
WHERE mint = '6XxjKYFbcndh2gDcsUrmZgVEsoDxXMnfsaGY6fpTJzNr'



SELECT *
FROM solana.core.fact_nft_sales
WHERE tx_id IN (
  'vsqqEKqud1LEC5zX7FuXwHmaqpza6tC6vtig8iWyuaJkfAUUFtpgvmdMVenrBJKyc7c9QdbpMs1WKDJNPtELfcV'
  , '52MBuWHddd23EaokX4LgbQvor15HfDv3pBPvrTL8txWTewRdh6oZmZZyaVHCLbE4fvSTBJZWza7z4sp1kKMvJw5F'
)

SELECT marketplace
, seller
, COUNT(1) AS n
FROM solana.core.fact_nft_sales
WHERE marketplace = 'solport'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 100


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

git remote set-url origin https://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@github.com/kblumberg/props-program.git

9zQABKCHpQaHhbA


SELECT *
FROM solana.core.fact_nft_sales
WHERE seller = 'CCWxahZcw47EsH8pavCeNKeGbpGNEs5JEKQ17fjP4ehA'
ORDER BY block_timestamp DESC



SELECT marketplace
, seller
, COUNT(1) AS n
FROM solana.core.fact_nft_sales
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 100

SELECT s.block_timestamp
, s.block_timestamp::date AS date
, s.marketplace
, s.tx_id
, s.mint
, s.sales_amount
, m.token_metadata:"Attribute Count"::string AS "Attribute Count"
, m.token_metadata:"Background"::string AS "Background"
, m.token_metadata:"Body"::string AS "Body"
, m.token_metadata:"Clothing"::string AS "Clothing"
, m.token_metadata:"Eyes"::string AS "Eyes"
, m.token_metadata:"Glasses"::string AS "Glasses"
, m.token_metadata:"Hair Hole"::string AS "Hair Hole"
, m.token_metadata:"Hat"::string AS "Hat"
, m.token_metadata:"Helmet"::string AS "Helmet"
, m.token_metadata:"Mouth"::string AS "Mouth"
, m.token_metadata:"Ski Mask"::string AS "Ski Mask"
FROM solana.core.fact_nft_sales s
JOIN solana.core.dim_nft_metadata m ON m.mint = s.mint
WHERE m.project_name ilike 'catalina whale%'
AND sales_amount > 0



SELECT s.block_timestamp
, s.block_timestamp::date AS date
, s.marketplace
, s.tx_id
, s.mint
, s.sales_amount
, m.token_metadata:"Accessories"::string AS "Accessories"
, m.token_metadata:"Attribute Count"::string AS "Attribute Count"
, m.token_metadata:"Background"::string AS "Background"
, m.token_metadata:"Big Drip"::string AS "Big Drip"
, m.token_metadata:"Body"::string AS "Body"
, m.token_metadata:"Coat"::string AS "Coat"
, m.token_metadata:"Eyes"::string AS "Eyes"
, m.token_metadata:"Head"::string AS "Head"
, m.token_metadata:"Mouth"::string AS "Mouth"
, m.token_metadata:"Shoe"::string AS "Shoe"
FROM solana.core.fact_nft_sales s
JOIN solana.core.dim_nft_metadata m ON m.mint = s.mint
WHERE m.project_name = 'Bubblegoose Ballers'
AND sales_amount > 0
ORDER BY block_timestamp DESC


With labeled_table as
(Select distinct m.mint, m.purchaser,Solana.core.dim_labels.LABEL,
 m.mint_price,m.BLOCK_TIMESTAMP,m.mint_currency
From Solana.core.fact_nft_mints m
INNER JOIN solana.core.dim_labels on m.MINT = solana.core.dim_labels.ADDRESS
where m.mint_currency = 'So11111111111111111111111111111111111111111'
and m.mint_price > 0.1 ),

person as(Select *
from solana.core.fact_nft_mints
order by purchaser),

firstsale as(
Select Distinct mint,sales_amount,block_timestamp
From (SELECT RANK() OVER (PARTITION BY mint order by block_timestamp) 
      as RN,mint,sales_amount,block_TIMESTAMP //as date 
      from solana.core.fact_nft_sales WHERE sales_amount > 0) as ST
Where ST.RN = 1
),



purchaser_table as(Select person.purchaser 
,sum(cast(firstsale.sales_amount as decimal(12,3)) - cast(person.mint_price as decimal(12,3))) as profit_sol
from person
Inner join firstsale on person.mint = firstsale.mint
group by purchaser
order by profit_sol desc
limit 100),

ayo as (Select moon.purchaser as purchaser, count(distinct moon.label) as collection_number
from labeled_table moon
group by purchaser
order by collection_number desc)



select ayo.purchaser,ayo.collection_number, purchaser_table.profit_sol
from ayo
inner join purchaser_table on ayo.purchaser = purchaser_table.purchaser
order by profit_sol desc


SELECT *
FROM solana.core.fact_transfers
WHERE tx_to_address = '9zky78suDwh8t8mPbBCP4nr2Ew9pH2e1kLJMvMGDFT6L'
OR tx_from_address = '9zky78suDwh8t8mPbBCP4nr2Ew9pH2e1kLJMvMGDFT6L'


WITH base AS (
  SELECT instructions[0]:parsed:info:source::string AS source
  , instructions[0]:parsed:info:destination::string AS destination
  , instructions[0]:parsed:info:lamports AS lamports
  FROM solana.core.fact_transactions
  WHERE tx_id = '5R1cRC8KUgPPDnZz19bbNBJvw6Me1THPTEkDVYLYQRPN97ptrtKVnne1SgFRccH6Go4EKD9bdkobjgYbRkdgSkwZ'
  AND instructions[0]:programId = '11111111111111111111111111111111'
  AND instructions[0]:parsed:type = 'transfer'
  AND ARRAY_SIZE(instructions) = 1
)





With person as(Select *
from solana.core.fact_nft_mints
where mint_price > 0.1
order by purchaser),
--gets a specific person and all thier transactions from the mint table



firstsale as(
Select mint,sales_amount,block_timestamp
From (SELECT RANK() OVER (PARTITION BY mint order by block_timestamp) 
      as RN,mint,sales_amount,block_TIMESTAMP //as date 
      from solana.core.fact_nft_sales) as ST
Where ST.RN = 1
)
--need to find the first transaction for each mint address



--this gets the person and than the first sales of the nft by mint and than finds it based on the mint adress of purchaser
Select person.purchaser 
,sum(cast(firstsale.sales_amount as decimal(12,3)) - cast(person.mint_price as decimal(12,3))) as profit_sol
from person
Inner join firstsale on person.mint = firstsale.mint
group by purchaser
order by profit_sol desc



SELECT ROUND(mint_price, 3) AS mp, COUNT(1) AS n
, COUNT(1) AS nn
FROM solana.dim_labels
JOIN solana.fact_nft_mints m ON LOWER(m.mint) = LOWER(l.address)
WHERE project_name = 'taiyo oil'
GROUP BY 1



WITH a AS (
  SELECT DISTINCT LOWER(mint) AS mint
  FROM solana.fact_nft_sales s
), b AS (
  SELECT l.label
  , SUM(s.mint_price) AS mint_volume
  , SUM(s.mint_price) / COUNT(DISTINCT s.mint) AS mint_price
  FROM solana.fact_nft_mints s
  JOIN solana.dim_labels l ON LOWER(l.address) = LOWER(s.mint)
  WHERE mint_currency = 'So11111111111111111111111111111111111111111'
  AND block_timestamp >= '2022-03-01'
  -- data issue, need to look into this
  AND NOT l.label IN ( 'degen trash pandas','mindfolk owls','bored ape solana club' )
  GROUP BY 1
), c AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY mint_volume DESC) AS mint_volume_rank
  FROM b
), d AS (
  SELECT l.label
  , SUM(s.sales_amount) AS sales_volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON LOWER(l.address) = LOWER(s.mint)
  WHERE block_timestamp >= '2022-03-01'
  GROUP BY 1
), e AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY sales_volume DESC) AS sales_volume_rank
  FROM d
), base AS (
  SELECT CASE WHEN c.label = 'taiyo oil' AND ROUND(m.mint_price, 1) <> 1.0 THEN 'Taiyo Infants' ELSE INITCAP(c.label) END AS collection
  , c.mint_volume
  , c.mint_volume_rank
  , e.sales_volume
  , e.sales_volume_rank
  , c.mint_price
  , COUNT(DISTINCT m.mint) AS n_mints
  , COUNT(DISTINCT a.mint) AS n_sell
  , 100 * (1 - (n_sell / n_mints)) AS pct_hold
  FROM solana.fact_nft_mints m
  JOIN solana.dim_labels l ON LOWER(l.address) = LOWER(m.mint)
  JOIN c ON c.label = l.label
  JOIN e ON e.label = l.label
  LEFT JOIN a ON LOWER(a.mint) = LOWER(m.mint)
  GROUP BY 1, 2, 3, 4, 5, 6
  ORDER BY sales_volume_rank
  LIMIT 70
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY pct_hold DESC) AS rn
  FROM base
)
SELECT *
, CASE WHEN rn < 10 THEN CONCAT('0', rn::varchar) ELSE rn::varchar END AS clean_rn
, CONCAT(clean_rn, '. ', collection) AS label
, CASE WHEN mint_price < 0.1 THEN 'Airdrop' ELSE 'Mint' END AS owner_type
FROM b2 
-- WHERE sales_volume_rank <= 70
ORDER BY label


Updates / Accomplishments
Now have 98% of ME sales labeled!
Twitter thread showing off how awesome our Solana NFT data is
Have all the THORChain tables except for transfers de-duped!
I ordered a Saga Phone!
Problems Encountered
Nothing major.
Priorities
Automate Solana NFT data pipeline
Get those collections added to NFT deal score
Concerns



SELECT INITCAP(l.label) AS collection
, SUM(s.sales_volume) AS volume
, MIN(s.block_timestamp::date) AS date
FROM solana.core.fact_nft_sales s
JOIN solana.core.dim_labels l ON l.addrss = s.mint
GROUP BY 1
ORDER BY 2 DESC



WITH a AS (
  SELECT DISTINCT LOWER(mint) AS mint
  FROM solana.fact_nft_sales s
), b AS (
  SELECT l.label
  , SUM(s.mint_price) AS mint_volume
  , SUM(s.mint_price) / COUNT(DISTINCT s.mint) AS mint_price
  FROM solana.fact_nft_mints s
  JOIN solana.dim_labels l ON LOWER(l.address) = LOWER(s.mint)
  WHERE mint_currency = 'So11111111111111111111111111111111111111111'
  AND block_timestamp >= '2022-03-01'
  -- data issue, need to look into this
  AND NOT l.label IN ( 'degen trash pandas','mindfolk owls','bored ape solana club' )
  GROUP BY 1
), c AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY mint_volume DESC) AS mint_volume_rank
  FROM b
), d AS (
  SELECT l.label
  , SUM(s.sales_amount) AS sales_volume
  FROM solana.fact_nft_sales s
  JOIN solana.dim_labels l ON LOWER(l.address) = LOWER(s.mint)
  WHERE block_timestamp >= '2022-03-01'
  GROUP BY 1
), e AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY sales_volume DESC) AS sales_volume_rank
  FROM d
), base AS (
  SELECT CASE WHEN c.label = 'taiyo oil' AND ROUND(m.mint_price, 1) <> 1.0 THEN 'Taiyo Infants' ELSE INITCAP(c.label) END AS collection
  , m.mint
  FROM solana.core.fact_nft_mints m
  JOIN solana.core.dim_labels l ON l.address = m.mint
)
SELECT marketplace
, SUM(s.sales_amount) AS volume
FROM base b 
JOIN solana.core.fact_nft_sales s ON s.mint = b.mint
WHERE b.collection = 'Taiyo Oil'
GROUP BY 1




WITH base AS (
  SELECT INITCAP(l.label) AS collection
  , SUM(COALESCE(s.mint_price, 0)) AS volume
  , MIN(s.block_timestamp::date) AS date
  , COUNT(DISTINCT l.address) AS n_tokens
  , COUNT(DISTINCT s.block_timestamp) AS n_mints
  , MAX(CASE WHEN s.mint IS NULL THEN l.address ELSE '' END) AS mint_example_1
  , MIN(CASE WHEN s.mint IS NULL THEN l.address ELSE '' END) AS mint_example_2
  FROM solana.core.dim_labels l
  LEFT JOIN solana.core.fact_nft_mints s ON LOWER(l.address) = LOWER(s.mint)
  WHERE label_type = 'nft'
  AND l.address IS NOT NULL
  GROUP BY 1
  HAVING n_tokens > 100
  ORDER BY 2 DESC
), b2 AS (
  SELECT INITCAP(l.label) AS collection
  , SUM(COALESCE(s.sales_amount, 0)) AS sales_volume
  FROM solana.core.fact_nft_sales s
  JOIN solana.core.dim_labels l on LOWER(l.address) = LOWER(s.mint)
  GROUP BY 1
  HAVING sales_volume > 1000
  AND MIN(block_timestamp) >= '2022-02-01'
)
SELECT b.*
, b2.sales_volume
, n_mints / n_tokens AS pct
FROM base b
JOIN b2 ON b2.collection = b.collection
ORDER BY pct, sales_volume DESC


SELECT *
FROM solana.core.dim_labels l
-- LEFT JOIN solana.core.fact_nft_mints s ON LOWER(l.address) = LOWER(s.mint)
WHERE label_type = 'nft'
AND l.label ilike 'okay bears'

SELECT DISTINCT address
FROM solana.core.dim_labels l
-- LEFT JOIN solana.core.fact_nft_mints s ON LOWER(l.address) = LOWER(s.mint)
WHERE label_type = 'nft'
AND l.label ilike 'okay bears'


SELECT CASE 
  WHEN tx_id = '2cL9Qj5Xc3FteQimae7qXHzARX1vkZgYavLbutuyt8xUz6Ebep4yfM3r891BrMrm8MEDKggXzyS4kzLWSowWagDP' THEN 'Cyber Samurai Gen 2'
  WHEN tx_id = '2MqHqoWUTRePzicvLiHJmcyPspFXH6nxzZJYRvSURjLjYXJoDKSYB8zgHSj5C2DbXws7fJCpc4HCg5LDh9z8QDxs' THEN 'Astrals'
  WHEN tx_id = '4XAZbakhNWVcxPB8rrCHD5YQat8oNq3n8H92oLjDzeEcDQJp8GCZfWcNJ2pGN5MySATVvoK2PTeHqc7SZYJsopHN' THEN 'Blocksmith Labs'
  WHEN tx_id = '55e6y11uYPso53U2sVct5TCmGZhD7GD3YsueMwspL9TgzndctH2ZdCh5ytZyzcm3XUx172VhiFWa82Lo7hggmQYQ' THEN 'Crypto Cowboy Country'
  WHEN tx_id = '668nNaC5u9zmyqYoyFQJNriaFdD79Tt65XJArwpadPGvWwDNZDZMPGBFLjkY3KTrtp2YBeT9K1b3c4bY6y41T2eC' THEN 'Ghostface Gen 2'
  WHEN tx_id = 'GxXD7S1Qyp2fzBjJ4tKAvrbSKn4bnV9hfcWvLD3xQkFDa7hQ22ov15ePJGScNQz8pd5wAmG23tfPnjBz71f4ieA' THEN 'Tombstoned High Society'
  WHEN tx_id = 'MxnMeosgLe2PvzCc387gK5tLwzBcBjjb8ENvyfuoo6eaQSziuJEhqJCmLNWjcEDdMLDnBwAdeyRyszNTJfqU527' THEN 'Shin Sengoku'
ELSE 'Other' END AS collection
, *
FROM solana.core.transactions
WHERE block_timestamp >= '2022-02-01'
AND tx_id IN (
  '2cL9Qj5Xc3FteQimae7qXHzARX1vkZgYavLbutuyt8xUz6Ebep4yfM3r891BrMrm8MEDKggXzyS4kzLWSowWagDP',
  , '2MqHqoWUTRePzicvLiHJmcyPspFXH6nxzZJYRvSURjLjYXJoDKSYB8zgHSj5C2DbXws7fJCpc4HCg5LDh9z8QDxs',
  , '4XAZbakhNWVcxPB8rrCHD5YQat8oNq3n8H92oLjDzeEcDQJp8GCZfWcNJ2pGN5MySATVvoK2PTeHqc7SZYJsopHN',
  , '55e6y11uYPso53U2sVct5TCmGZhD7GD3YsueMwspL9TgzndctH2ZdCh5ytZyzcm3XUx172VhiFWa82Lo7hggmQYQ',
  , '668nNaC5u9zmyqYoyFQJNriaFdD79Tt65XJArwpadPGvWwDNZDZMPGBFLjkY3KTrtp2YBeT9K1b3c4bY6y41T2eC',
  , 'GxXD7S1Qyp2fzBjJ4tKAvrbSKn4bnV9hfcWvLD3xQkFDa7hQ22ov15ePJGScNQz8pd5wAmG23tfPnjBz71f4ieA',
  , 'MxnMeosgLe2PvzCc387gK5tLwzBcBjjb8ENvyfuoo6eaQSziuJEhqJCmLNWjcEDdMLDnBwAdeyRyszNTJfqU527'
)



-- 8,456
WITH base AS (
  SELECT pool_name
  , from_address
  , SUM(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) AS net_stake_units
  FROM thorchain.liquidity_actions
  GROUP BY 1, 2
  HAVING net_stake_units > 0
)
SELECT COUNT(DISTINCT from_address) AS n
FROM base 

WITH lp1 AS (
  SELECT pool_name
  , from_address
  , block_timestamp::date AS date
  , SUM(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) AS net_stake_units
  FROM thorchain.liquidity_actions
  GROUP BY 1, 2, 3
), lp2 AS (
  SELECT *
  , SUM(net_stake_units) OVER (PARTITION BY pool_name, from_address ORDER BY date) AS cumu_stake_units
  FROM lp1
), lp3 AS (
  SELECT *
  , CASE WHEN net_stake_units > 0 AND cumu_stake_units > 0 AND cumu_stake_units - net_stake_units <= 0 THEN 1
    WHEN net_stake_units < 0 AND cumu_stake_units <= 0 AND cumu_stake_units - net_stake_units > 0 THEN -1
    ELSE 0 END AS net_user_stake_change
  FROM lp2
), lp4 AS (
  SELECT pool_name
  , from_address
  , SUM(net_user_stake_change) AS net_user_stake_change
  FROM lp3
  GROUP BY 1, 2
)
SELECT COUNT(DISTINCT from_address) AS n
, COUNT(1) AS n_lp
FROM lp4
WHERE net_user_stake_change > 0


WITH lp1 AS (
  SELECT pool_name
  , from_address
  , block_timestamp::date AS date
  , SUM(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) AS net_stake_units
  FROM thorchain.liquidity_actions
  GROUP BY 1, 2, 3
), lp2 AS (
  SELECT *
  , SUM(net_stake_units) OVER (PARTITION BY pool_name, from_address ORDER BY date) AS cumu_stake_units
  FROM lp1
), lp3 AS (
  SELECT *
  , CASE WHEN net_stake_units > 0 AND cumu_stake_units > 0 AND cumu_stake_units - net_stake_units <= 0 THEN 1
    WHEN net_stake_units < 0 AND cumu_stake_units <= 0 AND cumu_stake_units - net_stake_units > 0 THEN -1
    ELSE 0 END AS net_user_stake_change
  FROM lp2
), lp4 AS (
  SELECT from_address
  , date
  , SUM(net_user_stake_change) AS daily_net_user_stake_change
  FROM lp3
  GROUP BY 1, 2
  HAVING daily_net_user_stake_change <> 0
), lp5 AS (
  SELECT *
  , SUM(daily_net_user_stake_change) OVER (PARTITION BY from_address ORDER BY date) AS cumu_net_user_stake_change
  FROM lp4
) lp6 AS (
  SELECT date
  , from_address
  , CASE WHEN daily_net_user_stake_change > 0 AND cumu_net_user_stake_change > 0 AND cumu_net_user_stake_change - daily_net_user_stake_change <= 0 THEN 1
    WHEN daily_net_user_stake_change < 0 AND cumu_net_user_stake_change <= 0 AND cumu_net_user_stake_change - daily_net_user_stake_change > 0 THEN -1
    ELSE 0 END AS net_user_stake_change
  FROM lp5
), lp7 AS (
  SELECT date
  , SUM(net_user_stake_change) AS daily_n_lp_change
  FROM lp6
  GROUP BY 1
)
SELECT *
, SUM(daily_n_lp_change) OVER (ORDER BY date) AS cumu_n_lp
FROM lp7

WITH r1 AS (
  SELECT from_address AS address, 
  , block_timestamp::date AS date
  , SUM(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) AS net_stake_units
  FROM thorchain.liquidity_actions
  GROUP BY 1, 2, 3
), lp2 AS (
  SELECT *
  , SUM(net_stake_units) OVER (PARTITION BY pool_name, from_address ORDER BY date) AS cumu_stake_units
  FROM lp1
), lp3 AS (
  SELECT *
  , CASE WHEN net_stake_units > 0 AND cumu_stake_units > 0 AND cumu_stake_units - net_stake_units <= 0 THEN 1
    WHEN net_stake_units < 0 AND cumu_stake_units <= 0 AND cumu_stake_units - net_stake_units > 0 THEN -1
    ELSE 0 END AS net_user_stake_change
  FROM lp2
), lp4 AS (
  SELECT from_address
  , date
  , SUM(net_user_stake_change) AS daily_net_user_stake_change
  FROM lp3
  GROUP BY 1, 2
  HAVING daily_net_user_stake_change <> 0
), lp5 AS (
  SELECT *
  , SUM(daily_net_user_stake_change) OVER (PARTITION BY from_address ORDER BY date) AS cumu_net_user_stake_change
  FROM lp4
) lp6 AS (
  SELECT date
  , from_address
  , CASE WHEN daily_net_user_stake_change > 0 AND cumu_net_user_stake_change > 0 AND cumu_net_user_stake_change - daily_net_user_stake_change <= 0 THEN 1
    WHEN daily_net_user_stake_change < 0 AND cumu_net_user_stake_change <= 0 AND cumu_net_user_stake_change - daily_net_user_stake_change > 0 THEN -1
    ELSE 0 END AS net_user_stake_change
  FROM lp5
), lp7 AS (
  SELECT date
  , SUM(net_user_stake_change) AS daily_n_lp_change
  FROM lp6
  GROUP BY 1
)
SELECT *
, SUM(daily_n_lp_change) OVER (ORDER BY date) AS cumu_n_lp
FROM lp7


SELECT *
FROM crosschain.labels
WHERE blockchain ilike 'thorchain'


SELECT date_trunc('hour', block_timestamp) AS hour
, COUNT(1) AS n
FROM thorchain.swaps
WHERE block_timestamp >= '2022-05-07'
AND block_timestamp >= '2022-05-13'
AND pool_name like 'TERRA%'
GROUP BY 1




with data_sol as (
  select
    instruction:accounts[0]::string as address
    , MIN(block_timestamp) AS wormhole_time
  from
    solana.core.fact_events
  where
  block_timestamp::date >= CURRENT_DATE - 30
  and
    program_id = 'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb'
  and
    instruction:data = '4' -- retrieve from other chains
  and
    (
      inner_instruction:instructions[1]:parsed:type = 'mintTo' -- have careated associated token account
      or
      inner_instruction:instructions[3]:parsed:type = 'mintTo' -- not yet created associated token account
    )
  and
    succeeded = true
  group by 1
)
, data_user_programs as (
select
    b.address,
  case when label is null then COALESCE(program_id, 'None') else label end as labeling,
  ROW_NUMBER() OVER (PARTITION BY b.address ORDER BY block_timestamp) AS rn
from
  data_sol b
  LEFT JOIN 
  solana.core.fact_events a on a.block_timestamp > b.wormhole_time AND 
    b.address IN (
      instruction:accounts[0]
      , instruction:accounts[1]
      , instruction:accounts[2]
      , instruction:accounts[3]
      , instruction:accounts[4]
      , instruction:accounts[5]
      , instruction:accounts[6]
      , instruction:accounts[7]
      , instruction:accounts[8]
      , instruction:accounts[9]
    )
left join solana.core.dim_labels c on a.program_id = c.address
where
  a.block_timestamp::date >= CURRENT_DATE - 30
  and
    succeeded = true
  and
    program_id not in (
  'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb', -- exclude wormhole
  'worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth', -- exclude wormhole
  'DeJBGdMFa1uynnnKiwrVioatTuHmNLpyFKnmB5kaFdzQ', -- Phantom wallet program id for trasnfer https://docs.phantom.app/resources/faq
  '4MNPdKu9wFMvEeZBMt3Eipfs5ovVWTJb31pEXDJAAxX5' -- transfer token program
  ) -- exclude wormhole program id
  -- and
  --  block_timestamp::date >= CURRENT_DATE - 90
  and labeling != 'solana'
-- group by 1, 2 
)
select 
  CASE
    when labeling = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' then 'Magic Eden V2'
    when labeling = '3Katmm9dhvLQijAvomteYMo6rfVbY5NaCRNq9ZBqBgr6' then 'Francium Lend Reward Program'
    when labeling = 'QMNeHCGYnLVDn1icRAfQZpjPLBNkfGbSKRB83G5d8KB' then 'Quarry Protocol'
    when labeling = 'VoLT1mJz1sbnxwq5Fv2SXjdVDgPXrb9tJyC8WpMDkSp' then 'Friktion Protocol'
    when labeling = 'hausS13jsjafwWwGqZTUQRmWyvyxn9EQpqMwV1PBBmk' then 'Opensea'
    when labeling = '2nAAsYdXF3eTQzaeUQS3fr4o782dDg8L28mX39Wr5j8N' then 'lyfRaydiumProgramID'
  else labeling end as label, 
  count(*) as total 
  from data_user_programs
  where label != 'solana' -- remove program id that dedicated to solana
  and rn = 1
  
  group by 1 order by 2 
  desc limit 10


WITH lp1 AS (
  SELECT pool_name
  , from_address
  , block_timestamp::date AS date
  , SUM(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) AS net_stake_units
  FROM thorchain.liquidity_actions
  WHERE pool_name ilike 'terra%'
  GROUP BY 1, 2, 3
), lp2 AS (
  SELECT *
  , SUM(net_stake_units) OVER (PARTITION BY pool_name, from_address ORDER BY date) AS cumu_stake_units
  FROM lp1
), lp3 AS (
  SELECT *
  , CASE WHEN net_stake_units > 0 AND cumu_stake_units > 0 AND cumu_stake_units - net_stake_units <= 0 THEN 1
    WHEN net_stake_units < 0 AND cumu_stake_units <= 0 AND cumu_stake_units - net_stake_units > 0 THEN -1
    ELSE 0 END AS net_user_stake_change
  FROM lp2
), lp4 AS (
  SELECT from_address
  , date
  , SUM(net_user_stake_change) AS daily_net_user_stake_change
  FROM lp3
  GROUP BY 1, 2
  HAVING daily_net_user_stake_change <> 0
), lp5 AS (
  SELECT *
  , SUM(daily_net_user_stake_change) OVER (PARTITION BY from_address ORDER BY date) AS cumu_net_user_stake_change
  FROM lp4
), lp6 AS (
  SELECT date
  , from_address
  , CASE WHEN daily_net_user_stake_change > 0 AND cumu_net_user_stake_change > 0 AND cumu_net_user_stake_change - daily_net_user_stake_change <= 0 THEN 1
    WHEN daily_net_user_stake_change < 0 AND cumu_net_user_stake_change <= 0 AND cumu_net_user_stake_change - daily_net_user_stake_change > 0 THEN -1
    ELSE 0 END AS net_user_stake_change
  FROM lp5
), lp7 AS (
  SELECT date
  , SUM(net_user_stake_change) AS daily_n_lp_change
  FROM lp6
  GROUP BY 1
)
SELECT *
, SUM(daily_n_lp_change) OVER (ORDER BY date) AS cumu_n_lp
FROM lp7




SELECT pool_name, (block_timestamp::date) AS date, COUNT(1) AS n
FROM thorchain.swaps
WHERE pool_name LIKE 'TERRA%'
GROUP BY 1, 2
ORDER BY 2, 1

WITH terra_lplers AS (
  SELECT from_address AS address, sum(rune_amount) AS removed_rune_amount, min(block_timestamp) AS first_removal_time
  FROM flipside_prod_db.thorchain.liquidity_actions
  WHERE (pool_name = 'TERRA.LUNA' OR pool_name = 'TERRA.UST') AND
        lp_action = 'remove_liquidity' AND
      block_timestamp > TO_DATE('2022-05-15')
  GROUP BY address
  HAVING sum(rune_amount) > 0
), lp_addtions AS (
  SELECT from_address, sum(rune_amount) AS added_rune_amount
  FROM flipside_prod_db.thorchain.liquidity_actions
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE lp_action = 'add_liquidity' 
  GROUP BY from_address
),
swaps AS (
  SELECT from_address, sum(from_amount) AS swapped_rune_amount
  FROM flipside_prod_db.thorchain.swaps
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE from_asset = 'THOR.RUNE'
  GROUP BY from_address
),
swappers_and_lplers AS (
  SELECT DISTINCT address
  FROM terra_lplers
  LEFT JOIN lp_addtions ON lp_addtions.from_address = address
  LEFT JOIN swaps ON swaps.from_address = address
  WHERE added_rune_amount > 0 OR swapped_rune_amount > 0
),
tranfers AS (
  SELECT t.from_address, SUM(rune_amount) AS transferred_rune_amount
  FROM flipside_prod_db.thorchain.transfers t
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  LEFT JOIN crosschain.address_labels l ON l.address = t.to_address AND blockchain = 'thorchain'
  WHERE asset = 'THOR.RUNE' AND COALESCE(l.label_type, '') NOT IN ('defi','dex')
  GROUP BY 1
),
base AS (
  SELECT address
  , removed_rune_amount
  , COALESCE(la.added_rune_amount, 0) AS added_rune_amount
  , COALESCE(s.swapped_rune_amount, 0) AS swapped_rune_amount
  , COALESCE(t.transferred_rune_amount, 0) AS transferred_rune_amount
  FROM terra_lplers tl
  LEFT JOIN lp_addtions la ON la.from_address = tl.address
  LEFT JOIN swaps s ON s.from_address = tl.address
  LEFT JOIN tranfers t ON t.from_address = tl.address
), b2 AS (
  SELECT address
  , removed_rune_amount
  , GREATEST(0, removed_rune_amount - added_rune_amount - swapped_rune_amount - transferred_rune_amount) AS kept_amount_2
  , LEAST(b.added_rune_amount, b.removed_rune_amount) AS added_rune_amount_2
  , LEAST(b.removed_rune_amount - added_rune_amount_2, b.swapped_rune_amount) AS swapped_rune_amount_2
  , LEAST(b.removed_rune_amount - added_rune_amount_2 - swapped_rune_amount_2, b.transferred_rune_amount) AS transferred_rune_amount_2
  , CASE
    WHEN kept_amount_2 >= GREATEST(kept_amount_2, added_rune_amount_2, swapped_rune_amount_2, transferred_rune_amount_2) THEN 'Keeping it in their wallet'
    WHEN added_rune_amount_2 >= GREATEST(added_rune_amount_2, swapped_rune_amount_2, transferred_rune_amount_2) THEN 'Added to other pools'
    WHEN swapped_rune_amount_2 >= GREATEST(swapped_rune_amount_2, transferred_rune_amount_2) THEN 'Swapped for other assets'
    ELSE 'Transferred to other addresses' END AS action
  FROM base b
)
SELECT 'Keeping it in their wallet' AS action, SUM(kept_amount_2) AS rune_amount FROM b2 GROUP BY 1
UNION
SELECT 'Added to other pools' AS action, SUM(added_rune_amount_2) AS rune_amount FROM b2 GROUP BY 1
UNION
SELECT 'Swapped for other assets' AS action, SUM(swapped_rune_amount_2) AS rune_amount FROM b2 GROUP BY 1
UNION
SELECT 'Transferred to other addresses' AS action, SUM(transferred_rune_amount_2) AS rune_amount FROM b2 GROUP BY 1

, COUNT(DISTINCT address) AS n2
FROM b2
GROUP BY 1
ORDER BY 2 DESC




WITH terra_lplers AS (
  SELECT from_address AS address, sum(rune_amount) AS removed_rune_amount, min(block_timestamp) AS first_removal_time
  FROM flipside_prod_db.thorchain.liquidity_actions
  WHERE (pool_name = 'TERRA.LUNA' OR pool_name = 'TERRA.UST') AND
        lp_action = 'remove_liquidity' AND
      block_timestamp > TO_DATE('2022-05-15')
  GROUP BY address
  HAVING sum(rune_amount) > 0
), 
lp_addtions AS (
  SELECT from_address, sum(rune_amount) AS added_rune_amount
  FROM flipside_prod_db.thorchain.liquidity_actions
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE lp_action = 'add_liquidity' 
  GROUP BY from_address
),
swaps AS (
  SELECT from_address, sum(from_amount) AS swapped_rune_amount
  FROM flipside_prod_db.thorchain.swaps
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE from_asset = 'THOR.RUNE'
  GROUP BY from_address
),
swappers_and_lplers AS (
  SELECT DISTINCT address
  FROM terra_lplers
  LEFT JOIN lp_addtions ON lp_addtions.from_address = address
  LEFT JOIN swaps ON swaps.from_address = address
  WHERE added_rune_amount > 0 OR swapped_rune_amount > 0
),
tranfers AS (
  SELECT transfers.*, rune_amount AS transferred_rune_amount
  FROM flipside_prod_db.thorchain.transfers
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE asset = 'THOR.RUNE' AND from_address NOT IN (SELECT address FROM swappers_and_lplers) AND to_address != 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' --Reserve
),
movements AS (
  SELECT 'Swapped for other assets' AS action, sum(LEAST(removed_rune_amount, swapped_rune_amount)) AS rune_amount
  FROM terra_lplers
  JOIN swaps ON address = from_address
  UNION 
  SELECT 'Added to other pools' AS action, sum(LEAST(removed_rune_amount, added_rune_amount)) AS rune_amount
  FROM terra_lplers
  JOIN lp_addtions ON address = from_address
  UNION 
  SELECT 'Transferred to other addresses' AS action, sum(LEAST(removed_rune_amount, transferred_rune_amount)) AS rune_amount
  FROM terra_lplers
  JOIN tranfers ON address = from_address
),
movements1 AS (
  SELECT * FROM movements
  UNION 
  SELECT 'Keeping it in their wallet' AS action, 
         ((SELECT sum(removed_rune_amount) AS rune_amount FROM terra_lplers) - (SELECT sum(rune_amount) FROM movements)) AS rune_amount
)

SELECT * FROM movements1



WITH pool_start_date AS (
  SELECT SPLIT(pool_name, '-')[0] AS pool_name, MIN(block_timestamp::date) AS pool_start_date
  FROM thorchain.swaps
  GROUP BY 1
  ORDER BY 2 
), terra_lplers AS (
  SELECT from_address AS address, sum(rune_amount) AS removed_rune_amount, min(block_timestamp) AS first_removal_time
  FROM flipside_prod_db.thorchain.liquidity_actions
  WHERE (pool_name = 'TERRA.LUNA' OR pool_name = 'TERRA.UST') AND
        lp_action = 'remove_liquidity' AND
      block_timestamp > TO_DATE('2022-05-15')
  GROUP BY address
  HAVING sum(rune_amount) > 0
), 
lp_addtions AS (
  SELECT from_address, pool_name, sum(rune_amount) AS added_rune_amount
  FROM flipside_prod_db.thorchain.liquidity_actions
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE lp_action = 'add_liquidity' 
  GROUP BY from_address, pool_name
),
swaps AS (
  SELECT from_address, pool_name, sum(from_amount) AS swapped_rune_amount
  FROM flipside_prod_db.thorchain.swaps
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE from_asset = 'THOR.RUNE'
  GROUP BY from_address, pool_name
),
swappers_and_lplers AS (
  SELECT DISTINCT address
  FROM terra_lplers
  LEFT JOIN lp_addtions ON lp_addtions.from_address = address
  LEFT JOIN swaps ON swaps.from_address = address
  WHERE added_rune_amount > 0 OR swapped_rune_amount > 0
),
tranfers AS (
  SELECT transfers.*, rune_amount AS transferred_rune_amount
  FROM flipside_prod_db.thorchain.transfers
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE asset = 'THOR.RUNE' AND from_address NOT IN (SELECT address FROM swappers_and_lplers) AND to_address != 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' --Reserve
)
, base AS (
SELECT l.address, l.removed_rune_amount, SPLIT(a.pool_name, '-')[0] AS pool_name, a.block_timestamp, a.rune_amount
, SUM(a.rune_amount) OVER (PARTITION BY address ORDER BY a.block_timestamp) AS cumu_rune_amount
FROM terra_lplers l
JOIN flipside_prod_db.thorchain.liquidity_actions a 
  ON a.from_address = l.address 
  AND a.block_timestamp > first_removal_time
  AND a.lp_action = 'add_liquidity' 
), b2 AS (
  SELECT *
  , CASE WHEN cumu_rune_amount < removed_rune_amount THEN rune_amount
    WHEN cumu_rune_amount - rune_amount >= removed_rune_amount THEN 0
    ELSE removed_rune_amount - (cumu_rune_amount - rune_amount) END AS lp_rune_amount
  FROM base
), b3 AS (
  SELECT pool_name
  , SUM(lp_rune_amount) AS rune_amount
  FROM b2 
  GROUP BY 1
), b4 AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY rune_amount DESC) AS rn
  FROM b3
)
SELECT b4.*
, CASE WHEN rn < 10 THEN CONCAT('0', rn::varchar, '. ', b4.pool_name) ELSE CONCAT(rn::varchar, '. ', b4.pool_name) END AS label
, p.pool_start_date
FROM b4
JOIN pool_start_date p ON p.pool_name = b4.pool_name
ORDER BY label



WITH pool_start_date AS (
  SELECT SPLIT(pool_name, '-')[0] AS pool_name, MIN(block_timestamp::date) AS pool_start_date
  FROM thorchain.swaps
  GROUP BY 1
  ORDER BY 2 
), terra_lplers AS (
  SELECT from_address AS address, sum(rune_amount) AS removed_rune_amount, min(block_timestamp) AS first_removal_time
  FROM flipside_prod_db.thorchain.liquidity_actions
  WHERE (pool_name = 'TERRA.LUNA' OR pool_name = 'TERRA.UST') AND
        lp_action = 'remove_liquidity' AND
      block_timestamp > TO_DATE('2022-05-15')
  GROUP BY address
  HAVING sum(rune_amount) > 0
), 
lp_addtions AS (
  SELECT from_address, pool_name, sum(rune_amount) AS added_rune_amount
  FROM flipside_prod_db.thorchain.liquidity_actions
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE lp_action = 'add_liquidity' 
  GROUP BY from_address, pool_name
),
swaps AS (
  SELECT from_address, pool_name, sum(from_amount) AS swapped_rune_amount
  FROM flipside_prod_db.thorchain.swaps
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE from_asset = 'THOR.RUNE'
  GROUP BY from_address, pool_name
),
swappers_and_lplers AS (
  SELECT DISTINCT address
  FROM terra_lplers
  LEFT JOIN lp_addtions ON lp_addtions.from_address = address
  LEFT JOIN swaps ON swaps.from_address = address
  WHERE added_rune_amount > 0 OR swapped_rune_amount > 0
),
tranfers AS (
  SELECT transfers.*, rune_amount AS transferred_rune_amount
  FROM flipside_prod_db.thorchain.transfers
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE asset = 'THOR.RUNE' AND from_address NOT IN (SELECT address FROM swappers_and_lplers) AND to_address != 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' --Reserve
)
, base AS (
SELECT sum(LEAST(removed_rune_amount, added_rune_amount)) AS rune_amount, SPLIT(pool_name, '-')[0] AS pool_name
FROM terra_lplers
JOIN lp_addtions ON address = from_address
GROUP BY pool_name
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER (ORDER BY rune_amount DESC) AS rn
  FROM base
)
SELECT b2.*
, CASE WHEN rn < 10 THEN CONCAT('0', rn::varchar, '. ', b2.pool_name) ELSE CONCAT(rn::varchar, '. ', b2.pool_name) END AS label
, p.pool_start_date
FROM b2
JOIN pool_start_date p ON p.pool_name = b2.pool_name
ORDER BY label



WITH terra_lplers AS (
  SELECT from_address AS address, sum(rune_amount) AS removed_rune_amount, min(block_timestamp) AS first_removal_time
  FROM flipside_prod_db.thorchain.liquidity_actions
  WHERE (pool_name = 'TERRA.LUNA' OR pool_name = 'TERRA.UST') AND
        lp_action = 'remove_liquidity' AND
      block_timestamp > TO_DATE('2022-05-15')
  GROUP BY address
  HAVING sum(rune_amount) > 0
), 
lp_addtions AS (
  SELECT from_address, sum(rune_amount) AS added_rune_amount
  FROM flipside_prod_db.thorchain.liquidity_actions
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE lp_action = 'add_liquidity' 
  GROUP BY from_address
),
swaps AS (
  SELECT from_address, sum(from_amount) AS swapped_rune_amount
  FROM flipside_prod_db.thorchain.swaps
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE from_asset = 'THOR.RUNE'
  GROUP BY from_address
),
swappers_and_lplers AS (
  SELECT DISTINCT address
  FROM terra_lplers
  LEFT JOIN lp_addtions ON lp_addtions.from_address = address
  LEFT JOIN swaps ON swaps.from_address = address
  WHERE added_rune_amount > 0 OR swapped_rune_amount > 0
),
tranfers AS (
  SELECT transfers.*, rune_amount AS transferred_rune_amount
  FROM flipside_prod_db.thorchain.transfers
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE asset = 'THOR.RUNE' AND from_address NOT IN (SELECT address FROM swappers_and_lplers) AND to_address != 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' --Reserve
),
movements AS (
  SELECT 'Swapped for other assets' AS action, sum(LEAST(removed_rune_amount, swapped_rune_amount)) AS rune_amount
  FROM terra_lplers
  JOIN swaps ON address = from_address
  UNION 
  SELECT 'Added to other pools' AS action, sum(LEAST(removed_rune_amount, added_rune_amount)) AS rune_amount
  FROM terra_lplers
  JOIN lp_addtions ON address = from_address
  UNION 
  SELECT 'Transferred to other addresses' AS action, sum(LEAST(removed_rune_amount, transferred_rune_amount)) AS rune_amount
  FROM terra_lplers
  JOIN tranfers ON address = from_address
),
movements1 AS (
  SELECT * FROM movements
  UNION 
  SELECT 'Keeping it in their wallet' AS action, 
         ((SELECT sum(removed_rune_amount) AS rune_amount FROM terra_lplers) - (SELECT sum(rune_amount) FROM movements)) AS rune_amount
)

SELECT * FROM movements1



WITH terra_lplers AS (
  SELECT from_address AS address, sum(rune_amount) AS removed_rune_amount, min(block_timestamp) AS first_removal_time
  FROM flipside_prod_db.thorchain.liquidity_actions
  WHERE (pool_name = 'TERRA.LUNA' OR pool_name = 'TERRA.UST') AND
        lp_action = 'remove_liquidity' AND
      block_timestamp > TO_DATE('2022-05-15')
  GROUP BY address
  HAVING sum(rune_amount) > 0
), 
lp_addtions AS (
  SELECT from_address, pool_name, sum(rune_amount) AS added_rune_amount
  FROM flipside_prod_db.thorchain.liquidity_actions
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE lp_action = 'add_liquidity' 
  GROUP BY from_address, pool_name
),
swaps AS (
  SELECT from_address, pool_name, sum(from_amount) AS swapped_rune_amount
  FROM flipside_prod_db.thorchain.swaps
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE from_asset = 'THOR.RUNE'
  GROUP BY from_address, pool_name
),
swappers_and_lplers AS (
  SELECT DISTINCT address
  FROM terra_lplers
  LEFT JOIN lp_addtions ON lp_addtions.from_address = address
  LEFT JOIN swaps ON swaps.from_address = address
  WHERE added_rune_amount > 0 OR swapped_rune_amount > 0
),
tranfers AS (
  SELECT transfers.*, rune_amount AS transferred_rune_amount
  FROM flipside_prod_db.thorchain.transfers
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE asset = 'THOR.RUNE' AND from_address NOT IN (SELECT address FROM swappers_and_lplers) AND to_address != 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' --Reserve
)
, base AS (
SELECT sum(LEAST(removed_rune_amount, added_rune_amount)) AS rune_amount, SPLIT(pool_name, '-')[0] AS pool_name
FROM terra_lplers
JOIN lp_addtions ON address = from_address
GROUP BY pool_name
), b2 AS (
  SELECT *
  , ROW_NUMBER() OVER ORDER BY rune_amount DESC AS rn
  FROM base
)
SELECT *
, CASE WHEN rn < 10 THEN CONCAT('0', rn::varchar, '. ', pool_name) ELSE CONCAT(rn::varchar, '. ', pool_name) END AS label
FROM b2
ORDER BY label


SELECT *
, ROW_NUMBER()
FROM movements1


WITH terra_lplers AS (
  SELECT from_address AS address, sum(rune_amount) AS removed_rune_amount, min(block_timestamp) AS first_removal_time
  FROM flipside_prod_db.thorchain.liquidity_actions
  WHERE (pool_name = 'TERRA.LUNA' OR pool_name = 'TERRA.UST') AND
        lp_action = 'remove_liquidity' AND
      block_timestamp > TO_DATE('2022-05-15')
  GROUP BY address
  HAVING sum(rune_amount) > 0
), 
lp_addtions AS (
  SELECT from_address, pool_name, sum(rune_amount) AS added_rune_amount
  FROM flipside_prod_db.thorchain.liquidity_actions
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE lp_action = 'add_liquidity' 
  GROUP BY from_address, pool_name
),
swaps AS (
  SELECT from_address, pool_name, sum(from_amount) AS swapped_rune_amount
  FROM flipside_prod_db.thorchain.swaps
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE from_asset = 'THOR.RUNE'
  GROUP BY from_address, pool_name
),
swappers_and_lplers AS (
  SELECT DISTINCT address
  FROM terra_lplers
  LEFT JOIN lp_addtions ON lp_addtions.from_address = address
  LEFT JOIN swaps ON swaps.from_address = address
  WHERE added_rune_amount > 0 OR swapped_rune_amount > 0
),
tranfers AS (
  SELECT transfers.*, rune_amount AS transferred_rune_amount
  FROM flipside_prod_db.thorchain.transfers
  JOIN terra_lplers ON address = from_address AND block_timestamp > first_removal_time
  WHERE asset = 'THOR.RUNE' AND from_address NOT IN (SELECT address FROM swappers_and_lplers) AND to_address != 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt' --Reserve
)

SELECT sum(LEAST(removed_rune_amount, added_rune_amount)) AS rune_amount, pool_name
FROM terra_lplers
JOIN lp_addtions ON address = from_address
GROUP BY pool_name







SELECT *
FROM solana.core.fact_events e
JOIN solana.core.dim_labels l on e.program_id = l.address
WHERE e.block_timestamp >= CURRENT_DATE - 2
AND succeeded
AND l.label ilike 'jup%'
LIMIT 1000



SELECT * FROM movements1



WITH lp1 AS (
  SELECT pool_name
  , from_address
  , block_timestamp::date AS date
  , SUM(CASE WHEN lp_action = 'add_liquidity' THEN stake_units ELSE -stake_units END) AS net_stake_units
  FROM thorchain.liquidity_actions
  GROUP BY 1, 2, 3
), lp2 AS (
  SELECT *
  , SUM(net_stake_units) OVER (PARTITION BY pool_name, from_address ORDER BY date) AS cumu_stake_units
  FROM lp1
), lp3 AS (
  SELECT *
  , CASE WHEN net_stake_units > 0 AND cumu_stake_units > 0 AND cumu_stake_units - net_stake_units <= 0 THEN 1
    WHEN net_stake_units < 0 AND cumu_stake_units <= 0 AND cumu_stake_units - net_stake_units > 0 THEN -1
    ELSE 0 END AS net_user_stake_change
  FROM lp2
), lp4 AS (
  SELECT from_address
  , date
  , SUM(net_user_stake_change) AS net_user_stake_change
  FROM lp3
  GROUP BY 1, 2
), lp5 AS (
  SELECT *
  , SUM(net_user_stake_change) OVER (PARTITION BY from_address ORDER BY date) AS cumu_net_user_stake_change
  FROM lp4
), lp6 AS (
  SELECT date
  , from_address
  , CASE WHEN net_user_stake_change > 0 AND cumu_net_user_stake_change > 0 AND cumu_net_user_stake_change - net_user_stake_change <= 0 THEN 1
    WHEN net_user_stake_change < 0 AND cumu_net_user_stake_change <= 0 AND cumu_net_user_stake_change - net_user_stake_change > 0 THEN -1
    ELSE 0 END AS net_user_stake_change
  FROM lp5
)
SELECT date
, SUM(net_user_stake_change) OVER (ORDER BY date) AS net_user_lps
FROM lp6




WITH base AS (
  SELECT project_name AS collection
  , token_id
  , mint
  , ROW_NUMBER() OVER (PARTITION BY collection, token_id ORDER BY created_at_timestamp DESC) AS rn
  FROM solana.dim_nft_metadata
)
SELECT *
FROM solana.dim_nft_metadata m
JOIN base b ON b.project_name = m.project_name AND b.token_id = m.token_id
ORDER BY m.project_name, m.token_id

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

SELECT day
, rune_price_usd
FROM thorchain.daily_pool_stats
WHERE pool_name LIKE 'BNB.BUSD%'

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