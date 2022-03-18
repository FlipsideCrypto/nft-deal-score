


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


SELECT from_asset
, to_asset
, pool_name
, CASE WHEN memo like '%/%' THEN 1 ELSE 0 END AS is_synth
, COUNT(1) AS n
FROM thorchain.swap_events
WHERE block_timestamp >= '2022-03-09'
GROUP BY 1, 2, 3
ORDER BY 4 DESC

cp -R props props-protocol
find props \( -path "*/.git/*" -or -name ".git" \) -delete


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
	from "FLIPSIDE_DEV_DB"."ALGORAND"."APPLICATION_CALL_TRANSACTION" 
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