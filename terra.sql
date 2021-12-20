
AND additional_metadata:reservation_id = '4144'

WITH ancient_traits1 AS (
	SELECT block_timestamp,
	block_id,
	tx_id,
	msg_value:execute_msg:mint:extension:name::string                 AS name,
	msg_value:execute_msg:mint:token_id::float                        AS tokenid,
	msg_value:execute_msg:mint:extension:attributes[0]:value::string  AS rarity,
	msg_value:execute_msg:mint:extension:attributes[1]:value::string  AS rank,
	msg_value:execute_msg:mint:extension:attributes[2]:value::string  AS dust_volume,
	msg_value:execute_msg:mint:extension:attributes[3]:value::string  AS spirit_level,
	msg_value:execute_msg:mint:extension:attributes[4]:value::string  AS origin,
	msg_value:execute_msg:mint:extension:attributes[5]:value::string  AS bottling_date,
	msg_value:execute_msg:mint:extension:attributes[6]:value::string  AS essence,
	msg_value:execute_msg:mint:extension:attributes[7]:value::string  AS rune,
	msg_value:execute_msg:mint:extension:attributes[8]:value::string  AS infusion,
	msg_value:execute_msg:mint:extension:attributes[9]:value::string  AS ancient_gem,
	msg_value:execute_msg:mint:extension:attributes[10]:value::string AS rare_gem,
	msg_value:execute_msg:mint:extension:attributes[11]:value::string AS common_gem,
	msg_value:execute_msg:mint:extension:attributes[12]:value::string AS weight,
	null                                                              AS legendary_composition,
	msg_value:execute_msg:mint:extension:attributes[13]:value::string AS ancient_composition,
	msg_value:execute_msg:mint:extension:attributes[14]:value::string AS rare_composition,
	msg_value:execute_msg:mint:extension:attributes[15]:value::string AS common_composition,
	msg_value:execute_msg:mint:extension:attributes[16]:value::string AS shower,
	msg_value:execute_msg:mint:extension:attributes[17]:value::string AS meteor_id
	FROM   terra.msgs 
	WHERE  msg_value:contract::string = 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' 
	AND    msg_value:sender::string = 'terra1awy9ychm2z2hd696kz6yeq67l30l7nxs7n762t'
	AND    msg_value:execute_msg:mint is not null
	AND    tx_status = 'SUCCEEDED'
	AND    rarity = 'Ancient'
	AND    msg_value:execute_msg:mint:extension:attributes[13]:trait_type::string <> 'Legendary Composition'
), ancient_traits2 AS (
	SELECT block_timestamp,
	block_id,
	tx_id,
	msg_value:execute_msg:mint:extension:name::string                 AS name,
	msg_value:execute_msg:mint:token_id::float                        AS tokenid,
	msg_value:execute_msg:mint:extension:attributes[0]:value::string  AS rarity,
	msg_value:execute_msg:mint:extension:attributes[1]:value::string  AS rank,
	msg_value:execute_msg:mint:extension:attributes[2]:value::string  AS dust_volume,
	msg_value:execute_msg:mint:extension:attributes[3]:value::string  AS spirit_level,
	msg_value:execute_msg:mint:extension:attributes[4]:value::string  AS origin,
	msg_value:execute_msg:mint:extension:attributes[5]:value::string  AS bottling_date,
	msg_value:execute_msg:mint:extension:attributes[6]:value::string  AS essence,
	msg_value:execute_msg:mint:extension:attributes[7]:value::string  AS rune,
	msg_value:execute_msg:mint:extension:attributes[8]:value::string  AS infusion,
	msg_value:execute_msg:mint:extension:attributes[9]:value::string  AS ancient_gem,
	msg_value:execute_msg:mint:extension:attributes[10]:value::string AS rare_gem,
	msg_value:execute_msg:mint:extension:attributes[11]:value::string AS common_gem,
	msg_value:execute_msg:mint:extension:attributes[12]:value::string AS weight,
	msg_value:execute_msg:mint:extension:attributes[13]:value::string AS legendary_composition,
	msg_value:execute_msg:mint:extension:attributes[14]:value::string AS ancient_composition,
	msg_value:execute_msg:mint:extension:attributes[15]:value::string AS rare_composition,
	msg_value:execute_msg:mint:extension:attributes[16]:value::string AS common_composition,
	msg_value:execute_msg:mint:extension:attributes[17]:value::string AS shower,
	msg_value:execute_msg:mint:extension:attributes[18]:value::string AS meteor_id
	FROM   terra.msgs 
	WHERE  msg_value:contract::string = 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' 
	AND    msg_value:sender::string = 'terra1awy9ychm2z2hd696kz6yeq67l30l7nxs7n762t'
	AND    msg_value:execute_msg:mint is not null
	AND    tx_status = 'SUCCEEDED'
	AND    rarity = 'Ancient'
	AND    msg_value:execute_msg:mint:extension:attributes[13]:trait_type::string = 'Legendary Composition'
), rare_traits1 AS (
	SELECT block_timestamp,
	block_id,
	tx_id,
	msg_value:execute_msg:mint:extension:name::string                 AS name,
	msg_value:execute_msg:mint:token_id::float                        AS tokenid,
	msg_value:execute_msg:mint:extension:attributes[0]:value::string  AS rarity,
	msg_value:execute_msg:mint:extension:attributes[1]:value::string  AS rank,
	msg_value:execute_msg:mint:extension:attributes[2]:value::string  AS dust_volume,
	msg_value:execute_msg:mint:extension:attributes[3]:value::string  AS spirit_level,
	msg_value:execute_msg:mint:extension:attributes[4]:value::string  AS origin,
	msg_value:execute_msg:mint:extension:attributes[5]:value::string  AS bottling_date,
	msg_value:execute_msg:mint:extension:attributes[6]:value::string  AS essence,
	msg_value:execute_msg:mint:extension:attributes[7]:value::string  AS rune,
	msg_value:execute_msg:mint:extension:attributes[8]:value::string  AS infusion,
	null                                                              AS ancient_gem,
	msg_value:execute_msg:mint:extension:attributes[9]:value::string  AS rare_gem,
	msg_value:execute_msg:mint:extension:attributes[10]:value::string AS common_gem,
	msg_value:execute_msg:mint:extension:attributes[11]:value::string AS weight,
	msg_value:execute_msg:mint:extension:attributes[12]:value::string AS legendary_composition,
	null                                                              AS ancient_composition,
	msg_value:execute_msg:mint:extension:attributes[13]:value::string AS rare_composition,
	msg_value:execute_msg:mint:extension:attributes[14]:value::string AS common_composition,
	msg_value:execute_msg:mint:extension:attributes[15]:value::string AS shower,
	msg_value:execute_msg:mint:extension:attributes[16]:value::string AS meteor_id
	FROM   terra.msgs 
	WHERE  msg_value:contract::string = 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' 
	AND    msg_value:sender::string = 'terra1awy9ychm2z2hd696kz6yeq67l30l7nxs7n762t'
	AND    msg_value:execute_msg:mint is not null
	AND    tx_status = 'SUCCEEDED'
	AND    rarity = 'Rare'
	AND    msg_value:execute_msg:mint:extension:attributes[12]:trait_type::string = 'Legendary Composition'
), rare_traits2 AS (
	SELECT block_timestamp,
	block_id,
	tx_id,
	msg_value:execute_msg:mint:extension:name::string                 AS name,
	msg_value:execute_msg:mint:token_id::float                        AS tokenid,
	msg_value:execute_msg:mint:extension:attributes[0]:value::string  AS rarity,
	msg_value:execute_msg:mint:extension:attributes[1]:value::string  AS rank,
	msg_value:execute_msg:mint:extension:attributes[2]:value::string  AS dust_volume,
	msg_value:execute_msg:mint:extension:attributes[3]:value::string  AS spirit_level,
	msg_value:execute_msg:mint:extension:attributes[4]:value::string  AS origin,
	msg_value:execute_msg:mint:extension:attributes[5]:value::string  AS bottling_date,
	msg_value:execute_msg:mint:extension:attributes[6]:value::string  AS essence,
	msg_value:execute_msg:mint:extension:attributes[7]:value::string  AS rune,
	msg_value:execute_msg:mint:extension:attributes[8]:value::string  AS infusion,
	null                                                              AS ancient_gem,
	msg_value:execute_msg:mint:extension:attributes[9]:value::string  AS rare_gem,
	msg_value:execute_msg:mint:extension:attributes[10]:value::string AS common_gem,
	msg_value:execute_msg:mint:extension:attributes[11]:value::string AS weight,
	null                                                              AS legendary_composition,
	null                                                              AS ancient_composition,
	msg_value:execute_msg:mint:extension:attributes[12]:value::string AS rare_composition,
	msg_value:execute_msg:mint:extension:attributes[13]:value::string AS common_composition,
	msg_value:execute_msg:mint:extension:attributes[14]:value::string AS shower,
	msg_value:execute_msg:mint:extension:attributes[15]:value::string AS meteor_id
	FROM   terra.msgs 
	WHERE  msg_value:contract::string = 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' 
	AND    msg_value:sender::string = 'terra1awy9ychm2z2hd696kz6yeq67l30l7nxs7n762t'
	AND    msg_value:execute_msg:mint is not null
	AND    tx_status = 'SUCCEEDED'
	AND    rarity = 'Rare'
	AND    msg_value:execute_msg:mint:extension:attributes[12]:trait_type::string <> 'Legendary Composition'
), common_traits1 AS (
	SELECT block_timestamp,
	block_id,
	tx_id,
	msg_value:execute_msg:mint:extension:name::string                 AS name,
	msg_value:execute_msg:mint:token_id::float                        AS tokenid,
	msg_value:execute_msg:mint:extension:attributes[0]:value::string  AS rarity,
	msg_value:execute_msg:mint:extension:attributes[1]:value::string  AS rank,
	msg_value:execute_msg:mint:extension:attributes[2]:value::string  AS dust_volume,
	msg_value:execute_msg:mint:extension:attributes[3]:value::string  AS spirit_level,
	msg_value:execute_msg:mint:extension:attributes[4]:value::string  AS origin,
	msg_value:execute_msg:mint:extension:attributes[5]:value::string  AS bottling_date,
	msg_value:execute_msg:mint:extension:attributes[6]:value::string  AS essence,
	msg_value:execute_msg:mint:extension:attributes[7]:value::string  AS rune,
	msg_value:execute_msg:mint:extension:attributes[8]:value::string  AS infusion,
	null                                                              AS ancient_gem,
	null                                                              AS rare_gem,
	msg_value:execute_msg:mint:extension:attributes[9]:value::string  AS common_gem,
	msg_value:execute_msg:mint:extension:attributes[10]:value::string AS weight,
	msg_value:execute_msg:mint:extension:attributes[11]:value::string AS legendary_composition,
	null                                                              AS ancient_composition,
	null                                                              AS rare_composition,
	msg_value:execute_msg:mint:extension:attributes[12]:value::string AS common_composition,
	msg_value:execute_msg:mint:extension:attributes[13]:value::string AS shower,
	msg_value:execute_msg:mint:extension:attributes[14]:value::string AS meteor_id
	FROM   terra.msgs 
	WHERE  msg_value:contract::string = 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' 
	AND    msg_value:sender::string = 'terra1awy9ychm2z2hd696kz6yeq67l30l7nxs7n762t'
	AND    msg_value:execute_msg:mint is not null
	AND    tx_status = 'SUCCEEDED'
	AND    rarity = 'Common'
	AND    msg_value:execute_msg:mint:extension:attributes[11]:trait_type::string = 'Legendary Composition'
), common_traits2 AS (
	SELECT block_timestamp,
	block_id,
	tx_id,
	msg_value:execute_msg:mint:extension:name::string                 AS name,
	msg_value:execute_msg:mint:token_id::float                        AS tokenid,
	msg_value:execute_msg:mint:extension:attributes[0]:value::string  AS rarity,
	msg_value:execute_msg:mint:extension:attributes[1]:value::string  AS rank,
	msg_value:execute_msg:mint:extension:attributes[2]:value::string  AS dust_volume,
	msg_value:execute_msg:mint:extension:attributes[3]:value::string  AS spirit_level,
	msg_value:execute_msg:mint:extension:attributes[4]:value::string  AS origin,
	msg_value:execute_msg:mint:extension:attributes[5]:value::string  AS bottling_date,
	msg_value:execute_msg:mint:extension:attributes[6]:value::string  AS essence,
	msg_value:execute_msg:mint:extension:attributes[7]:value::string  AS rune,
	msg_value:execute_msg:mint:extension:attributes[8]:value::string  AS infusion,
	null                                                              AS ancient_gem,
	null                                                              AS rare_gem,
	msg_value:execute_msg:mint:extension:attributes[9]:value::string  AS common_gem,
	msg_value:execute_msg:mint:extension:attributes[10]:value::string AS weight,
	null                                                              AS legendary_composition,
	null                                                              AS ancient_composition,
	null                                                              AS rare_composition,
	msg_value:execute_msg:mint:extension:attributes[11]:value::string AS common_composition,
	msg_value:execute_msg:mint:extension:attributes[12]:value::string AS shower,
	msg_value:execute_msg:mint:extension:attributes[13]:value::string AS meteor_id
	FROM   terra.msgs 
	WHERE  msg_value:contract::string = 'terra1p70x7jkqhf37qa7qm4v23g4u4g8ka4ktxudxa7' 
	AND    msg_value:sender::string = 'terra1awy9ychm2z2hd696kz6yeq67l30l7nxs7n762t'
	AND    msg_value:execute_msg:mint is not null
	AND    tx_status = 'SUCCEEDED'
	AND    rarity = 'Common'
	AND    msg_value:execute_msg:mint:extension:attributes[11]:trait_type::string <> 'Legendary Composition'
), combine AS (
	SELECT * FROM ancient_traits1
	UNION ALL
	SELECT * FROM ancient_traits2
	UNION ALL
	SELECT * FROM rare_traits1
	UNION ALL
	SELECT * FROM rare_traits2
	UNION ALL
	SELECT * FROM common_traits1
	UNION ALL
	SELECT * FROM common_traits2
)
SELECT * FROM combine order by tokenid

