CREATE OR REPLACE FUNCTION nft.insert_opensea_v2_beta(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

-- Get ERC721 and ERC1155 transfer data for every trade transaction
WITH opensea_erc_union AS (
SELECT DISTINCT
    erc721.evt_tx_hash,
    erc721.evt_index,
    'erc721' as erc_type,
    CAST(erc721."tokenId" AS TEXT) AS "tokenId",
    erc721."from",
    erc721."to",
    erc721.contract_address,
    NULL::numeric AS value
FROM erc721."ERC721_evt_Transfer" erc721
INNER JOIN opensea."WyvernExchange_evt_OrdersMatched" trades ON erc721.evt_tx_hash = trades.evt_tx_hash
WHERE erc721."from" <> '\x0000000000000000000000000000000000000000' -- Exclude mints
AND erc721.evt_block_time >= start_ts
AND erc721.evt_block_time < end_ts
UNION ALL
SELECT DISTINCT
    erc1155.evt_tx_hash,
    erc1155.evt_index,
    'erc1155' as erc_type,
    CAST(erc1155.id AS TEXT) AS "tokenId",
    erc1155."from",
    erc1155."to",
    erc1155.contract_address,
    erc1155.value
FROM erc1155."ERC1155_evt_TransferSingle" erc1155
INNER JOIN opensea."WyvernExchange_evt_OrdersMatched" trades ON erc1155.evt_tx_hash = trades.evt_tx_hash
WHERE erc1155."from" <> '\x0000000000000000000000000000000000000000' -- Exclude mints
AND erc1155.evt_block_time >= start_ts
AND erc1155.evt_block_time < end_ts
),

-- aggregate NFT transfers per transaction 
opensea_erc_subsets AS (SELECT
    evt_tx_hash,
    "tokenId" as token_id,
    array_agg("tokenId") AS token_id_array,
    CASE WHEN erc_type = 'erc1155' AND cardinality(array_agg("tokenId")) > 1 THEN COUNT(DISTINCT "tokenId")
         WHEN erc_type = 'erc1155' AND cardinality(array_agg("tokenId")) = 1 THEN value
         WHEN erc_type = 'erc721'  THEN cardinality(array_agg("tokenId")) END AS no_of_transfers,
    COUNT(DISTINCT "tokenId") as count_token_id,
    array_agg("from") AS from_array,
    array_agg("to") AS to_array,
    array_agg(erc_type) AS erc_type_array,
    array_agg(contract_address) AS nft_contract_addresses_array,
    array_agg(value) AS erc1155_value_array,
    array_agg(evt_index) AS evt_index_array
FROM opensea_erc_union
GROUP BY 1,erc_type,value),


rows AS (
    INSERT INTO nft.trades_v2_beta (
    block_time,
    nft_project_name,
    nft_token_id,
    erc_standard,
    platform,
    platform_version,
    trade_type,
    number_of_items,
    category,
    evt_type,
    aggregator,
    usd_amount,
    seller,
    buyer,
    original_amount,
    original_amount_raw,
    eth_amount,
    royalty_fees_percent,
    original_royalty_fees,
    usd_royalty_fees,
    platform_fees_percent,
    original_platform_fees,
    usd_platform_fees,
    original_currency,
    original_currency_contract,
    currency_contract,
    nft_contract_address,
    exchange_contract_address,
    tx_hash,
    block_number,
    nft_token_ids_array,
    senders_array,
    recipients_array,
    erc_types_array,
    nft_contract_addresses_array,
    erc_values_array,
    evt_index_array,
    tx_from,
    tx_to,
    trace_address,
    evt_index,
    trade_id
    )

SELECT
    DISTINCT trades.evt_block_time AS block_time,
    CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE  tokens.name END AS nft_project_name,
    -- Set NFT token ID to `NULL` if the trade consists of multiple NFT transfers
    CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE erc.token_id END AS nft_token_id,
    -- Set ERC standard to `NULL` if the trade consists of multiple NFT transfers
    CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE COALESCE(erc.erc_type_array[1], tokens.standard) END AS erc_standard,
    trades.platform,
    trades.platform_version,
    CASE WHEN agg.name in ('GenieSwap','Gem') THEN 'Aggregator Trade'
    WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' 
    WHEN erc.erc_type_array[1] = 'erc1155' AND  erc.erc1155_value_array[1] > 1 THEN 'Bundle Trade' 
    ELSE 'Single Item Trade' END AS trade_type,
    erc.no_of_transfers AS number_of_items,
    trades.category,
    trades.evt_type,
    agg.name AS aggregator,
    CASE WHEN erc.no_of_transfers > 1 AND erc.erc_type_array[1] = 'erc721' THEN tx.value / 10 ^ erc20.decimals * p.price 
    WHEN count_token_id > 1 AND erc.erc_type_array[1] = 'erc1155' THEN tx.value / 10 ^ erc20.decimals * p.price
    WHEN count_token_id = 1 AND erc.erc_type_array[1] = 'erc1155' THEN trades.price / 10 ^ erc20.decimals * p.price 
    ELSE trades.price / 10 ^ erc20.decimals * p.price END AS usd_amount,
    
    CASE WHEN erc.no_of_transfers > 1  THEN NULL
    ELSE wc.seller END AS seller,
    
    CASE WHEN erc.no_of_transfers > 1  THEN NULL
    ELSE wc.buyer END AS buyer,
    
    CASE WHEN erc.no_of_transfers > 1 AND erc.erc_type_array[1] = 'erc721' THEN tx.value / 10 ^ erc20.decimals
    WHEN count_token_id > 1 AND erc.erc_type_array[1] = 'erc1155' THEN tx.value / 10 ^ erc20.decimals
    WHEN count_token_id = 1 AND erc.erc_type_array[1] = 'erc1155' THEN trades.price / 10 ^ erc20.decimals 
    ELSE trades.price / 10 ^ erc20.decimals END AS original_amount,

    CASE WHEN erc.no_of_transfers > 1 AND erc.erc_type_array[1] = 'erc721' THEN tx.value
    WHEN count_token_id > 1 AND erc.erc_type_array[1] = 'erc1155' THEN tx.value
    WHEN count_token_id = 1 AND erc.erc_type_array[1] = 'erc1155' THEN trades.price 
    ELSE trades.price END AS original_amount_raw,

    CASE WHEN erc.no_of_transfers > 1 AND erc.erc_type_array[1] = 'erc721' THEN tx.value / 10 ^ erc20.decimals * p.price / peth.price 
    WHEN count_token_id > 1 AND erc.erc_type_array[1] = 'erc1155' THEN tx.value / 10 ^ erc20.decimals * p.price / peth.price
    WHEN count_token_id = 1 AND erc.erc_type_array[1] = 'erc1155' THEN trades.price / 10 ^ erc20.decimals * p.price / peth.price 
    ELSE trades.price / 10 ^ erc20.decimals * p.price / peth.price END AS eth_amount,
     
    CASE WHEN erc.no_of_transfers > 1 THEN NULL::numeric
    ELSE  ROUND(cast((wc.fees - 2.5*(trades.price / 10 ^ erc20.decimals)/100) * (100) / NULLIF(tx.value / 10 ^ erc20.decimals,0) as numeric),7) END AS royalty_fees_percent,
    
    CASE WHEN erc.no_of_transfers > 1 THEN NULL::numeric
    ELSE  ROUND(cast(wc.fees - 2.5*(trades.price / 10 ^ erc20.decimals)/100 as numeric),7) END AS original_royalty_fees,
    
    CASE WHEN erc.no_of_transfers > 1 THEN NULL::numeric
    ELSE ROUND(cast(wc.fees * p.price - 2.5 * (trades.price / 10 ^ erc20.decimals * p.price) / 100 as numeric),7) END AS usd_royalty_fees,
    
    2.5 AS platform_fees_percent,
    
    CASE WHEN erc.no_of_transfers > 1 AND erc.erc_type_array[1] = 'erc721' THEN ROUND(cast(2.5*(tx.value / 10 ^ erc20.decimals)/100 as numeric),7)
    WHEN count_token_id > 1 AND erc.erc_type_array[1] = 'erc1155' THEN ROUND(cast(2.5*(tx.value / 10 ^ erc20.decimals)/100 as numeric),7)
    WHEN count_token_id = 1 AND erc.erc_type_array[1] = 'erc1155' THEN ROUND(cast(2.5*(trades.price / 10 ^ erc20.decimals)/100 as numeric),7)
    ELSE ROUND(cast(2.5*(trades.price / 10 ^ erc20.decimals)/100 as numeric),7) END AS original_platform_fees,
    
    CASE WHEN erc.no_of_transfers > 1 AND erc.erc_type_array[1] = 'erc721' THEN ROUND(cast(2.5*(tx.value / 10 ^ erc20.decimals * p.price)/100 as numeric),7)
    WHEN count_token_id > 1 AND erc.erc_type_array[1] = 'erc1155' THEN ROUND(cast(2.5*(tx.value / 10 ^ erc20.decimals * p.price)/100 as numeric),7)
    WHEN count_token_id = 1 AND erc.erc_type_array[1] = 'erc1155' THEN ROUND(cast(2.5*(trades.price / 10 ^ erc20.decimals * p.price)/100 as numeric),7)
    ELSE ROUND(cast(2.5*(trades.price / 10 ^ erc20.decimals * p.price)/100 as numeric),7) END AS usd_platform_fees, 

    CASE WHEN wc.original_currency_address[1] = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
    wc.original_currency_address[1] AS original_currency_contract,
    wc.currency_token AS currency_contract,
    COALESCE(erc.nft_contract_addresses_array[1], wc.nft_contract_address) AS nft_contract_address,
    trades.contract_address AS exchange_contract_address,
    trades.evt_tx_hash AS tx_hash,
    trades.evt_block_number,
    -- Sometimes multiple NFT transfers occur in a given trade; the 'array' fields below provide info for these use cases 
    erc.token_id_array AS nft_token_ids_array,
    erc.from_array AS senders_array,
    erc.to_array AS recipients_array,
    erc.erc_type_array AS erc_types_array,
    erc.nft_contract_addresses_array AS nft_contract_addresses_array,
    CASE WHEN erc.erc_type_array[1] = 'erc1155' THEN erc.erc1155_value_array 
    ELSE NULL::numeric[] END AS erc_values_array,
    erc.evt_index_array AS evt_index_array,
    tx."from" AS tx_from,
    tx."to" AS tx_to,
    NULL::integer[] as trace_address,
    CASE WHEN agg.name in ('GenieSwap','Gem') THEN NULL::integer
    ELSE trades.evt_index END AS evt_index,
    row_number() OVER (PARTITION BY trades.platform, trades.evt_tx_hash, trades.evt_index, wc.call_trace_address, trades.category ORDER BY trades.platform_version, trades.evt_type) AS trade_id

FROM
    (SELECT 
        DISTINCT 'OpenSea' AS platform,
        '1' AS platform_version,
        'Buy' AS category,
        'Trade' AS evt_type,
        maker,
        evt_block_time,
        price,
        contract_address,
        evt_tx_hash,
        evt_block_number,
        evt_index
        FROM opensea."WyvernExchange_evt_OrdersMatched") trades
INNER JOIN ethereum.transactions tx
    ON trades.evt_tx_hash = tx.hash
    AND tx.block_time >= start_ts
    AND tx.block_time < end_ts
    AND tx.block_number >= start_block
    AND tx.block_number < end_block
LEFT JOIN opensea_erc_subsets erc ON erc.evt_tx_hash = trades.evt_tx_hash
LEFT JOIN nft.wyvern_data wc ON wc.call_tx_hash = trades.evt_tx_hash AND wc.seller = trades.maker
LEFT JOIN nft.tokens tokens ON tokens.contract_address = wc.nft_contract_address
LEFT JOIN nft.aggregators agg ON agg.contract_address = tx."to"
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
    AND p.contract_address = wc.currency_token
    AND p.minute >= start_ts
    AND p.minute < end_ts
LEFT JOIN prices.usd peth ON peth.minute = date_trunc('minute', trades.evt_block_time)
    AND peth.contract_address = wc.currency_token
    AND peth.minute >= start_ts
    AND peth.minute < end_ts
LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
WHERE
    NOT EXISTS (SELECT * -- Exclude OpenSea mint transactions
        FROM erc721."ERC721_evt_Transfer" erc721
        WHERE trades.evt_tx_hash = erc721.evt_tx_hash
        AND erc721."from" = '\x0000000000000000000000000000000000000000')
        AND peth.symbol = 'WETH'
        AND trades.evt_block_time >= start_ts
        AND trades.evt_block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

