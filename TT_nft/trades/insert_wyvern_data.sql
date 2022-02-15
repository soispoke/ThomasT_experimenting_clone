CREATE OR REPLACE FUNCTION nft.insert_wyvern_data(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH wyvern_calldata AS (SELECT 
        DISTINCT call_tx_hash,
        addrs [5] AS nft_contract_address,
        CASE -- Replace `ETH` with `WETH` for ERC20 lookup later
            WHEN addrs [7] = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE addrs [7]
        END AS currency_token,
        addrs [2] AS buyer,
        addrs [9] AS seller,
        CASE WHEN call_trace_address[1] IS NULL THEN ARRAY[3]::varchar
             ELSE call_trace_address::varchar END AS call_trace_address,
        array_agg(DISTINCT addrs [7]) AS original_currency_address
    FROM
        opensea."WyvernExchange_call_atomicMatch_"
    WHERE
        "call_success"
        AND call_block_time >= start_ts
        AND call_block_time < end_ts
    GROUP BY 1,2,3,4,5,6),
    
royalty_fees as (SELECT  
CASE WHEN trace_address[1] = 3 THEN '{3}'
    ELSE format('%s%s', LEFT(trace_address::varchar,4), '}') END AS trace_address,
tx_hash,
CASE WHEN trace_address::varchar like '%,%,3}%' THEN value/10^18
     WHEN trace_address::varchar like'{3}%' THEN value/10^18 END AS fees,
traces."from",
traces."to"
FROM ethereum.traces
WHERE traces.to = '\x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
AND traces.block_time >= start_ts
AND traces.block_time < end_ts),

rows AS (
    INSERT INTO nft.wyvern_data (
        call_tx_hash,
        nft_contract_address,
        currency_token,
        buyer,
        seller,
        call_trace_address,
        original_currency_address,
        fees
        )
    SELECT 
        call_tx_hash,
        nft_contract_address,
        currency_token,
        buyer,
        seller,
        call_trace_address,
        original_currency_address,
        fees
    FROM wyvern_calldata wc 
    JOIN royalty_fees rf ON rf.tx_hash = wc.call_tx_hash AND rf.trace_address = wc.call_trace_address
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

