CREATE OR REPLACE VIEW address_type_eoa_contracts AS 

WITH contracts AS 
(
SELECT DISTINCT
        address,
        'Ethereum'     AS blockchain,
        'Address Type' AS category,
        'Contract'     AS name,
        cast(NULL AS STRING) AS metric,
        cast(NULL AS ARRAY<STRING>) AS details,
        cast(NULL AS BIGINT) AS quantity,
        cast(NULL AS DECIMAL) AS percentile,
        cast(NULL AS BIGINT) AS rank,
        'query'        AS source,
        'soispoke'     AS author
    FROM ethereum.traces
    WHERE type = 'create' AND success = true),
eoa AS 
((SELECT from AS address
  FROM ethereum.traces)
                     UNION
 (SELECT to AS address
  FROM ethereum.traces)) 

select address,
        'Ethereum'     AS blockchain,
        'Address Type' AS category,
        'EOA'          AS name,
        cast(NULL AS STRING) AS metric,
        cast(NULL AS ARRAY<STRING>) AS details,
        cast(NULL AS BIGINT) AS quantity,
        cast(NULL AS DECIMAL) AS percentile,
        cast(NULL AS BIGINT) AS rank,
        'query'        AS source,
        'soispoke'     AS author
    FROM eoa 
        WHERE address NOT IN (SELECT address FROM contracts)
    UNION
    SELECT * FROM contracts