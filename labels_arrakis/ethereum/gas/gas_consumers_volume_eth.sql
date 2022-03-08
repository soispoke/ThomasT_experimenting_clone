CREATE OR REPLACE VIEW gas_consumers_volume_eth AS

WITH alltransactions AS
(
SELECT
    from as address,
    'Consumer' AS name,
	block_time,
	success,
	gas_price/10e9 AS gas_price,
	gas_used,
	(gas_price*gas_used)/10e18 AS eth_paid_for_tx,
	hash
FROM ethereum.transactions
),

total AS (
SELECT
'Consumer' AS name,
COUNT(DISTINCT address)::numeric AS total
FROM alltransactions
)

SELECT 
address,
'Ethereum' AS blockchain,
'Gas' AS category,
CASE WHEN ((ROW_NUMBER() OVER(ORDER BY sum(eth_paid_for_tx) DESC))::numeric / total * 100) < 1 THEN  'Top 1% Gas Consumer'
     WHEN ((ROW_NUMBER() OVER(ORDER BY sum(eth_paid_for_tx) DESC))::numeric / total * 100) < 5 THEN  'Top 5% Gas Consumer'
     WHEN ((ROW_NUMBER() OVER(ORDER BY sum(eth_paid_for_tx) DESC))::numeric / total * 100) < 10 THEN 'Top 10% Gas Consumer'
ELSE 'Gas Consumer' END AS name,
'Volume in ETH' AS metric,
cast(NULL AS ARRAY<STRING>) AS details,
sum(eth_paid_for_tx) as quantity,
(ROW_NUMBER() OVER(ORDER BY sum(eth_paid_for_tx) DESC))::numeric / total * 100 as percentile,
ROW_NUMBER() OVER(ORDER BY sum(eth_paid_for_tx) DESC) as rank,
'query' AS source,
'soispoke' AS author
FROM alltransactions
JOIN total on total.name = alltransactions.name
GROUP BY address, total