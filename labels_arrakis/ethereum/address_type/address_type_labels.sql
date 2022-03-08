CREATE OR REPLACE VIEW address_type_labels AS

SELECT * FROM address_type_funds
UNION ALL
SELECT * FROM address_type_exchanges
UNION ALL
SELECT * FROM address_type_whales
UNION ALL
SELECT * FROM address_type_eoa_contracts