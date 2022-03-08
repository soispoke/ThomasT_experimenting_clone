CREATE OR REPLACE VIEW identity_labels AS

SELECT * FROM identity_funds
UNION ALL
SELECT * FROM identity_exchanges
UNION ALL
SELECT * FROM identity_whales
