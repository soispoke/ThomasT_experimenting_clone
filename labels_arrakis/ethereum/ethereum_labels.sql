CREATE OR REPLACE VIEW ethereum_labels AS

SELECT * FROM address_type_labels
UNION ALL
SELECT * FROM gas_labels
UNION ALL
SELECT * FROM identity_labels