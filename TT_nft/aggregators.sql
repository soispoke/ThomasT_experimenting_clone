CREATE TABLE IF NOT EXISTS nft.aggregators (
    contract_address bytea PRIMARY KEY,
    name text
);

BEGIN;
TRUNCATE nft.aggregators;

COPY nft.aggregators (contract_address, name) FROM stdin;
\\x0a267cf51ef038fc00e71801f5a524aec06e4f07	GenieSwap
\\x0000000031F7382A812C64b604dA4Fc520AfeF4b	Gem
\.

COMMIT;
CREATE INDEX IF NOT EXISTS nft_aggregators_contract_address_name_idx ON nft.aggregators (contract_address, name);
