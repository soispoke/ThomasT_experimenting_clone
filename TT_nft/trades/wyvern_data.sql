CREATE SCHEMA IF NOT EXISTS nft;

DROP TABLE nft.wyvern_data;
CREATE TABLE IF NOT EXISTS nft.wyvern_data (
    call_tx_hash bytea,
    nft_contract_address bytea,
    currency_token bytea,
    buyer bytea,
    seller bytea,
    call_trace_address varchar,
    original_currency_address bytea[],
    fees numeric,
    PRIMARY KEY (call_tx_hash, seller)
);
