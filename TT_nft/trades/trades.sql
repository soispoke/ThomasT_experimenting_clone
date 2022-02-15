CREATE SCHEMA IF NOT EXISTS nft;

DROP TABLE nft.trades_v2_beta;
CREATE TABLE IF NOT EXISTS nft.trades_v2_beta (
    block_time timestamptz NOT NULL,
    nft_project_name text,
    nft_token_id text,
    erc_standard text,
    platform text NOT NULL,
    platform_version text,
    trade_type text,
    number_of_items integer,
    category text,
    evt_type text,
    aggregator text,
    usd_amount numeric,
    seller bytea,
    buyer bytea,
    original_amount numeric,
    original_amount_raw numeric,
    eth_amount numeric,
    royalty_fees_percent numeric,
    original_royalty_fees numeric,
    usd_royalty_fees numeric,
    platform_fees_percent numeric,
    original_platform_fees numeric,
    usd_platform_fees numeric,
    original_currency text,
    original_currency_contract bytea,
    currency_contract bytea,
    nft_contract_address bytea NOT NULL,
    exchange_contract_address bytea NOT NULL,
    tx_hash bytea NOT NULL,
    block_number integer,
    nft_token_ids_array text[],
    senders_array bytea[],
    recipients_array bytea[],
    erc_types_array text[],
    nft_contract_addresses_array bytea[],
    erc_values_array numeric[],
    evt_index_array integer[],
    tx_from bytea NOT NULL,
    tx_to bytea,
    trace_address integer[],
    evt_index integer,
    trade_id integer
);

CREATE UNIQUE INDEX IF NOT EXISTS nft_v2b_trades_platform_tx_hash_evt_index_trade_id_uniq_idx ON nft.trades_v2_beta (platform, tx_hash, evt_index, trade_id);
CREATE UNIQUE INDEX IF NOT EXISTS nft_v2b_trades_platform_tx_hash_trace_address_trade_id_uniq_idx ON nft.trades_v2_beta (platform, tx_hash, trace_address, trade_id);
CREATE INDEX IF NOT EXISTS nft_v2b_trades_block_time_idx ON nft.trades_v2_beta USING BRIN (block_time);
CREATE INDEX IF NOT EXISTS nft_v2b_trades_seller_idx ON nft.trades_v2_beta (seller);
CREATE INDEX IF NOT EXISTS nft_v2b_trades_buyer_idx ON nft.trades_v2_beta (buyer);
CREATE INDEX IF NOT EXISTS nft_v2b_trades_nft_project_name_nft_token_id_block_time_idx ON nft.trades_v2_beta (nft_project_name, nft_token_id, block_time);
CREATE INDEX IF NOT EXISTS nft_v2b_trades_block_time_platform_seller_buyer_nft_project_name_nft_token_id_idx ON nft.trades_v2_beta (block_time, platform, seller, buyer, nft_project_name, nft_token_id);
CREATE INDEX IF NOT EXISTS nft_v2b_trades_nft_token_ids_array_idx ON nft.trades_v2_beta USING GIN(nft_token_ids_array);
CREATE INDEX IF NOT EXISTS nft_v2b_trades_nft_contract_addresses_array_idx ON nft.trades_v2_beta USING GIN(nft_contract_addresses_array);
CREATE INDEX IF NOT EXISTS nft_v2b_trades_senders_array_idx ON nft.trades_v2_beta USING GIN(senders_array);
CREATE INDEX IF NOT EXISTS nft_v2b_trades_recipients_array_idx ON nft.trades_v2_beta USING GIN(recipients_array);
