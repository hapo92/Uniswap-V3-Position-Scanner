-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS pool_snapshots_id_seq;

-- Table Definition
CREATE TABLE "public"."pool_snapshots" (
    "id" int4 NOT NULL DEFAULT nextval('pool_snapshots_id_seq'::regclass),
    "track_pool_url" text,
    "name" text,
    "type" text,
    "tick_spacing" int8,
    "current_tick" int8,
    "pool_fee" numeric,
    "pool_tvl" numeric,
    "pool_tvl0" numeric,
    "pool_tvl1" numeric,
    "fee_growth_global_0_x128" numeric,
    "fee_growth_global_1_x128" numeric,
    "pool_active_liquidity" numeric,
    "liquidity" numeric,
    "token0" text,
    "token1" text,
    "symbol_t0" text,
    "symbol_t1" text,
    "decimals_t0" int4,
    "decimals_t1" int4,
    "token0_price_eth" numeric,
    "token1_price_eth" numeric,
    "reward_token" text,
    "reward_token_symbol" text,
    "reward_token_price_eth" numeric,
    "number_of_active_positions" int4,
    "last_snapshot_timestamp" int8,
    "chain_id" int8 NOT NULL,
    "protocol" text,
    "position_manager" text,
    "collect_address" text,
    "pool_address" text,
    "fees_interval_eth" numeric,
    "volume_interval_eth" numeric,
    "interval_seconds" int8,
    "yield_growth" numeric,
    "nominal_liquidity_eth" numeric,
    "in_range_liquidity_eth" numeric,
    "token_id" int8,
    "nominal_yield_eth" numeric,
    PRIMARY KEY ("id")
);


-- Indices
CREATE UNIQUE INDEX idx_pool_snapshots_addr_chain_time ON public.pool_snapshots USING btree (pool_address, chain_id, last_snapshot_timestamp);
CREATE UNIQUE INDEX idx_pool_snapshots_addr_chain_tokenid_time ON public.pool_snapshots USING btree (pool_address, chain_id, token_id, last_snapshot_timestamp);