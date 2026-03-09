-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS pools_id_seq;

-- Table Definition
CREATE TABLE "public"."pools" (
    "id" int4 NOT NULL DEFAULT nextval('pools_id_seq'::regclass),
    "name" text,
    "track_pool_url" text,
    "type" text,
    "tick_spacing" int8,
    "current_tick" int8,
    "pool_fee" numeric,
    "tvl_eth" numeric,
    "fees_eth" numeric,
    "volume_eth" numeric,
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
    "position_manager" text,
    "collect_address" text,
    "last_updated" int8,
    "last_updated_hour" numeric,
    "pool_address" text NOT NULL,
    "protocol" text,
    "chain_id" int8 NOT NULL,
    "apr" numeric,
    "range_1_reb_day" int4,
    "range_2_reb_day" int4,
    "range_3_reb_day" int4,
    "range_4_reb_day" int4,
    "range_5_reb_day" int4,
    "num_snapshots_used" int4,
    "data_avg_update_hour" numeric,
    "volatility" numeric,
    "max_tick_deviation" int8,
    "tick_std_dev" numeric,
    "pool_tvl_24h" numeric,
    "open_date" text,
    "in_range_liquidity_eth" numeric,
    PRIMARY KEY ("id")
);


-- Indices
CREATE UNIQUE INDEX pools_pool_address_chain_id_key ON public.pools USING btree (pool_address, chain_id);