-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS vx_bots_id_seq;

-- Table Definition
CREATE TABLE "public"."vx_bots" (
    "id" int4 NOT NULL DEFAULT nextval('vx_bots_id_seq'::regclass),
    "vx_bot_address" text,
    "name" text,
    "range" text,
    "total_duration_hours" numeric,
    "rebalances_amount" int4,
    "est_rebalance_day" numeric,
    "apr" numeric,
    "max_roi" numeric,
    "avg_aum" numeric,
    "pnl" numeric,
    "yield" numeric,
    "aum" numeric,
    "manager_account" text,
    "total_deposited" numeric,
    "total_withdrawn" numeric,
    "total_collected_fees" numeric,
    "total_collected_rewards" numeric,
    "uncollected_fees" numeric,
    "uncollected_rewards" numeric,
    "track_pool_url" text,
    "pool_tvl" numeric,
    "type" text,
    "is_closed" bool,
    "token_id" int8,
    "chain_name" text,
    "chain_id" int8,
    "position_manager" text,
    "collect_address" text,
    "combined_events" jsonb,
    PRIMARY KEY ("id")
);


-- Indices
CREATE UNIQUE INDEX vx_bots_unique_address_chain_manager ON public.vx_bots USING btree (vx_bot_address, chain_id, position_manager);