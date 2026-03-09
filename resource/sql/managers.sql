-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS managers_id_seq;

-- Table Definition
CREATE TABLE "public"."managers" (
    "id" int4 NOT NULL DEFAULT nextval('managers_id_seq'::regclass),
    "manager_account" text NOT NULL,
    "total_active_positions" int4,
    "aero_active_positions" int4,
    "uniswap_active_positions" int4,
    "pancake_active_positions" int4,
    "contract_positions" int4,
    "manual_positions" int4,
    "total_aum" numeric,
    "avg_roi" numeric,
    "is_vx_bot_manager" bool,
    PRIMARY KEY ("id")
);


-- Indices
CREATE UNIQUE INDEX managers_manager_account_key ON public.managers USING btree (manager_account);