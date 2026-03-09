-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS token_ids_id_seq;

-- Table Definition
CREATE TABLE "public"."token_ids" (
    "id" int4 NOT NULL DEFAULT nextval('token_ids_id_seq'::regclass),
    "token_id" int8 NOT NULL,
    "block_number" int8,
    "block_timestamp" int8,
    "block_timestamp_readable" text,
    "protocol_name" text,
    "position_manager" text,
    "chain_name" text,
    "chain_id" int8,
    "tx_hash" text,
    "extra_data" jsonb,
    "is_burned" bool,
    "is_closed" bool,
    "token_id_owner" text,
    "fetch_timestamp_readable" text,
    PRIMARY KEY ("id")
);


-- Indices
CREATE UNIQUE INDEX unique_tokenid_chainid_manager ON public.token_ids USING btree (token_id, chain_id, position_manager);