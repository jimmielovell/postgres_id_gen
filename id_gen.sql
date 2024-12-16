-- Constants table for configuration
CREATE TABLE IF NOT EXISTS t_id_generator_config (
    shard_id int NOT NULL CHECK (shard_id BETWEEN 0 AND 31),  -- 5 bits max
    epoch_ms bigint NOT NULL,
    sequence_bits int NOT NULL DEFAULT 10,  -- 10 bits = 1024 sequences
    shard_bits int NOT NULL DEFAULT 5,      -- 5 bits = 32 shards (servers)
    CONSTRAINT single_row CHECK (shard_id IS NOT NULL),  -- Ensure only one row
    CONSTRAINT valid_bits CHECK (sequence_bits + shard_bits <= 23)  -- Ensure we don't exceed bits
);

-- Default configuration
INSERT INTO t_id_generator_config (shard_id, epoch_ms)
VALUES (1, 1704067200000)  -- Jan 1, 2024
ON CONFLICT DO NOTHING;

-- Sequence with larger cache for better performance
CREATE SEQUENCE IF NOT EXISTS table_id_seq
    AS bigint
    START WITH 1000
    CACHE 100;

CREATE OR REPLACE FUNCTION next_id(OUT result bigint) AS $$
DECLARE
    config RECORD;
    seq_id bigint;
    now_millis bigint;
    max_sequence int;
BEGIN
    -- Get configuration
    SELECT * INTO config FROM t_id_generator_config LIMIT 1;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ID generator configuration not found';
    END IF;

    -- Calculate maximum sequence value
    max_sequence := (1 << config.sequence_bits) - 1;

    -- Get sequence ID with wraparound
    SELECT nextval('table_id_seq') % (max_sequence + 1) INTO seq_id;

    -- Get current timestamp with microsecond precision
    SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;

    -- Validate timestamp
    IF now_millis < config.epoch_ms THEN
        RAISE EXCEPTION 'Clock moved backwards. Waiting until % ms', config.epoch_ms;
    END IF;

    -- Calculate available timestamp bits (63 - sequence_bits - shard_bits)
    -- 63 bits because bigint is signed
    result := (now_millis - config.epoch_ms) << (config.sequence_bits + config.shard_bits);

    -- Add shard ID
    result := result | (config.shard_id << config.sequence_bits);

    -- Add sequence number
    result := result | seq_id;

    -- Ensure result is positive
    IF result < 0 THEN
        RAISE EXCEPTION 'Generated ID is negative: %', result;
    END IF;
EXCEPTION
    WHEN numeric_value_out_of_range THEN
        RAISE EXCEPTION 'ID generation failed: numeric overflow';
END;
$$ LANGUAGE PLPGSQL;

-- Function to extract timestamp from ID
CREATE OR REPLACE FUNCTION extract_timestamp_from_id(id bigint)
RETURNS timestamptz AS $$
DECLARE
    config RECORD;
    timestamp_ms bigint;
BEGIN
    SELECT * INTO config FROM t_id_generator_config LIMIT 1;

    timestamp_ms := (id >> (config.sequence_bits + config.shard_bits)) + config.epoch_ms;

    RETURN to_timestamp(timestamp_ms::double precision / 1000);
END;
$$ LANGUAGE PLPGSQL;

-- Function to extract shard from ID
CREATE OR REPLACE FUNCTION extract_shard_from_id(id bigint)
RETURNS int AS $$
DECLARE
    config RECORD;
BEGIN
    SELECT * INTO config FROM t_id_generator_config LIMIT 1;

    RETURN (id >> config.sequence_bits) & ((1 << config.shard_bits) - 1);
END;
$$ LANGUAGE PLPGSQL;

-- Function to extract sequence from ID
CREATE OR REPLACE FUNCTION extract_sequence_from_id(id bigint)
RETURNS int AS $$
DECLARE
    config RECORD;
BEGIN
    SELECT * INTO config FROM t_id_generator_config LIMIT 1;

    RETURN id & ((1 << config.sequence_bits) - 1);
END;
$$ LANGUAGE PLPGSQL;
