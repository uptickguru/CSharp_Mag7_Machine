-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

DO $$
DECLARE
  catalog_version TEXT;
BEGIN
  SELECT value INTO catalog_version FROM _timescaledb_catalog.metadata WHERE key='timescaledb_version' AND value <> '2.10.2';
  IF FOUND THEN
    RAISE EXCEPTION 'catalog version mismatch, expected "%" seen "%"', '2.10.2', catalog_version;
  END IF;
END$$;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file is always prepended to all upgrade and downgrade scripts.
-- This file must avoid referencing extension objects directly as that
-- would limit the things we can alter in extension update/downgrade
-- itself.
SET LOCAL search_path TO pg_catalog, pg_temp;

-- Disable parallel execution for the duration of the update process.
-- This avoids version mismatch errors that would have beeen triggered by the
-- parallel workers in ts_extension_check_version().
SET LOCAL max_parallel_workers = 0;

-- Triggers should be disabled during upgrades to avoid having them
-- invoke functions that might load an old version of the shared
-- library before those functions have been updated.
DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_command_end;
DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_sql_drop;

-- Since we want to call the new version of restart_background_workers we
-- create a function that points to that version. The proper restart_background_workers
-- may either be in _timescaledb_internal or in _timescaledb_functions
-- depending on the version we are upgrading from and we can't make
-- the move in this location as the new schema might not have been set up.
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_namespace WHERE nspname='_timescaledb_functions') THEN
    CREATE FUNCTION _timescaledb_functions._tmp_restart_background_workers() RETURNS BOOL
    AS '$libdir/timescaledb', 'ts_bgw_db_workers_restart' LANGUAGE C VOLATILE;
    PERFORM _timescaledb_functions._tmp_restart_background_workers();
    DROP FUNCTION _timescaledb_functions._tmp_restart_background_workers();
  ELSE
    -- timescaledb < 2.11 does not have _timescaledb_functions schema
    CREATE FUNCTION _timescaledb_internal._tmp_restart_background_workers() RETURNS BOOL
    AS '$libdir/timescaledb', 'ts_bgw_db_workers_restart' LANGUAGE C VOLATILE;
    PERFORM _timescaledb_internal._tmp_restart_background_workers();
    DROP FUNCTION _timescaledb_internal._tmp_restart_background_workers();
  END IF;
END
$$;

-- Table for ACL and initprivs of tables.
CREATE TABLE _timescaledb_internal.saved_privs(
       tmpnsp name,
       tmpname name,
       tmpacl aclitem[],
       tmpini aclitem[],
       UNIQUE (tmpnsp, tmpname));

-- We save away both the ACL and the initprivs for all tables and
-- views in the extension (but not for chunks and internal objects) so
-- that we can restore them to the proper state after the update.
INSERT INTO _timescaledb_internal.saved_privs
SELECT nspname, relname, relacl, initprivs
  FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace
                   JOIN pg_init_privs ip ON ip.objoid = cl.oid AND ip.objsubid = 0 AND ip.classoid = 'pg_class'::regclass
WHERE
  nspname IN ('_timescaledb_catalog', '_timescaledb_config')
  OR (
    relname IN ('hypertable_chunk_local_size', 'compressed_chunk_stats', 'bgw_job_stat', 'bgw_policy_chunk_stats')
    AND nspname = '_timescaledb_internal'
  )
;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file is always prepended to all upgrade scripts.

CREATE TABLE _timescaledb_catalog.continuous_aggs_watermark (
  mat_hypertable_id integer NOT NULL,
  watermark bigint NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_watermark_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_watermark TO PUBLIC;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_watermark', '');

CREATE FUNCTION _timescaledb_internal.cagg_watermark_materialized(hypertable_id INTEGER)
RETURNS INT8 AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_watermark_materialized' LANGUAGE C STABLE STRICT PARALLEL SAFE;
CREATE FUNCTION _timescaledb_internal.recompress_chunk_segmentwise(REGCLASS, BOOLEAN) RETURNS REGCLASS
AS '$libdir/timescaledb-2.20.3', 'ts_recompress_chunk_segmentwise' LANGUAGE C STRICT VOLATILE;
CREATE FUNCTION _timescaledb_internal.get_compressed_chunk_index_for_recompression(REGCLASS) RETURNS REGCLASS
AS '$libdir/timescaledb-2.20.3', 'ts_get_compressed_chunk_index_for_recompression' LANGUAGE C STRICT VOLATILE;

DROP FUNCTION _timescaledb_internal.dimension_is_finite;
DROP FUNCTION _timescaledb_internal.dimension_slice_get_constraint_sql;

CREATE SCHEMA _timescaledb_functions;
GRANT USAGE ON SCHEMA _timescaledb_functions TO PUBLIC;

-- migrate histogram support functions into _timescaledb_functions schema
ALTER FUNCTION _timescaledb_internal.hist_sfunc (state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_combinefunc(state1 INTERNAL, state2 INTERNAL) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_serializefunc(INTERNAL) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_deserializefunc(bytea, INTERNAL) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_finalfunc(state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER) SET SCHEMA _timescaledb_functions;

-- migrate first/last support functions into _timescaledb_functions schema
ALTER FUNCTION _timescaledb_internal.first_sfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.first_combinefunc(internal, internal) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.last_sfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.last_combinefunc(internal, internal) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.bookend_finalfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.bookend_serializefunc(internal) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.bookend_deserializefunc(bytea, internal) SET SCHEMA _timescaledb_functions;

DROP FUNCTION IF EXISTS _timescaledb_internal.is_main_table(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.is_main_table(name, name);
DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_from_main_table(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.main_table_from_hypertable(integer);
DROP FUNCTION IF EXISTS _timescaledb_internal.time_literal_sql(bigint, regtype);

ALTER FUNCTION _timescaledb_internal.compressed_data_in(CSTRING) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_data_out(_timescaledb_internal.compressed_data) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_data_send(_timescaledb_internal.compressed_data) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_data_recv(internal) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.rxid_in(cstring) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.rxid_out(@extschema@.rxid) SET SCHEMA _timescaledb_functions;

ALTER TABLE _timescaledb_config.bgw_job
    ALTER COLUMN owner SET DEFAULT pg_catalog.quote_ident(current_role)::regrole;

ALTER TABLE _timescaledb_catalog.continuous_agg_migrate_plan
  ADD COLUMN user_view_definition TEXT,
  DROP CONSTRAINT continuous_agg_migrate_plan_mat_hypertable_id_fkey;

-- Log with events that will be sent out with the telemetry. The log
-- will be flushed after it has been sent out. We do not save it to
-- backups since it should not contain important data.
CREATE TABLE _timescaledb_catalog.telemetry_event (
       created timestamptz NOT NULL DEFAULT current_timestamp,
       tag name NOT NULL,
       body jsonb NOT NULL
);

GRANT SELECT ON _timescaledb_catalog.telemetry_event TO PUBLIC;
DROP FUNCTION IF EXISTS @extschema@.alter_job(
    INTEGER,
    INTERVAL,
    INTERVAL,
    INTEGER,
    INTERVAL,
    BOOL,
    JSONB,
    TIMESTAMPTZ,
    BOOL,
    REGPROC
);

CREATE FUNCTION @extschema@.alter_job(
    job_id INTEGER,
    schedule_interval INTERVAL = NULL,
    max_runtime INTERVAL = NULL,
    max_retries INTEGER = NULL,
    retry_period INTERVAL = NULL,
    scheduled BOOL = NULL,
    config JSONB = NULL,
    next_start TIMESTAMPTZ = NULL,
    if_exists BOOL = FALSE,
    check_config REGPROC = NULL,
    fixed_schedule BOOL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT DEFAULT NULL
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB,
next_start TIMESTAMPTZ, check_config TEXT, fixed_schedule BOOL, initial_start TIMESTAMPTZ, timezone TEXT)
AS '$libdir/timescaledb-2.20.3', 'ts_job_alter'
LANGUAGE C VOLATILE;

-- when upgrading from old versions on PG13 this function might not be present
-- since there is no ALTER FUNCTION IF EXISTS we have to work around it with a DO block
DO $$
DECLARE
  foid regprocedure;
  funcs text[] = '{
    drop_dist_ht_invalidation_trigger,
    subtract_integer_from_now,
    get_approx_row_count,
    chunk_status,
    create_chunk,create_chunk_table,
    freeze_chunk,unfreeze_chunk,drop_chunk,
    attach_osm_table_chunk
  }';
BEGIN
  FOR foid IN
    SELECT oid FROM pg_proc WHERE proname = ANY(funcs) AND pronamespace = '_timescaledb_internal'::regnamespace
  LOOP
    EXECUTE format('ALTER FUNCTION %s SET SCHEMA _timescaledb_functions', foid);
  END LOOP;
END;
$$;

DROP FUNCTION IF EXISTS _timescaledb_internal.get_time_type(integer);

ALTER FUNCTION _timescaledb_internal.insert_blocker() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_create_command(name) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.to_unix_microseconds(timestamptz) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.to_timestamp(bigint) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.to_timestamp_without_timezone(bigint) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.to_date(bigint) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.to_interval(bigint) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.interval_to_usec(interval) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.time_to_internal(anyelement) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.set_dist_id(uuid) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.set_peer_dist_id(uuid) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.validate_as_data_node() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.show_connection_cache() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.ping_data_node(name, interval) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.remote_txn_heal_data_node(oid) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.relation_size(regclass) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.data_node_hypertable_info(name, name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.data_node_chunk_info(name, name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hypertable_local_size(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hypertable_remote_size(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.chunks_local_size(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.chunks_remote_size(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.range_value_to_pretty(bigint, regtype) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.data_node_compressed_chunk_stats(name, name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_chunk_local_stats(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_chunk_remote_stats(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.indexes_local_size(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.data_node_index_size(name, name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.indexes_remote_size(name, name, name) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.generate_uuid() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_git_commit() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_os_info() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.tsl_loaded() SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.calculate_chunk_interval(int, bigint, bigint) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.chunks_in(record, integer[]) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.chunk_id_from_relid(oid) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.show_chunk(regclass) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_chunk_relstats(regclass) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_chunk_colstats(regclass) SET SCHEMA _timescaledb_functions;

UPDATE _timescaledb_catalog.hypertable SET chunk_sizing_func_schema = '_timescaledb_functions' WHERE chunk_sizing_func_schema = '_timescaledb_internal' AND chunk_sizing_func_name = 'calculate_chunk_interval';

DO $$
DECLARE
  foid regprocedure;
  kind text;
  funcs text[] = '{
    policy_compression_check,policy_compression_execute,policy_compression,
    policy_job_error_retention_check,policy_job_error_retention,
    policy_recompression,
    policy_refresh_continuous_aggregate_check,policy_refresh_continuous_aggregate,
    policy_reorder_check,policy_reorder,policy_retention_check,policy_retention,

    cagg_watermark, cagg_watermark_materialized,
    cagg_migrate_plan_exists, cagg_migrate_pre_validation, cagg_migrate_create_plan, cagg_migrate_execute_create_new_cagg,
    cagg_migrate_execute_disable_policies, cagg_migrate_execute_enable_policies, cagg_migrate_execute_copy_policies,
    cagg_migrate_execute_refresh_new_cagg, cagg_migrate_execute_copy_data, cagg_migrate_execute_override_cagg,
    cagg_migrate_execute_drop_old_cagg, cagg_migrate_execute_plan,

    finalize_agg,

    hypertable_invalidation_log_delete, invalidation_cagg_log_add_entry, invalidation_hyper_log_add_entry,
    invalidation_process_cagg_log, invalidation_process_hypertable_log, materialization_invalidation_log_delete,

    alter_job_set_hypertable_id,

    set_chunk_default_data_node,

    create_compressed_chunk, get_compressed_chunk_index_for_recompression, recompress_chunk_segmentwise,
    chunk_drop_replica, chunk_index_clone, chunk_index_replace, create_chunk_replica_table, drop_stale_chunks,
		chunk_constraint_add_table_constraint, hypertable_constraint_add_table_fk_constraint,
    health, wait_subscription_sync
  }';
BEGIN
  FOR foid, kind IN
    SELECT oid,
    CASE
      WHEN prokind = 'f' THEN 'FUNCTION'
      WHEN prokind = 'a' THEN 'AGGREGATE'
      ELSE 'PROCEDURE'
    END
    FROM pg_proc WHERE proname = ANY(funcs) AND pronamespace = '_timescaledb_internal'::regnamespace
  LOOP
    EXECUTE format('ALTER %s %s SET SCHEMA _timescaledb_functions', kind, foid);
  END LOOP;
END;
$$;

UPDATE _timescaledb_config.bgw_job SET proc_schema = '_timescaledb_functions' WHERE proc_schema = '_timescaledb_internal';
UPDATE _timescaledb_config.bgw_job SET check_schema = '_timescaledb_functions' WHERE check_schema = '_timescaledb_internal';

ALTER FUNCTION _timescaledb_internal.start_background_workers() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.stop_background_workers() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.restart_background_workers() SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.process_ddl_event() SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.get_partition_for_key(val anyelement) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_partition_hash(val anyelement) SET SCHEMA _timescaledb_functions;

UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = '_timescaledb_functions' WHERE partitioning_func_schema = '_timescaledb_internal' AND partitioning_func IN ('get_partition_for_key','get_partition_hash');

ALTER FUNCTION _timescaledb_internal.finalize_agg_ffunc(internal,text,name,name,name[],bytea,anyelement) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.finalize_agg_sfunc(internal,text,name,name,name[],bytea,anyelement) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.partialize_agg(anyelement) SET SCHEMA _timescaledb_functions;

-- Fix osm chunk ranges
UPDATE _timescaledb_catalog.dimension_slice ds
  SET range_start = 9223372036854775806
FROM _timescaledb_catalog.chunk_constraint cc
INNER JOIN _timescaledb_catalog.chunk c ON c.id = cc.chunk_id AND c.osm_chunk
WHERE cc.dimension_slice_id = ds.id AND ds.range_start <> 9223372036854775806;

-- OSM support - table must be rebuilt to ensure consistent attribute numbers
-- we cannot just ALTER TABLE .. ADD COLUMN
ALTER TABLE _timescaledb_config.bgw_job
    DROP CONSTRAINT bgw_job_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk
    DROP CONSTRAINT chunk_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_index
    DROP CONSTRAINT chunk_index_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_agg
    DROP CONSTRAINT continuous_agg_mat_hypertable_id_fkey,
    DROP CONSTRAINT continuous_agg_raw_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_bucket_function
    DROP CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold
    DROP CONSTRAINT continuous_aggs_invalidation_threshold_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.dimension
    DROP CONSTRAINT dimension_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.hypertable
    DROP CONSTRAINT hypertable_compressed_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.hypertable_compression
    DROP CONSTRAINT hypertable_compression_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.hypertable_data_node
    DROP CONSTRAINT hypertable_data_node_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.tablespace
    DROP CONSTRAINT tablespace_hypertable_id_fkey;

DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.jobs;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS timescaledb_information.dimensions;
DROP VIEW IF EXISTS timescaledb_information.compression_settings;
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;
DROP VIEW IF EXISTS timescaledb_experimental.chunk_replication_status;
DROP VIEW IF EXISTS timescaledb_experimental.policies;

-- recreate table
CREATE TABLE _timescaledb_catalog.hypertable_tmp AS SELECT * FROM _timescaledb_catalog.hypertable;
CREATE TABLE _timescaledb_catalog.tmp_hypertable_seq_value AS SELECT last_value, is_called FROM _timescaledb_catalog.hypertable_id_seq;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.hypertable;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.hypertable_id_seq;

SET timescaledb.restoring = on; -- must disable the hooks otherwise we can't do anything without the table _timescaledb_catalog.hypertable

DROP TABLE _timescaledb_catalog.hypertable;

CREATE SEQUENCE _timescaledb_catalog.hypertable_id_seq MINVALUE 1;
SELECT setval('_timescaledb_catalog.hypertable_id_seq', last_value, is_called) FROM _timescaledb_catalog.tmp_hypertable_seq_value;
DROP TABLE _timescaledb_catalog.tmp_hypertable_seq_value;

CREATE TABLE _timescaledb_catalog.hypertable (
    id INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('_timescaledb_catalog.hypertable_id_seq'),
    schema_name name NOT NULL,
    table_name name NOT NULL,
    associated_schema_name name NOT NULL,
    associated_table_prefix name NOT NULL,
    num_dimensions smallint NOT NULL,
    chunk_sizing_func_schema name NOT NULL,
    chunk_sizing_func_name name NOT NULL,
    chunk_target_size bigint NOT NULL, -- size in bytes
    compression_state smallint NOT NULL DEFAULT 0,
    compressed_hypertable_id integer,
    replication_factor smallint NULL,
    status integer NOT NULL DEFAULT 0
);

SET timescaledb.restoring = off;

INSERT INTO _timescaledb_catalog.hypertable (
    id,
    schema_name,
    table_name,
    associated_schema_name,
    associated_table_prefix,
    num_dimensions,
    chunk_sizing_func_schema,
    chunk_sizing_func_name,
    chunk_target_size,
    compression_state,
    compressed_hypertable_id,
    replication_factor
)
SELECT
    id,
    schema_name,
    table_name,
    associated_schema_name,
    associated_table_prefix,
    num_dimensions,
    chunk_sizing_func_schema,
    chunk_sizing_func_name,
    chunk_target_size,
    compression_state,
    compressed_hypertable_id,
    replication_factor
FROM
    _timescaledb_catalog.hypertable_tmp
ORDER BY id;

UPDATE _timescaledb_catalog.hypertable h
SET status = 3
WHERE EXISTS (
  SELECT FROM _timescaledb_catalog.chunk c WHERE c.osm_chunk AND c.hypertable_id = h.id
);

ALTER SEQUENCE _timescaledb_catalog.hypertable_id_seq OWNED BY _timescaledb_catalog.hypertable.id;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', 'WHERE id >= 1');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_id_seq', '');

GRANT SELECT ON _timescaledb_catalog.hypertable TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.hypertable_id_seq TO PUBLIC;

DROP TABLE _timescaledb_catalog.hypertable_tmp;
-- now add any constraints
ALTER TABLE _timescaledb_catalog.hypertable
    ADD CONSTRAINT hypertable_associated_schema_name_associated_table_prefix_key UNIQUE (associated_schema_name, associated_table_prefix),
    ADD CONSTRAINT hypertable_table_name_schema_name_key UNIQUE (table_name, schema_name),
    ADD CONSTRAINT hypertable_schema_name_check CHECK (schema_name != '_timescaledb_catalog'),
    ADD CONSTRAINT hypertable_dim_compress_check CHECK (num_dimensions > 0 OR compression_state = 2),
    ADD CONSTRAINT hypertable_chunk_target_size_check CHECK (chunk_target_size >= 0),
    ADD CONSTRAINT hypertable_compress_check CHECK ( (compression_state = 0 OR compression_state = 1 )  OR (compression_state = 2 AND compressed_hypertable_id IS NULL)),
    ADD CONSTRAINT hypertable_replication_factor_check CHECK (replication_factor > 0 OR replication_factor = -1),
    ADD CONSTRAINT hypertable_compressed_hypertable_id_fkey FOREIGN KEY (compressed_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id);

GRANT SELECT ON TABLE _timescaledb_catalog.hypertable TO PUBLIC;

-- 3. reestablish constraints on other tables
ALTER TABLE _timescaledb_config.bgw_job
    ADD CONSTRAINT bgw_job_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk
    ADD CONSTRAINT chunk_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id);
ALTER TABLE _timescaledb_catalog.chunk_index
    ADD CONSTRAINT chunk_index_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_agg
    ADD CONSTRAINT continuous_agg_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    ADD CONSTRAINT continuous_agg_raw_hypertable_id_fkey FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_bucket_function
    ADD CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold
    ADD CONSTRAINT continuous_aggs_invalidation_threshold_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.dimension
    ADD CONSTRAINT dimension_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.hypertable_compression
    ADD CONSTRAINT hypertable_compression_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.hypertable_data_node
    ADD CONSTRAINT hypertable_data_node_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id);
ALTER TABLE _timescaledb_catalog.tablespace
    ADD CONSTRAINT tablespace_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
CREATE TYPE _timescaledb_internal.dimension_info;

CREATE OR REPLACE FUNCTION _timescaledb_functions.dimension_info_in(cstring)
    RETURNS _timescaledb_internal.dimension_info
    LANGUAGE C STRICT IMMUTABLE
    AS '$libdir/timescaledb-2.20.3', 'ts_dimension_info_in';

CREATE OR REPLACE FUNCTION _timescaledb_functions.dimension_info_out(_timescaledb_internal.dimension_info)
    RETURNS cstring
    LANGUAGE C STRICT IMMUTABLE
    AS '$libdir/timescaledb-2.20.3', 'ts_dimension_info_out';

CREATE TYPE _timescaledb_internal.dimension_info (
    INPUT = _timescaledb_functions.dimension_info_in,
    OUTPUT = _timescaledb_functions.dimension_info_out,
    INTERNALLENGTH = VARIABLE
);

CREATE FUNCTION @extschema@.create_hypertable(
    relation                REGCLASS,
    dimension               _timescaledb_internal.dimension_info,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    migrate_data            BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, created BOOL) AS '$libdir/timescaledb-2.20.3', 'ts_hypertable_create_general' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.add_dimension(
    hypertable              REGCLASS,
    dimension               _timescaledb_internal.dimension_info,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, created BOOL)
AS '$libdir/timescaledb-2.20.3', 'ts_dimension_add_general' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.set_partitioning_interval(
    hypertable              REGCLASS,
    partition_interval      ANYELEMENT,
    dimension_name          NAME = NULL
) RETURNS VOID AS '$libdir/timescaledb-2.20.3', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.by_hash(column_name NAME, number_partitions INTEGER,
                                    partition_func regproc = NULL)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '$libdir/timescaledb-2.20.3', 'ts_hash_dimension';

CREATE FUNCTION @extschema@.by_range(column_name NAME,
                                     partition_interval ANYELEMENT = NULL::bigint,
                                     partition_func regproc = NULL)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '$libdir/timescaledb-2.20.3', 'ts_range_dimension';

--
-- Rebuild the catalog table `_timescaledb_catalog.chunk` to
-- add new column `creation_time`
--
CREATE TABLE _timescaledb_internal.chunk_tmp
AS SELECT * from _timescaledb_catalog.chunk;

CREATE TABLE _timescaledb_internal.tmp_chunk_seq_value AS
SELECT last_value, is_called FROM _timescaledb_catalog.chunk_id_seq;

--drop foreign keys on chunk table
ALTER TABLE _timescaledb_catalog.chunk_constraint DROP CONSTRAINT
chunk_constraint_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_index DROP CONSTRAINT
chunk_index_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_data_node DROP CONSTRAINT
chunk_data_node_chunk_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats DROP CONSTRAINT
bgw_policy_chunk_stats_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT
compression_chunk_size_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT
compression_chunk_size_compressed_chunk_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_copy_operation DROP CONSTRAINT
chunk_copy_operation_chunk_id_fkey;

--drop dependent views
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;
DROP VIEW IF EXISTS timescaledb_experimental.chunk_replication_status;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_id_seq;
DROP TABLE _timescaledb_catalog.chunk;

CREATE SEQUENCE _timescaledb_catalog.chunk_id_seq MINVALUE 1;

-- now create table without self referential foreign key
CREATE TABLE _timescaledb_catalog.chunk (
  id integer NOT NULL DEFAULT nextval('_timescaledb_catalog.chunk_id_seq'),
  hypertable_id int NOT NULL,
  schema_name name NOT NULL,
  table_name name NOT NULL,
  compressed_chunk_id integer ,
  dropped boolean NOT NULL DEFAULT FALSE,
  status integer NOT NULL DEFAULT 0,
  osm_chunk boolean NOT NULL DEFAULT FALSE,
  creation_time timestamptz,
  -- table constraints
  CONSTRAINT chunk_pkey PRIMARY KEY (id),
  CONSTRAINT chunk_schema_name_table_name_key UNIQUE (schema_name, table_name)
);

INSERT INTO _timescaledb_catalog.chunk
( id, hypertable_id, schema_name, table_name,
  compressed_chunk_id, dropped, status, osm_chunk)
SELECT id, hypertable_id, schema_name, table_name,
  compressed_chunk_id, dropped, status, osm_chunk
FROM _timescaledb_internal.chunk_tmp;

-- update creation_time for chunks
UPDATE
    _timescaledb_catalog.chunk c
SET
    creation_time = (pg_catalog.pg_stat_file(pg_catalog.pg_relation_filepath(r.oid))).modification
FROM
    pg_class r, pg_namespace n
WHERE
    r.relnamespace = n.oid
    AND r.relname = c.table_name
    AND n.nspname = c.schema_name
    AND r.relkind = 'r'
    AND c.dropped IS FALSE;

-- Make sure that there are no record with empty creation time
UPDATE _timescaledb_catalog.chunk SET creation_time = now() WHERE creation_time IS NULL;

--add indexes to the chunk table
CREATE INDEX chunk_hypertable_id_idx ON _timescaledb_catalog.chunk (hypertable_id);
CREATE INDEX chunk_compressed_chunk_id_idx ON _timescaledb_catalog.chunk (compressed_chunk_id);
CREATE INDEX chunk_osm_chunk_idx ON _timescaledb_catalog.chunk (osm_chunk, hypertable_id);
CREATE INDEX chunk_hypertable_id_creation_time_idx ON _timescaledb_catalog.chunk(hypertable_id, creation_time);

ALTER SEQUENCE _timescaledb_catalog.chunk_id_seq OWNED BY _timescaledb_catalog.chunk.id;
SELECT setval('_timescaledb_catalog.chunk_id_seq', last_value, is_called) FROM _timescaledb_internal.tmp_chunk_seq_value;

-- add self referential foreign key
ALTER TABLE _timescaledb_catalog.chunk ADD CONSTRAINT chunk_compressed_chunk_id_fkey FOREIGN KEY ( compressed_chunk_id )
 REFERENCES _timescaledb_catalog.chunk( id );

--add foreign key constraint
ALTER TABLE _timescaledb_catalog.chunk
      ADD CONSTRAINT chunk_hypertable_id_fkey
      FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_id_seq', '');

-- Add non-null constraint
ALTER TABLE _timescaledb_catalog.chunk
  ALTER COLUMN creation_time SET NOT NULL;

--add the foreign key constraints
ALTER TABLE _timescaledb_catalog.chunk_constraint ADD CONSTRAINT
chunk_constraint_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id);
ALTER TABLE _timescaledb_catalog.chunk_index ADD CONSTRAINT
chunk_index_chunk_id_fkey FOREIGN KEY (chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk_data_node ADD CONSTRAINT
chunk_data_node_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk(id);
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats ADD CONSTRAINT
bgw_policy_chunk_stats_chunk_id_fkey FOREIGN KEY (chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT
compression_chunk_size_chunk_id_fkey FOREIGN KEY (chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT
compression_chunk_size_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id)
REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk_copy_operation ADD CONSTRAINT
chunk_copy_operation_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE;

--cleanup
DROP TABLE _timescaledb_internal.chunk_tmp;
DROP TABLE _timescaledb_internal.tmp_chunk_seq_value;

GRANT SELECT ON _timescaledb_catalog.chunk_id_seq TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk TO PUBLIC;
-- end recreate _timescaledb_catalog.chunk table --

--
-- Rebuild the catalog table `_timescaledb_catalog.compression_chunk_size` to
-- add new column `numrows_frozen_immediately`
--
CREATE TABLE _timescaledb_internal.compression_chunk_size_tmp
    AS SELECT * from _timescaledb_catalog.compression_chunk_size;

-- Drop depended views
-- We assume that '_timescaledb_internal.compressed_chunk_stats' was already dropped in this update
-- (see above)

-- Drop table
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.compression_chunk_size;
DROP TABLE _timescaledb_catalog.compression_chunk_size;

CREATE TABLE _timescaledb_catalog.compression_chunk_size (
  chunk_id integer NOT NULL,
  compressed_chunk_id integer NOT NULL,
  uncompressed_heap_size bigint NOT NULL,
  uncompressed_toast_size bigint NOT NULL,
  uncompressed_index_size bigint NOT NULL,
  compressed_heap_size bigint NOT NULL,
  compressed_toast_size bigint NOT NULL,
  compressed_index_size bigint NOT NULL,
  numrows_pre_compression bigint,
  numrows_post_compression bigint,
  numrows_frozen_immediately bigint,
  -- table constraints
  CONSTRAINT compression_chunk_size_pkey PRIMARY KEY (chunk_id),
  CONSTRAINT compression_chunk_size_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  CONSTRAINT compression_chunk_size_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.compression_chunk_size
(chunk_id, compressed_chunk_id, uncompressed_heap_size, uncompressed_toast_size,
  uncompressed_index_size, compressed_heap_size, compressed_toast_size,
  compressed_index_size, numrows_pre_compression, numrows_post_compression, numrows_frozen_immediately)
SELECT chunk_id, compressed_chunk_id, uncompressed_heap_size, uncompressed_toast_size,
  uncompressed_index_size, compressed_heap_size, compressed_toast_size,
  compressed_index_size, numrows_pre_compression, numrows_post_compression, 0
FROM _timescaledb_internal.compression_chunk_size_tmp;

DROP TABLE _timescaledb_internal.compression_chunk_size_tmp;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_chunk_size', '');

GRANT SELECT ON _timescaledb_catalog.compression_chunk_size TO PUBLIC;

-- End modify `_timescaledb_catalog.compression_chunk_size`

DROP FUNCTION @extschema@.drop_chunks(REGCLASS, "any", "any", BOOL);
CREATE FUNCTION @extschema@.drop_chunks(
     relation               REGCLASS,
     older_than             "any" = NULL,
     newer_than             "any" = NULL,
     verbose                BOOLEAN = FALSE,
     created_before         "any" = NULL,
     created_after          "any" = NULL
 ) RETURNS SETOF TEXT AS '$libdir/timescaledb-2.20.3', 'ts_chunk_drop_chunks'
 LANGUAGE C VOLATILE PARALLEL UNSAFE;

DROP FUNCTION @extschema@.show_chunks(REGCLASS, "any", "any");
CREATE FUNCTION @extschema@.show_chunks(
     relation               REGCLASS,
     older_than             "any" = NULL,
     newer_than             "any" = NULL,
     created_before         "any" = NULL,
     created_after          "any" = NULL
 ) RETURNS SETOF REGCLASS AS '$libdir/timescaledb-2.20.3', 'ts_chunk_show_chunks'
 LANGUAGE C STABLE PARALLEL SAFE;

DROP FUNCTION @extschema@.add_retention_policy(REGCLASS, "any", BOOL, INTERVAL, TIMESTAMPTZ, TEXT);
CREATE FUNCTION @extschema@.add_retention_policy(
       relation REGCLASS,
       drop_after "any" = NULL,
       if_not_exists BOOL = false,
       schedule_interval INTERVAL = NULL,
       initial_start TIMESTAMPTZ = NULL,
       timezone TEXT = NULL,
       drop_created_before INTERVAL = NULL
)
RETURNS INTEGER AS '$libdir/timescaledb-2.20.3', 'ts_policy_retention_add'
LANGUAGE C VOLATILE;

DROP FUNCTION @extschema@.add_compression_policy(REGCLASS, "any", BOOL, INTERVAL, TIMESTAMPTZ, TEXT);
CREATE FUNCTION @extschema@.add_compression_policy(
    hypertable REGCLASS,
    compress_after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    compress_created_before INTERVAL = NULL
)
RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_policy_compression_add'
LANGUAGE C VOLATILE;

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(INTEGER, INTEGER, ANYELEMENT, INTEGER, BOOLEAN, BOOLEAN);
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_execute(INTEGER, INTEGER, ANYELEMENT, INTEGER, BOOLEAN, BOOLEAN);
CREATE PROCEDURE
_timescaledb_functions.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN,
  use_creation_time   BOOLEAN)
AS $$
DECLARE
  htoid       REGCLASS;
  chunk_rec   RECORD;
  numchunks   INTEGER := 1;
  _message     text;
  _detail      text;
  -- chunk status bits:
  bit_compressed int := 1;
  bit_compressed_unordered int := 2;
  bit_frozen int := 4;
  bit_compressed_partial int := 8;
  creation_lag INTERVAL := NULL;
BEGIN

  -- procedures with SET clause cannot execute transaction
  -- control so we adjust search_path in procedure body
  SET LOCAL search_path TO pg_catalog, pg_temp;

  SELECT format('%I.%I', schema_name, table_name) INTO htoid
  FROM _timescaledb_catalog.hypertable
  WHERE id = htid;

  -- for the integer cases, we have to compute the lag w.r.t
  -- the integer_now function and then pass on to show_chunks
  IF pg_typeof(lag) IN ('BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype) THEN
    -- cannot have use_creation_time set with this
    IF use_creation_time IS TRUE THEN
        RAISE EXCEPTION 'job % cannot use creation time with integer_now function', job_id;
    END IF;
    lag := _timescaledb_functions.subtract_integer_from_now(htoid, lag::BIGINT);
  END IF;

  -- if use_creation_time has been specified then the lag needs to be used with the
  -- "compress_created_before" argument. Otherwise the usual "older_than" argument
  -- is good enough
  IF use_creation_time IS TRUE THEN
    creation_lag := lag;
    lag := NULL;
  END IF;

  FOR chunk_rec IN
    SELECT
      show.oid, ch.schema_name, ch.table_name, ch.status
    FROM
      @extschema@.show_chunks(htoid, older_than => lag, created_before => creation_lag) AS show(oid)
      INNER JOIN pg_class pgc ON pgc.oid = show.oid
      INNER JOIN pg_namespace pgns ON pgc.relnamespace = pgns.oid
      INNER JOIN _timescaledb_catalog.chunk ch ON ch.table_name = pgc.relname AND ch.schema_name = pgns.nspname AND ch.hypertable_id = htid
    WHERE
      ch.dropped IS FALSE
      AND (
        ch.status = 0 OR
        (
          ch.status & bit_compressed > 0 AND (
            ch.status & bit_compressed_unordered > 0 OR
            ch.status & bit_compressed_partial > 0
          )
        )
      )
  LOOP
    IF chunk_rec.status = 0 THEN
      BEGIN
        PERFORM @extschema@.compress_chunk( chunk_rec.oid );
      EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _message = MESSAGE_TEXT,
            _detail = PG_EXCEPTION_DETAIL;
        RAISE WARNING 'compressing chunk "%" failed when compression policy is executed', chunk_rec.oid::regclass::text
            USING DETAIL = format('Message: (%s), Detail: (%s).', _message, _detail),
                  ERRCODE = sqlstate;
      END;
    ELSIF
      (
        chunk_rec.status & bit_compressed > 0 AND (
          chunk_rec.status & bit_compressed_unordered > 0 OR
          chunk_rec.status & bit_compressed_partial > 0
        )
      ) AND recompress_enabled IS TRUE THEN
      BEGIN
        PERFORM @extschema@.decompress_chunk(chunk_rec.oid, if_compressed => true);
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'decompressing chunk "%" failed when compression policy is executed', chunk_rec.oid::regclass::text
            USING DETAIL = format('Message: (%s), Detail: (%s).', _message, _detail),
                  ERRCODE = sqlstate;
      END;
      -- SET LOCAL is only active until end of transaction.
      -- While we could use SET at the start of the function we do not
      -- want to bleed out search_path to caller, so we do SET LOCAL
      -- again after COMMIT
      BEGIN
        PERFORM @extschema@.compress_chunk(chunk_rec.oid);
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'compressing chunk "%" failed when compression policy is executed', chunk_rec.oid::regclass::text
            USING DETAIL = format('Message: (%s), Detail: (%s).', _message, _detail),
                  ERRCODE = sqlstate;
      END;
    END IF;
    COMMIT;
    -- SET LOCAL is only active until end of transaction.
    -- While we could use SET at the start of the function we do not
    -- want to bleed out search_path to caller, so we do SET LOCAL
    -- again after COMMIT
    SET LOCAL search_path TO pg_catalog, pg_temp;
    IF verbose_log THEN
       RAISE LOG 'job % completed processing chunk %.%', job_id, chunk_rec.schema_name, chunk_rec.table_name;
    END IF;
    numchunks := numchunks + 1;
    IF maxchunks > 0 AND numchunks >= maxchunks THEN
         EXIT;
    END IF;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

-- fix atttypmod and attcollation for segmentby columns
DO $$
DECLARE
  htc_id INTEGER;
  htc REGCLASS;
  _attname NAME;
  _atttypmod INTEGER;
  _attcollation OID;
BEGIN
  -- find any segmentby columns where typmod and collation in
  -- the compressed hypertable does not match the uncompressed
  -- hypertable values
  FOR htc_id, htc, _attname, _atttypmod, _attcollation IN
    SELECT cat.htc_id, cat.htc, pga.attname, ht_mod, ht_coll
    FROM pg_attribute pga
    INNER JOIN
    (
      SELECT
        htc.id AS htc_id,
        format('%I.%I',htc.schema_name,htc.table_name) AS htc,
        att_ht.atttypmod AS ht_mod,
        att_ht.attcollation AS ht_coll,
        c.attname
      FROM _timescaledb_catalog.hypertable_compression c
      INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id=c.hypertable_id
      INNER JOIN pg_attribute att_ht ON att_ht.attname = c.attname AND att_ht.attrelid = format('%I.%I',ht.schema_name,ht.table_name)::regclass
      INNER JOIN _timescaledb_catalog.hypertable htc ON htc.id=ht.compressed_hypertable_id
      WHERE c.segmentby_column_index > 0
    ) cat ON cat.htc::regclass = pga.attrelid AND cat.attname = pga.attname
    WHERE pga.atttypmod <> ht_mod OR pga.attcollation <> ht_coll
  LOOP
    -- fix typmod and collation for the compressed hypertable and all compressed chunks
    UPDATE pg_attribute SET atttypmod = _atttypmod, attcollation = _attcollation WHERE attname = _attname AND attrelid IN (
      SELECT format('%I.%I',schema_name,table_name)::regclass from _timescaledb_catalog.chunk WHERE hypertable_id = htc_id AND NOT dropped UNION ALL SELECT htc
    );
  END LOOP;
END
$$;

-- ERROR if trying to update the extension while multinode is present
DO $$
DECLARE
  data_nodes TEXT;
  dist_hypertables TEXT;
BEGIN
  SELECT string_agg(format('%I.%I', schema_name, table_name), ', ')
  INTO dist_hypertables
  FROM _timescaledb_catalog.hypertable
  WHERE replication_factor > 0;

  IF dist_hypertables IS NOT NULL THEN
    RAISE USING
      ERRCODE = 'feature_not_supported',
      MESSAGE = 'cannot upgrade because multi-node has been removed in 2.14.0',
      DETAIL = 'The following distributed hypertables should be migrated to regular: '||dist_hypertables;
  END IF;

  SELECT string_agg(format('%I', srv.srvname), ', ')
  INTO data_nodes
  FROM pg_foreign_server srv
  JOIN pg_foreign_data_wrapper fdw ON srv.srvfdw = fdw.oid AND fdw.fdwname = 'timescaledb_fdw';

  IF data_nodes IS NOT NULL THEN
    RAISE USING
      ERRCODE = 'feature_not_supported',
      MESSAGE = 'cannot upgrade because multi-node has been removed in 2.14.0',
      DETAIL = 'The following data nodes should be removed: '||data_nodes;
  END IF;

  IF EXISTS(SELECT FROM _timescaledb_catalog.metadata WHERE key = 'dist_uuid') THEN
    RAISE USING
      ERRCODE = 'feature_not_supported',
      MESSAGE = 'cannot upgrade because multi-node has been removed in 2.14.0',
      DETAIL = 'This node appears to be part of a multi-node installation';
  END IF;
END $$;

DROP FUNCTION IF EXISTS _timescaledb_functions.ping_data_node;
DROP FUNCTION IF EXISTS _timescaledb_internal.ping_data_node;
DROP FUNCTION IF EXISTS _timescaledb_functions.remote_txn_heal_data_node;
DROP FUNCTION IF EXISTS _timescaledb_internal.remote_txn_heal_data_node;

DROP FUNCTION IF EXISTS _timescaledb_functions.set_dist_id;
DROP FUNCTION IF EXISTS _timescaledb_internal.set_dist_id;
DROP FUNCTION IF EXISTS _timescaledb_functions.set_peer_dist_id;
DROP FUNCTION IF EXISTS _timescaledb_internal.set_peer_dist_id;
DROP FUNCTION IF EXISTS _timescaledb_functions.validate_as_data_node;
DROP FUNCTION IF EXISTS _timescaledb_internal.validate_as_data_node;
DROP FUNCTION IF EXISTS _timescaledb_functions.show_connection_cache;
DROP FUNCTION IF EXISTS _timescaledb_internal.show_connection_cache;

DROP FUNCTION IF EXISTS @extschema@.create_hypertable(relation REGCLASS, time_column_name NAME, partitioning_column NAME, number_partitions INTEGER, associated_schema_name NAME, associated_table_prefix NAME, chunk_time_interval ANYELEMENT, create_default_indexes BOOLEAN, if_not_exists BOOLEAN, partitioning_func REGPROC, migrate_data BOOLEAN, chunk_target_size TEXT, chunk_sizing_func REGPROC, time_partitioning_func REGPROC, replication_factor INTEGER, data_nodes NAME[], distributed BOOLEAN);

CREATE FUNCTION @extschema@.create_hypertable(
    relation                REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    chunk_time_interval     ANYELEMENT = NULL::bigint,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    partitioning_func       REGPROC = NULL,
    migrate_data            BOOLEAN = FALSE,
    chunk_target_size       TEXT = NULL,
    chunk_sizing_func       REGPROC = '_timescaledb_functions.calculate_chunk_interval'::regproc,
    time_partitioning_func  REGPROC = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '$libdir/timescaledb-2.20.3', 'ts_hypertable_create' LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS @extschema@.create_distributed_hypertable;

DROP FUNCTION IF EXISTS @extschema@.add_data_node;
DROP FUNCTION IF EXISTS @extschema@.delete_data_node;
DROP FUNCTION IF EXISTS @extschema@.attach_data_node;
DROP FUNCTION IF EXISTS @extschema@.detach_data_node;
DROP FUNCTION IF EXISTS @extschema@.alter_data_node;

DROP PROCEDURE IF EXISTS @extschema@.distributed_exec;
DROP FUNCTION IF EXISTS @extschema@.create_distributed_restore_point;
DROP FUNCTION IF EXISTS @extschema@.set_replication_factor;

CREATE TABLE _timescaledb_catalog.compression_settings (
  relid regclass NOT NULL,
  segmentby text[],
  orderby text[],
  orderby_desc bool[],
  orderby_nullsfirst bool[],
  CONSTRAINT compression_settings_pkey PRIMARY KEY (relid),
  CONSTRAINT compression_settings_check_segmentby CHECK (array_ndims(segmentby) = 1),
  CONSTRAINT compression_settings_check_orderby_null CHECK ( (orderby IS NULL AND orderby_desc IS NULL AND orderby_nullsfirst IS NULL) OR (orderby IS NOT NULL AND orderby_desc IS NOT NULL AND orderby_nullsfirst IS NOT NULL) ),
  CONSTRAINT compression_settings_check_orderby_cardinality CHECK (array_ndims(orderby) = 1 AND array_ndims(orderby_desc) = 1 AND array_ndims(orderby_nullsfirst) = 1 AND cardinality(orderby) = cardinality(orderby_desc) AND cardinality(orderby) = cardinality(orderby_nullsfirst))
);

INSERT INTO _timescaledb_catalog.compression_settings(relid, segmentby, orderby, orderby_desc, orderby_nullsfirst)
  SELECT
    format('%I.%I', ht.schema_name, ht.table_name)::regclass,
    array_agg(attname ORDER BY segmentby_column_index) FILTER(WHERE segmentby_column_index >= 1) AS compress_segmentby,
    array_agg(attname ORDER BY orderby_column_index) FILTER(WHERE orderby_column_index >= 1) AS compress_orderby,
    array_agg(NOT orderby_asc ORDER BY orderby_column_index) FILTER(WHERE orderby_column_index >= 1) AS compress_orderby_desc,
    array_agg(orderby_nullsfirst ORDER BY orderby_column_index) FILTER(WHERE orderby_column_index >= 1) AS compress_orderby_nullsfirst
  FROM _timescaledb_catalog.hypertable_compression hc
    INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id = hc.hypertable_id
  GROUP BY hypertable_id, ht.schema_name, ht.table_name;

INSERT INTO _timescaledb_catalog.compression_settings
SELECT format('%I.%I',ch.schema_name,ch.table_name)::regclass,s.segmentby,s.orderby,s.orderby_desc,s.orderby_nullsfirst
FROM _timescaledb_catalog.hypertable ht1
INNER JOIN _timescaledb_catalog.hypertable ht2 ON ht2.id = ht1.compressed_hypertable_id
INNER JOIN _timescaledb_catalog.compression_settings s ON s.relid = format('%I.%I',ht1.schema_name,ht1.table_name)::regclass
INNER JOIN _timescaledb_catalog.chunk ch ON ch.hypertable_id = ht2.id ON CONFLICT DO NOTHING;

GRANT SELECT ON _timescaledb_catalog.compression_settings TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_settings', '');

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.hypertable_compression;
DROP VIEW IF EXISTS timescaledb_information.compression_settings;
DROP TABLE _timescaledb_catalog.hypertable_compression;

DROP FOREIGN DATA WRAPPER IF EXISTS timescaledb_fdw;
DROP FUNCTION IF EXISTS @extschema@.timescaledb_fdw_handler();
DROP FUNCTION IF EXISTS @extschema@.timescaledb_fdw_validator(text[], oid);


DROP FUNCTION IF EXISTS _timescaledb_functions.create_chunk_replica_table;
DROP FUNCTION IF EXISTS _timescaledb_functions.chunk_drop_replica;
DROP PROCEDURE IF EXISTS _timescaledb_functions.wait_subscription_sync;
DROP FUNCTION IF EXISTS _timescaledb_functions.health;
DROP FUNCTION IF EXISTS _timescaledb_functions.drop_stale_chunks;

DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_replica_table;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_drop_replica;
DROP PROCEDURE IF EXISTS _timescaledb_internal.wait_subscription_sync;
DROP FUNCTION IF EXISTS _timescaledb_internal.health;
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_stale_chunks;

ALTER TABLE _timescaledb_catalog.remote_txn DROP CONSTRAINT remote_txn_remote_transaction_id_check;

DROP TYPE IF EXISTS @extschema@.rxid CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_functions.rxid_in;
DROP FUNCTION IF EXISTS _timescaledb_functions.rxid_out;

DROP FUNCTION IF EXISTS _timescaledb_functions.data_node_hypertable_info;
DROP FUNCTION IF EXISTS _timescaledb_functions.data_node_chunk_info;
DROP FUNCTION IF EXISTS _timescaledb_functions.data_node_compressed_chunk_stats;
DROP FUNCTION IF EXISTS _timescaledb_functions.data_node_index_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.data_node_hypertable_info;
DROP FUNCTION IF EXISTS _timescaledb_internal.data_node_chunk_info;
DROP FUNCTION IF EXISTS _timescaledb_internal.data_node_compressed_chunk_stats;
DROP FUNCTION IF EXISTS _timescaledb_internal.data_node_index_size;

DROP FUNCTION IF EXISTS timescaledb_experimental.block_new_chunks;
DROP FUNCTION IF EXISTS timescaledb_experimental.allow_new_chunks;
DROP FUNCTION IF EXISTS timescaledb_experimental.subscription_exec;
DROP PROCEDURE IF EXISTS timescaledb_experimental.move_chunk;
DROP PROCEDURE IF EXISTS timescaledb_experimental.copy_chunk;
DROP PROCEDURE IF EXISTS timescaledb_experimental.cleanup_copy_chunk_operation;

DROP FUNCTION IF EXISTS _timescaledb_functions.set_chunk_default_data_node;
DROP FUNCTION IF EXISTS _timescaledb_internal.set_chunk_default_data_node;

DROP FUNCTION IF EXISTS _timescaledb_functions.drop_dist_ht_invalidation_trigger;
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_dist_ht_invalidation_trigger;

-- remove multinode catalog tables
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS timescaledb_information.data_nodes;
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_experimental.chunk_replication_status;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.remote_txn;
DROP TABLE _timescaledb_catalog.remote_txn;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.hypertable_data_node;
DROP TABLE _timescaledb_catalog.hypertable_data_node;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk_data_node;
DROP TABLE _timescaledb_catalog.chunk_data_node;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk_copy_operation;
DROP TABLE _timescaledb_catalog.chunk_copy_operation;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_copy_operation_id_seq;
DROP SEQUENCE _timescaledb_catalog.chunk_copy_operation_id_seq;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.dimension_partition;
DROP TABLE _timescaledb_catalog.dimension_partition;

DROP FUNCTION IF EXISTS _timescaledb_functions.hypertable_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_functions.chunks_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunks_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_functions.indexes_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.indexes_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_functions.compressed_chunk_remote_stats;
DROP FUNCTION IF EXISTS _timescaledb_internal.compressed_chunk_remote_stats;

-- rebuild _timescaledb_catalog.hypertable
ALTER TABLE _timescaledb_config.bgw_job
    DROP CONSTRAINT bgw_job_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk
    DROP CONSTRAINT chunk_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_index
    DROP CONSTRAINT chunk_index_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_agg
    DROP CONSTRAINT continuous_agg_mat_hypertable_id_fkey,
    DROP CONSTRAINT continuous_agg_raw_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_bucket_function
    DROP CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold
    DROP CONSTRAINT continuous_aggs_invalidation_threshold_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.dimension
    DROP CONSTRAINT dimension_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.hypertable
    DROP CONSTRAINT hypertable_compressed_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.tablespace
    DROP CONSTRAINT tablespace_hypertable_id_fkey;

DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.jobs;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS timescaledb_information.dimensions;
DROP VIEW IF EXISTS timescaledb_information.compression_settings;
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;
DROP VIEW IF EXISTS timescaledb_experimental.chunk_replication_status;
DROP VIEW IF EXISTS timescaledb_experimental.policies;

-- recreate table
CREATE TABLE _timescaledb_catalog.hypertable_tmp AS SELECT * FROM _timescaledb_catalog.hypertable;
CREATE TABLE _timescaledb_catalog.tmp_hypertable_seq_value AS SELECT last_value, is_called FROM _timescaledb_catalog.hypertable_id_seq;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.hypertable;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.hypertable_id_seq;

SET timescaledb.restoring = on; -- must disable the hooks otherwise we can't do anything without the table _timescaledb_catalog.hypertable

DROP TABLE _timescaledb_catalog.hypertable;

CREATE SEQUENCE _timescaledb_catalog.hypertable_id_seq MINVALUE 1;
SELECT setval('_timescaledb_catalog.hypertable_id_seq', last_value, is_called) FROM _timescaledb_catalog.tmp_hypertable_seq_value;
DROP TABLE _timescaledb_catalog.tmp_hypertable_seq_value;

CREATE TABLE _timescaledb_catalog.hypertable (
    id INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('_timescaledb_catalog.hypertable_id_seq'),
    schema_name name NOT NULL,
    table_name name NOT NULL,
    associated_schema_name name NOT NULL,
    associated_table_prefix name NOT NULL,
    num_dimensions smallint NOT NULL,
    chunk_sizing_func_schema name NOT NULL,
    chunk_sizing_func_name name NOT NULL,
    chunk_target_size bigint NOT NULL, -- size in bytes
    compression_state smallint NOT NULL DEFAULT 0,
    compressed_hypertable_id integer,
    status integer NOT NULL DEFAULT 0
);

SET timescaledb.restoring = off;

INSERT INTO _timescaledb_catalog.hypertable (
    id,
    schema_name,
    table_name,
    associated_schema_name,
    associated_table_prefix,
    num_dimensions,
    chunk_sizing_func_schema,
    chunk_sizing_func_name,
    chunk_target_size,
    compression_state,
    compressed_hypertable_id
)
SELECT
    id,
    schema_name,
    table_name,
    associated_schema_name,
    associated_table_prefix,
    num_dimensions,
    chunk_sizing_func_schema,
    chunk_sizing_func_name,
    chunk_target_size,
    compression_state,
    compressed_hypertable_id
FROM
    _timescaledb_catalog.hypertable_tmp
ORDER BY id;

UPDATE _timescaledb_catalog.hypertable h
SET status = 3
WHERE EXISTS (
  SELECT FROM _timescaledb_catalog.chunk c WHERE c.osm_chunk AND c.hypertable_id = h.id
);

ALTER SEQUENCE _timescaledb_catalog.hypertable_id_seq OWNED BY _timescaledb_catalog.hypertable.id;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_id_seq', '');

GRANT SELECT ON _timescaledb_catalog.hypertable TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.hypertable_id_seq TO PUBLIC;

DROP TABLE _timescaledb_catalog.hypertable_tmp;
-- now add any constraints
ALTER TABLE _timescaledb_catalog.hypertable
    ADD CONSTRAINT hypertable_associated_schema_name_associated_table_prefix_key UNIQUE (associated_schema_name, associated_table_prefix),
    ADD CONSTRAINT hypertable_table_name_schema_name_key UNIQUE (table_name, schema_name),
    ADD CONSTRAINT hypertable_schema_name_check CHECK (schema_name != '_timescaledb_catalog'),
    ADD CONSTRAINT hypertable_dim_compress_check CHECK (num_dimensions > 0 OR compression_state = 2),
    ADD CONSTRAINT hypertable_chunk_target_size_check CHECK (chunk_target_size >= 0),
    ADD CONSTRAINT hypertable_compress_check CHECK ( (compression_state = 0 OR compression_state = 1 )  OR (compression_state = 2 AND compressed_hypertable_id IS NULL)),
    ADD CONSTRAINT hypertable_compressed_hypertable_id_fkey FOREIGN KEY (compressed_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id);

GRANT SELECT ON TABLE _timescaledb_catalog.hypertable TO PUBLIC;

-- 3. reestablish constraints on other tables
ALTER TABLE _timescaledb_config.bgw_job
    ADD CONSTRAINT bgw_job_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk
    ADD CONSTRAINT chunk_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id);
ALTER TABLE _timescaledb_catalog.chunk_index
    ADD CONSTRAINT chunk_index_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_agg
    ADD CONSTRAINT continuous_agg_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    ADD CONSTRAINT continuous_agg_raw_hypertable_id_fkey FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_bucket_function
    ADD CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold
    ADD CONSTRAINT continuous_aggs_invalidation_threshold_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.dimension
    ADD CONSTRAINT dimension_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.tablespace
    ADD CONSTRAINT tablespace_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;

CREATE SCHEMA _timescaledb_debug;

-- Migrate existing compressed hypertables to new internal format
DO $$
DECLARE
  chunk regclass;
  hypertable regclass;
  ht_id integer;
  index regclass;
  column_name name;
  cmd text;
BEGIN
  SET timescaledb.restoring TO ON;

  -- Detach compressed chunks from their parent hypertables
  FOR chunk, hypertable, ht_id IN
    SELECT
      format('%I.%I',ch.schema_name,ch.table_name)::regclass chunk,
      format('%I.%I',ht.schema_name,ht.table_name)::regclass hypertable,
      ht.id
    FROM _timescaledb_catalog.chunk ch
    INNER JOIN _timescaledb_catalog.hypertable ht_uncomp
      ON ch.hypertable_id = ht_uncomp.compressed_hypertable_id
    INNER JOIN _timescaledb_catalog.hypertable ht
      ON ht.id = ht_uncomp.compressed_hypertable_id
  LOOP

    cmd := format('ALTER TABLE %s NO INHERIT %s', chunk, hypertable);
    EXECUTE cmd;
    -- remove references to indexes from the compressed hypertable
    DELETE FROM _timescaledb_catalog.chunk_index WHERE hypertable_id = ht_id;

  END LOOP;


  FOR hypertable IN
    SELECT
      format('%I.%I',ht.schema_name,ht.table_name)::regclass hypertable
    FROM _timescaledb_catalog.hypertable ht_uncomp
    INNER JOIN _timescaledb_catalog.hypertable ht
      ON ht.id = ht_uncomp.compressed_hypertable_id
  LOOP

    -- remove indexes from the compressed hypertable (but not chunks)
    FOR index IN
      SELECT indexrelid::regclass FROM pg_index WHERE indrelid = hypertable
    LOOP
      cmd := format('DROP INDEX %s', index);
      EXECUTE cmd;
    END LOOP;

    -- remove columns from the compressed hypertable (but not chunks)
    FOR column_name IN
      SELECT attname FROM pg_attribute WHERE attrelid = hypertable AND attnum > 0 AND NOT attisdropped
    LOOP
      cmd := format('ALTER TABLE %s DROP COLUMN %I', hypertable, column_name);
      EXECUTE cmd;
    END LOOP;

  END LOOP;

  SET timescaledb.restoring TO OFF;
END $$;

DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_constraint_add_table_fk_constraint;
DROP FUNCTION IF EXISTS _timescaledb_functions.hypertable_constraint_add_table_fk_constraint;

-- only define stub here, actual code will be filled in at end of update script
CREATE FUNCTION _timescaledb_functions.constraint_clone(constraint_oid OID,target_oid REGCLASS) RETURNS VOID LANGUAGE PLPGSQL AS $$BEGIN END$$ SET search_path TO pg_catalog, pg_temp;

DROP FUNCTION IF EXISTS _timescaledb_functions.chunks_in;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunks_in;

CREATE FUNCTION _timescaledb_functions.metadata_insert_trigger() RETURNS TRIGGER LANGUAGE PLPGSQL
AS $$
BEGIN
  IF EXISTS (SELECT FROM _timescaledb_catalog.metadata WHERE key = NEW.key) THEN
    UPDATE _timescaledb_catalog.metadata SET value = NEW.value WHERE key = NEW.key;
    RETURN NULL;
  END IF;
  RETURN NEW;
END
$$ SET search_path TO pg_catalog, pg_temp;

CREATE TRIGGER metadata_insert_trigger BEFORE INSERT ON _timescaledb_catalog.metadata FOR EACH ROW EXECUTE PROCEDURE _timescaledb_functions.metadata_insert_trigger();

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.metadata', $$ WHERE key <> 'uuid' $$);

-- Remove unwanted entries from extconfig and extcondition in pg_extension
-- We use ALTER EXTENSION DROP TABLE to remove these entries.
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_cache.cache_inval_hypertable;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_cache.cache_inval_extension;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_cache.cache_inval_bgw_job;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_internal.job_errors;

-- Associate the above tables back to keep the dependencies safe
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_cache.cache_inval_hypertable;
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_cache.cache_inval_extension;
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_cache.cache_inval_bgw_job;
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_internal.job_errors;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.hypertable;
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_catalog.hypertable;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', 'WHERE id >= 1');

CREATE FUNCTION _timescaledb_functions.relation_approximate_size(relation REGCLASS)
RETURNS TABLE (total_size BIGINT, heap_size BIGINT, index_size BIGINT, toast_size BIGINT)
AS '$libdir/timescaledb-2.20.3', 'ts_relation_approximate_size' LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION @extschema@.hypertable_approximate_detailed_size(relation REGCLASS)
RETURNS TABLE (table_bytes BIGINT, index_bytes BIGINT, toast_bytes BIGINT, total_bytes BIGINT)
AS '$libdir/timescaledb-2.20.3', 'ts_hypertable_approximate_size' LANGUAGE C VOLATILE;

--- returns approximate total-bytes for a hypertable (includes table + index)
CREATE FUNCTION @extschema@.hypertable_approximate_size(
    hypertable              REGCLASS)
RETURNS BIGINT
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
   SELECT sum(total_bytes)::bigint
   FROM @extschema@.hypertable_approximate_detailed_size(hypertable);
$BODY$ SET search_path TO pg_catalog, pg_temp;

DROP FUNCTION IF EXISTS @extschema@.compress_chunk;
CREATE FUNCTION @extschema@.compress_chunk(uncompressed_chunk REGCLASS, if_not_compressed BOOLEAN = true, recompress BOOLEAN = false) RETURNS REGCLASS AS '' LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;


CREATE VIEW timescaledb_information.hypertable_compression_settings AS
	SELECT
		format('%I.%I',ht.schema_name,ht.table_name)::regclass AS hypertable,
		array_to_string(segmentby,',') AS segmentby,
		un.orderby,
    d.compress_interval_length
  FROM _timescaledb_catalog.hypertable ht
  JOIN LATERAL (
    SELECT
      CASE WHEN d.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
        _timescaledb_functions.to_interval(d.compress_interval_length)::text
      ELSE
        d.compress_interval_length::text
      END AS compress_interval_length
    FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = ht.id ORDER BY id LIMIT 1
  ) d ON true
  LEFT JOIN _timescaledb_catalog.compression_settings s ON format('%I.%I',ht.schema_name,ht.table_name)::regclass = s.relid
	LEFT JOIN LATERAL (
		SELECT
			string_agg(
				format('%I%s%s',orderby,
					CASE WHEN "desc" THEN ' DESC' ELSE '' END,
					CASE WHEN nullsfirst AND NOT "desc" THEN ' NULLS FIRST' WHEN NOT nullsfirst AND "desc" THEN ' NULLS LAST' ELSE '' END
				)
			,',') AS orderby
		FROM unnest(s.orderby, s.orderby_desc, s.orderby_nullsfirst) un(orderby, "desc", nullsfirst)
	) un ON true;

CREATE VIEW timescaledb_information.chunk_compression_settings AS
	SELECT
		format('%I.%I',ht.schema_name,ht.table_name)::regclass AS hypertable,
		format('%I.%I',ch.schema_name,ch.table_name)::regclass AS chunk,
		array_to_string(segmentby,',') AS segmentby,
		un.orderby
	FROM _timescaledb_catalog.hypertable ht
	INNER JOIN _timescaledb_catalog.chunk ch ON ch.hypertable_id = ht.id
  INNER JOIN _timescaledb_catalog.chunk ch2 ON ch2.id = ch.compressed_chunk_id
  LEFT JOIN _timescaledb_catalog.compression_settings s ON format('%I.%I',ch2.schema_name,ch2.table_name)::regclass = s.relid
	LEFT JOIN LATERAL (
		SELECT
			string_agg(
				format('%I%s%s',orderby,
					CASE WHEN "desc" THEN ' DESC' ELSE '' END,
					CASE WHEN nullsfirst AND NOT "desc" THEN ' NULLS FIRST' WHEN NOT nullsfirst AND "desc" THEN ' NULLS LAST' ELSE '' END
			),',') AS orderby
		FROM unnest(s.orderby, s.orderby_desc, s.orderby_nullsfirst) un(orderby, "desc", nullsfirst)
	) un ON true;

INSERT INTO _timescaledb_catalog.compression_settings
SELECT
	format('%I.%I',ch.schema_name,ch.table_name)::regclass,s.segmentby,s.orderby,s.orderby_desc,s.orderby_nullsfirst
FROM _timescaledb_catalog.hypertable ht1
INNER JOIN _timescaledb_catalog.hypertable ht2 ON ht2.id = ht1.compressed_hypertable_id
INNER JOIN _timescaledb_catalog.compression_settings s ON s.relid = format('%I.%I',ht1.schema_name,ht1.table_name)::regclass
INNER JOIN _timescaledb_catalog.chunk ch ON ch.hypertable_id = ht2.id ON CONFLICT DO NOTHING;

-- Remove multi-node CAGG support
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_cagg_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_hyper_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_internal.materialization_invalidation_log_delete(integer);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_invalidation_log_delete(integer);

DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_cagg_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_hyper_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_functions.materialization_invalidation_log_delete(integer);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.hypertable_invalidation_log_delete(integer);

-- Remove chunk metadata when marked as dropped
CREATE FUNCTION _timescaledb_functions.remove_dropped_chunk_metadata(_hypertable_id INTEGER)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
  _chunk_id INTEGER;
  _removed INTEGER := 0;
BEGIN
  FOR _chunk_id IN
    SELECT id FROM _timescaledb_catalog.chunk
    WHERE hypertable_id = _hypertable_id
    AND dropped IS TRUE
    AND NOT EXISTS (
        SELECT FROM information_schema.tables
        WHERE tables.table_schema = chunk.schema_name
        AND tables.table_name = chunk.table_name
    )
    AND NOT EXISTS (
        SELECT FROM _timescaledb_catalog.hypertable
        JOIN _timescaledb_catalog.continuous_agg ON continuous_agg.raw_hypertable_id = hypertable.id
        WHERE hypertable.id = chunk.hypertable_id
        -- for the old caggs format we need to keep chunk metadata for dropped chunks
        AND continuous_agg.finalized IS FALSE
    )
  LOOP
    _removed := _removed + 1;
    RAISE INFO 'Removing metadata of chunk % from hypertable %', _chunk_id, _hypertable_id;

    WITH _dimension_slice_remove AS (
        DELETE FROM _timescaledb_catalog.dimension_slice
        USING _timescaledb_catalog.chunk_constraint
        WHERE dimension_slice.id = chunk_constraint.dimension_slice_id
        AND chunk_constraint.chunk_id = _chunk_id
        AND NOT EXISTS (
            SELECT FROM _timescaledb_catalog.chunk_constraint cc
            WHERE cc.chunk_id <> _chunk_id
            AND cc.dimension_slice_id = dimension_slice.id
        )
        RETURNING _timescaledb_catalog.dimension_slice.id
    )
    DELETE FROM _timescaledb_catalog.chunk_constraint
    USING _dimension_slice_remove
    WHERE chunk_constraint.dimension_slice_id = _dimension_slice_remove.id;

    DELETE FROM _timescaledb_catalog.chunk_constraint
    WHERE chunk_constraint.chunk_id = _chunk_id;

    DELETE FROM _timescaledb_internal.bgw_policy_chunk_stats
    WHERE bgw_policy_chunk_stats.chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.chunk_index
    WHERE chunk_index.chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.compression_chunk_size
    WHERE compression_chunk_size.chunk_id = _chunk_id
    OR compression_chunk_size.compressed_chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.chunk
    WHERE chunk.id = _chunk_id
    OR chunk.compressed_chunk_id = _chunk_id;
  END LOOP;

  RETURN _removed;
END;
$$ SET search_path TO pg_catalog, pg_temp;

SELECT _timescaledb_functions.remove_dropped_chunk_metadata(id) AS chunks_metadata_removed
FROM _timescaledb_catalog.hypertable;

--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_aggs_bucket_function`
--

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_get_bucket_function(
    mat_hypertable_id INTEGER
) RETURNS regprocedure AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_get_bucket_function' LANGUAGE C STRICT VOLATILE;

-- Since we need now the regclass of the used bucket function, we have to recover it
-- by parsing the view query by calling 'cagg_get_bucket_function'.
CREATE TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function AS
    SELECT
      mat_hypertable_id,
      _timescaledb_functions.cagg_get_bucket_function(mat_hypertable_id),
      bucket_width,
      origin,
      NULL::text AS bucket_offset,
      timezone,
      false AS bucket_fixed_width
    FROM
      _timescaledb_catalog.continuous_aggs_bucket_function
    ORDER BY
         mat_hypertable_id;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function (
  mat_hypertable_id integer NOT NULL,
  -- The bucket function
  bucket_func regprocedure NOT NULL,
  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,
  -- optional `origin` argument of the function provided by the user
  bucket_origin text,
  -- optional `offset` argument of the function provided by the user
  bucket_offset text,
  -- optional `timezone` argument of the function provided by the user
  bucket_timezone text,
  -- fixed or variable sized bucket
  bucket_fixed_width bool NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_bucket_function_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  SELECT * FROM _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_aggs_bucket_function TO PUBLIC;

ANALYZE _timescaledb_catalog.continuous_aggs_bucket_function;

ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_functions.cagg_get_bucket_function(INTEGER);
DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_get_bucket_function(INTEGER);

--
-- End rebuild the catalog table `_timescaledb_catalog.continuous_aggs_bucket_function`
--

-- Convert _timescaledb_catalog.continuous_aggs_bucket_function.bucket_origin to TimestampTZ
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function
   SET bucket_origin = bucket_origin::timestamp::timestamptz::text
   WHERE length(bucket_origin) > 1;

-- Historically, we have used empty strings for undefined bucket_origin and timezone
-- attributes. This is now replaced by proper NULL values. We use TRIM() to ensure we handle empty string well.
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function SET bucket_origin = NULL WHERE TRIM(bucket_origin) = '';
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function SET bucket_timezone = NULL WHERE TRIM(bucket_timezone) = '';

-- So far, there were no difference between 0 and -1 retries. Since now on, 0 means no retries. Updating the retry
-- count of existing jobs to -1 to keep the current semantics.
UPDATE _timescaledb_config.bgw_job SET max_retries = -1 WHERE max_retries = 0;

DROP FUNCTION IF EXISTS _timescaledb_functions.get_chunk_relstats;
DROP FUNCTION IF EXISTS _timescaledb_functions.get_chunk_colstats;
DROP FUNCTION IF EXISTS _timescaledb_internal.get_chunk_relstats;
DROP FUNCTION IF EXISTS _timescaledb_internal.get_chunk_colstats;

-- In older TSDB versions, we disabled autovacuum for compressed chunks
-- to keep the statistics. However, this restriction was removed in
-- #5118 but no migration was performed to remove the custom
-- autovacuum setting for existing chunks.
DO $$
DECLARE
  chunk regclass;
BEGIN
  FOR chunk IN
    SELECT pg_catalog.format('%I.%I', schema_name, table_name)::regclass
      FROM _timescaledb_catalog.chunk c
      JOIN pg_catalog.pg_class AS pc ON (pc.oid=format('%I.%I', schema_name, table_name)::regclass)
      CROSS JOIN unnest(reloptions) AS u(option)
      WHERE
        dropped = false
        AND osm_chunk = false
        AND option LIKE 'autovacuum_enabled%'
  LOOP
    EXECUTE pg_catalog.format('ALTER TABLE %s RESET (autovacuum_enabled);', chunk::text);
  END LOOP;
END
$$;

--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_agg`
--

-- (1) Create missing entries in _timescaledb_catalog.continuous_aggs_bucket_function
CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_get_bucket_function(
    mat_hypertable_id INTEGER
) RETURNS regprocedure AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_get_bucket_function' LANGUAGE C STRICT VOLATILE;

-- Make sure function points to the new version of TSDB
CREATE OR REPLACE FUNCTION _timescaledb_functions.to_interval(unixtime_us BIGINT) RETURNS INTERVAL
    AS '$libdir/timescaledb-2.20.3', 'ts_pg_unix_microseconds_to_interval' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- We need to create entries in continuous_aggs_bucket_function for all CAggs that were treated so far
-- as fixed indicated by a bucket_width != -1
INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  SELECT
  mat_hypertable_id,
  _timescaledb_functions.cagg_get_bucket_function(mat_hypertable_id),
  -- Intervals needs to be converted into the proper interval format
  -- Function name could be prefixed with 'public.'. Therefore LIKE instead of starts_with is used
  CASE WHEN _timescaledb_functions.cagg_get_bucket_function(mat_hypertable_id)::text LIKE '%time_bucket(interval,%' THEN
    _timescaledb_functions.to_interval(bucket_width)::text
  ELSE
    bucket_width::text
  END,
  NULL, -- bucket_origin
  NULL, -- bucket_offset
  NULL, -- bucket_timezone
  true  -- bucket_fixed_width
  FROM _timescaledb_catalog.continuous_agg WHERE bucket_width != -1;

ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_functions.cagg_get_bucket_function(INTEGER);
DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_get_bucket_function(INTEGER);

-- (2) Rebuild catalog table
DROP VIEW IF EXISTS timescaledb_experimental.policies;
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

DROP PROCEDURE IF EXISTS @extschema@.cagg_migrate (REGCLASS, BOOLEAN, BOOLEAN);
DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_migrate_pre_validation (TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_migrate_pre_validation (TEXT, TEXT, TEXT);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);

DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_migrate_plan_exists (INTEGER);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_plan (_timescaledb_catalog.continuous_agg);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_plan (_timescaledb_catalog.continuous_agg);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_create_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_create_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_disable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_disable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_enable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_enable_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_copy_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_copy_policies (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_refresh_new_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_copy_data (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_copy_data (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_override_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_override_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_internal.cagg_migrate_execute_drop_old_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_execute_drop_old_cagg (_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step);

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;

ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    DROP CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog._tmp_continuous_agg AS
    SELECT
        mat_hypertable_id,
        raw_hypertable_id,
        parent_mat_hypertable_id,
        user_view_schema,
        user_view_name,
        partial_view_schema,
        partial_view_name,
        direct_view_schema,
        direct_view_name,
        materialized_only,
        finalized
    FROM
        _timescaledb_catalog.continuous_agg
    ORDER BY
        mat_hypertable_id;

DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog.continuous_agg (
    mat_hypertable_id integer NOT NULL,
    raw_hypertable_id integer NOT NULL,
    parent_mat_hypertable_id integer,
    user_view_schema name NOT NULL,
    user_view_name name NOT NULL,
    partial_view_schema name NOT NULL,
    partial_view_name name NOT NULL,
    direct_view_schema name NOT NULL,
    direct_view_name name NOT NULL,
    materialized_only bool NOT NULL DEFAULT FALSE,
    finalized bool NOT NULL DEFAULT TRUE,
    -- table constraints
    CONSTRAINT continuous_agg_pkey PRIMARY KEY (mat_hypertable_id),
    CONSTRAINT continuous_agg_partial_view_schema_partial_view_name_key UNIQUE (partial_view_schema, partial_view_name),
    CONSTRAINT continuous_agg_user_view_schema_user_view_name_key UNIQUE (user_view_schema, user_view_name),
    CONSTRAINT continuous_agg_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
    CONSTRAINT continuous_agg_raw_hypertable_id_fkey
        FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
    CONSTRAINT continuous_agg_parent_mat_hypertable_id_fkey
        FOREIGN KEY (parent_mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_agg
SELECT * FROM _timescaledb_catalog._tmp_continuous_agg;
DROP TABLE _timescaledb_catalog._tmp_continuous_agg;

CREATE INDEX continuous_agg_raw_hypertable_id_idx ON _timescaledb_catalog.continuous_agg (raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_agg TO PUBLIC;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    ADD CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey
        FOREIGN KEY (materialization_id)
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;

ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    ADD CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE;

ANALYZE _timescaledb_catalog.continuous_agg;

--
-- END Rebuild the catalog table `_timescaledb_catalog.continuous_agg`
--

--
-- START bgw_job_stat_history
--
DROP VIEW IF EXISTS timescaledb_information.job_errors;

CREATE SEQUENCE _timescaledb_internal.bgw_job_stat_history_id_seq MINVALUE 1;

CREATE TABLE _timescaledb_internal.bgw_job_stat_history (
  id INTEGER NOT NULL DEFAULT nextval('_timescaledb_internal.bgw_job_stat_history_id_seq'),
  job_id INTEGER NOT NULL,
  pid INTEGER,
  execution_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  execution_finish TIMESTAMPTZ,
  succeeded boolean NOT NULL DEFAULT FALSE,
  data jsonb,
  -- table constraints
  CONSTRAINT bgw_job_stat_history_pkey PRIMARY KEY (id)
);

ALTER SEQUENCE _timescaledb_internal.bgw_job_stat_history_id_seq OWNED BY _timescaledb_internal.bgw_job_stat_history.id;

CREATE INDEX bgw_job_stat_history_job_id_idx ON _timescaledb_internal.bgw_job_stat_history (job_id);

REVOKE ALL ON _timescaledb_internal.bgw_job_stat_history FROM PUBLIC;

INSERT INTO _timescaledb_internal.bgw_job_stat_history (job_id, pid, execution_start, execution_finish, data)
SELECT
  job_errors.job_id,
  job_errors.pid,
  job_errors.start_time,
  job_errors.finish_time,
  jsonb_build_object('job', to_jsonb(bgw_job.*)) || jsonb_build_object('error_data', job_errors.error_data)
FROM
  _timescaledb_internal.job_errors
  LEFT JOIN _timescaledb_config.bgw_job ON bgw_job.id = job_errors.job_id
ORDER BY
  job_errors.job_id, job_errors.start_time;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_internal.job_errors;

DROP TABLE _timescaledb_internal.job_errors;

UPDATE _timescaledb_config.bgw_job SET scheduled = false WHERE id = 2;
INSERT INTO _timescaledb_config.bgw_job (
    id,
    application_name,
    schedule_interval,
    max_runtime,
    max_retries,
    retry_period,
    proc_schema,
    proc_name,
    owner,
    scheduled,
    config,
    check_schema,
    check_name,
    fixed_schedule,
    initial_start
)
VALUES
(
    3,
    'Job History Log Retention Policy [3]',
    INTERVAL '1 month',
    INTERVAL '1 hour',
    -1,
    INTERVAL '1h',
    '_timescaledb_functions',
    'policy_job_stat_history_retention',
    pg_catalog.quote_ident(current_role)::regrole,
    true,
    '{"drop_after":"1 month"}',
    '_timescaledb_functions',
    'policy_job_stat_history_retention_check',
    true,
    '2000-01-01 00:00:00+00'::timestamptz
) ON CONFLICT (id) DO NOTHING;

DROP FUNCTION IF EXISTS _timescaledb_internal.policy_job_error_retention(job_id integer,config jsonb);
DROP FUNCTION IF EXISTS _timescaledb_internal.policy_job_error_retention_check(config jsonb);
DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_error_retention(job_id integer,config jsonb);
DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_error_retention_check(config jsonb);

--
-- END bgw_job_stat_history
--

-- Migrate existing CAggs using time_bucket_ng to time_bucket
CREATE PROCEDURE _timescaledb_functions.cagg_migrate_to_time_bucket(cagg REGCLASS)
   AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_migrate_to_time_bucket' LANGUAGE C;

DO $$
DECLARE
  cagg_name regclass;
BEGIN
  FOR cagg_name IN
    SELECT pg_catalog.format('%I.%I', user_view_schema, user_view_name)::regclass
      FROM _timescaledb_catalog.continuous_agg cagg
      JOIN _timescaledb_catalog.continuous_aggs_bucket_function AS bf ON (cagg.mat_hypertable_id = bf.mat_hypertable_id)
      WHERE
         bf.bucket_func::text LIKE '%time_bucket_ng%'
  LOOP
    CALL _timescaledb_functions.cagg_migrate_to_time_bucket(cagg_name);
  END LOOP;
END
$$;
CREATE TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function AS
    SELECT
      mat_hypertable_id,
      bucket_func::text AS bucket_func,
      bucket_width,
      bucket_origin,
      bucket_offset,
      bucket_timezone,
      bucket_fixed_width
    FROM
      _timescaledb_catalog.continuous_aggs_bucket_function
    ORDER BY
         mat_hypertable_id;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function (
  mat_hypertable_id integer NOT NULL,
  -- The bucket function
  bucket_func text NOT NULL,
  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,
  -- optional `origin` argument of the function provided by the user
  bucket_origin text,
  -- optional `offset` argument of the function provided by the user
  bucket_offset text,
  -- optional `timezone` argument of the function provided by the user
  bucket_timezone text,
  -- fixed or variable sized bucket
  bucket_fixed_width bool NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_bucket_function_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  CONSTRAINT continuous_aggs_bucket_function_func_check CHECK (pg_catalog.to_regprocedure(bucket_func) IS DISTINCT FROM 0)
);

INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  SELECT * FROM _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_aggs_bucket_function TO PUBLIC;

ANALYZE _timescaledb_catalog.continuous_aggs_bucket_function;
-- Enable tracking of statistics on a column of a hypertable.
--
-- hypertable - OID of the table to which the column belongs to
-- column_name - The column to track statistics for
-- if_not_exists - If set, and the entry already exists, generate a notice instead of an error
CREATE FUNCTION @extschema@.enable_chunk_skipping(
    hypertable              REGCLASS,
    column_name             NAME,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(column_stats_id INT, enabled BOOL)
AS 'SELECT NULL,NULL' LANGUAGE SQL VOLATILE SET search_path = pg_catalog, pg_temp;

-- Disable tracking of statistics on a column of a hypertable.
--
-- hypertable - OID of the table to remove from
-- column_name - NAME of the column on which the stats are tracked
-- if_not_exists - If set, and the entry does not exist,
-- generate a notice instead of an error
CREATE FUNCTION @extschema@.disable_chunk_skipping(
    hypertable              REGCLASS,
    column_name             NAME,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, column_name NAME, disabled BOOL)
AS 'SELECT NULL,NULL,NULL' LANGUAGE SQL VOLATILE SET search_path = pg_catalog, pg_temp;

-- Track statistics for columns of chunks from a hypertable.
-- Currently, we track the min/max range for a given column across chunks.
-- More statistics (like bloom filters) will be added in the future.
--
-- A "special" entry for a column with invalid chunk_id, PG_INT64_MAX,
-- PG_INT64_MIN indicates that min/max ranges could be computed for this column
-- for chunks.
--
-- The ranges can overlap across chunks. The values could be out-of-date if
-- modifications/changes occur in the corresponding chunk and such entries
-- should be marked as "invalid" to ensure that the chunk is in
-- appropriate state to be able to use these values. Thus these entries
-- are different from dimension_slice which is used for tracking partitioning
-- column ranges which have different characteristics.
--
-- Currently this catalog supports datatypes like INT, SERIAL, BIGSERIAL,
-- DATE, TIMESTAMP etc. by storing the ranges in bigint columns. In the
-- future, we could support additional datatypes (which support btree style
-- >, <, = comparators) by storing their textual representation.
--
CREATE TABLE _timescaledb_catalog.chunk_column_stats (
  id serial NOT NULL,
  hypertable_id integer NOT NULL,
  chunk_id integer NOT NULL,
  column_name name NOT NULL,
  range_start bigint NOT NULL,
  range_end bigint NOT NULL,
  valid boolean NOT NULL,
  -- table constraints
  CONSTRAINT chunk_column_stats_pkey PRIMARY KEY (id),
  CONSTRAINT chunk_column_stats_ht_id_chunk_id_colname_key UNIQUE (hypertable_id, chunk_id, column_name),
  CONSTRAINT chunk_column_stats_range_check CHECK (range_start <= range_end),
  CONSTRAINT chunk_column_stats_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id),
  CONSTRAINT chunk_column_stats_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_column_stats', '');

SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk_column_stats', 'id'), '');

GRANT SELECT ON _timescaledb_catalog.chunk_column_stats TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.chunk_column_stats_id_seq TO PUBLIC;

-- Remove foreign key constraints from compressed chunks
DO $$
DECLARE
  conrelid regclass;
  conname name;
BEGIN
  FOR conrelid, conname IN
  SELECT
    con.conrelid::regclass,
    con.conname
  FROM _timescaledb_catalog.chunk ch
  JOIN pg_constraint con ON con.conrelid = format('%I.%I',schema_name,table_name)::regclass AND con.contype='f'
  WHERE NOT ch.dropped AND EXISTS(SELECT FROM _timescaledb_catalog.chunk ch2 WHERE NOT ch2.dropped AND ch2.compressed_chunk_id=ch.id)
  LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', conrelid, conname);
  END LOOP;
END $$;

CREATE FUNCTION _timescaledb_functions.compressed_data_info(_timescaledb_internal.compressed_data)
    RETURNS TABLE (algorithm name, has_nulls bool)
    AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
    LANGUAGE C STRICT IMMUTABLE SET search_path = pg_catalog, pg_temp;

CREATE INDEX compression_chunk_size_idx ON _timescaledb_catalog.compression_chunk_size (compressed_chunk_id);

CREATE FUNCTION _timescaledb_functions.drop_osm_chunk(hypertable REGCLASS)
	RETURNS BOOL
	AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
	LANGUAGE C VOLATILE;
-- remove obsolete job
DELETE FROM _timescaledb_config.bgw_job WHERE id = 2;

-- Hypercore updates
CREATE FUNCTION _timescaledb_debug.is_compressed_tid(tid) RETURNS BOOL
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder' LANGUAGE C STRICT;

DROP FUNCTION IF EXISTS @extschema@.compress_chunk(uncompressed_chunk REGCLASS,	if_not_compressed BOOLEAN, recompress BOOLEAN);

CREATE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = true,
    recompress BOOLEAN = false,
    hypercore_use_access_method BOOL = NULL
) RETURNS REGCLASS AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder' LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(hypertable REGCLASS, compress_after "any", if_not_exists BOOL, schedule_interval INTERVAL, initial_start TIMESTAMPTZ, timezone TEXT, compress_created_before INTERVAL);

CREATE FUNCTION @extschema@.add_compression_policy(
    hypertable REGCLASS,
    compress_after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    compress_created_before INTERVAL = NULL,
    hypercore_use_access_method BOOL = NULL
)
RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS timescaledb_experimental.add_policies(relation REGCLASS, if_not_exists BOOL, refresh_start_offset "any", refresh_end_offset "any", compress_after "any", drop_after "any");

CREATE FUNCTION timescaledb_experimental.add_policies(
    relation REGCLASS,
    if_not_exists BOOL = false,
    refresh_start_offset "any" = NULL,
    refresh_end_offset "any" = NULL,
    compress_after "any" = NULL,
    drop_after "any" = NULL,
    hypercore_use_access_method BOOL = NULL)
RETURNS BOOL
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(job_id INTEGER, htid INTEGER, lag ANYELEMENT, maxchunks INTEGER, verbose_log BOOLEAN, recompress_enabled  BOOLEAN, use_creation_time BOOLEAN);

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression(job_id INTEGER, config JSONB);

CREATE PROCEDURE @extschema@.convert_to_columnstore(
    chunk REGCLASS,
    if_not_columnstore BOOLEAN = true,
    recompress BOOLEAN = false,
    hypercore_use_access_method BOOL = NULL)
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C;

CREATE PROCEDURE @extschema@.convert_to_rowstore(
    chunk REGCLASS,
    if_columnstore BOOLEAN = true)
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C;

CREATE PROCEDURE @extschema@.add_columnstore_policy(
    hypertable REGCLASS,
    after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    created_before INTERVAL = NULL,
    hypercore_use_access_method BOOL = NULL
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder';

CREATE PROCEDURE @extschema@.remove_columnstore_policy(
       hypertable REGCLASS,
       if_exists BOOL = false
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder';

CREATE FUNCTION @extschema@.chunk_columnstore_stats (hypertable REGCLASS)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE SQL
    STABLE STRICT
    AS 'SELECT * FROM @extschema@.chunk_compression_stats($1)'
    SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.hypertable_columnstore_stats (hypertable REGCLASS)
    RETURNS TABLE (
        total_chunks bigint,
        number_compressed_chunks bigint,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE SQL
    STABLE STRICT
    AS 'SELECT * FROM @extschema@.hypertable_compression_stats($1)'
    SET search_path TO pg_catalog, pg_temp;

-- Recreate `refresh_continuous_aggregate` procedure to add `force` argument
DROP PROCEDURE IF EXISTS @extschema@.refresh_continuous_aggregate (continuous_aggregate REGCLASS, window_start "any", window_end "any");

CREATE PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    window_start             "any",
    window_end               "any",
    force                    BOOLEAN = FALSE
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder';

-- Add `include_tiered_data` argument to `add_continuous_aggregate_policy`
DROP FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS, start_offset "any",
    end_offset "any", schedule_interval INTERVAL,
    if_not_exists BOOL,
    initial_start TIMESTAMPTZ,
    timezone TEXT
);
CREATE FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS, start_offset "any",
    end_offset "any", schedule_interval INTERVAL,
    if_not_exists BOOL = false,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
	include_tiered_data BOOL = NULL
)
RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

-- Merge chunks
CREATE PROCEDURE @extschema@.merge_chunks(
    chunk1 REGCLASS, chunk2 REGCLASS
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder';

CREATE PROCEDURE @extschema@.merge_chunks(
    chunks REGCLASS[]
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder';

CREATE FUNCTION ts_hypercore_handler(internal) RETURNS table_am_handler
AS '$libdir/timescaledb-2.20.3', 'ts_hypercore_handler' LANGUAGE C;

CREATE FUNCTION ts_hypercore_proxy_handler(internal) RETURNS index_am_handler
AS '$libdir/timescaledb-2.20.3', 'ts_hypercore_proxy_handler' LANGUAGE C;

CREATE ACCESS METHOD hypercore TYPE TABLE HANDLER ts_hypercore_handler;
COMMENT ON ACCESS METHOD hypercore IS 'Storage engine using hybrid row/columnar compression';

CREATE ACCESS METHOD hypercore_proxy TYPE INDEX HANDLER ts_hypercore_proxy_handler;
COMMENT ON ACCESS METHOD hypercore_proxy IS 'Hypercore proxy index access method';

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING hypercore_proxy AS
       OPERATOR 1 = (int4, int4),
       FUNCTION 1 hashint4(int4);
ALTER TABLE _timescaledb_internal.bgw_job_stat_history
    ALTER COLUMN succeeded DROP NOT NULL,
    ALTER COLUMN succeeded DROP DEFAULT;
CREATE FUNCTION _timescaledb_functions.compressed_data_has_nulls(_timescaledb_internal.compressed_data)
    RETURNS BOOL
    LANGUAGE C STRICT IMMUTABLE
    AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder';

INSERT INTO _timescaledb_catalog.compression_algorithm( id, version, name, description) values
( 5, 1, 'COMPRESSION_ALGORITHM_BOOL', 'bool'),
( 6, 1, 'COMPRESSION_ALGORITHM_NULL', 'null')
;

-------------------------------
-- Update compression settings
-------------------------------
CREATE TABLE _timescaledb_catalog.tempsettings (LIKE _timescaledb_catalog.compression_settings);
INSERT INTO _timescaledb_catalog.tempsettings SELECT * FROM _timescaledb_catalog.compression_settings;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.compression_settings;
DROP TABLE _timescaledb_catalog.compression_settings CASCADE;

CREATE TABLE _timescaledb_catalog.compression_settings (
  relid regclass NOT NULL,
  compress_relid regclass NULL,
  segmentby text[],
  orderby text[],
  orderby_desc bool[],
  orderby_nullsfirst bool[],
  CONSTRAINT compression_settings_pkey PRIMARY KEY (relid),
  CONSTRAINT compression_settings_check_segmentby CHECK (array_ndims(segmentby) = 1),
  CONSTRAINT compression_settings_check_orderby_null CHECK ((orderby IS NULL AND orderby_desc IS NULL AND orderby_nullsfirst IS NULL) OR (orderby IS NOT NULL AND orderby_desc IS NOT NULL AND orderby_nullsfirst IS NOT NULL)),
  CONSTRAINT compression_settings_check_orderby_cardinality CHECK (array_ndims(orderby) = 1 AND array_ndims(orderby_desc) = 1 AND array_ndims(orderby_nullsfirst) = 1 AND cardinality(orderby) = cardinality(orderby_desc) AND cardinality(orderby) = cardinality(orderby_nullsfirst))
);

-- Insert updated settings
INSERT INTO _timescaledb_catalog.compression_settings
SELECT
    CASE
        WHEN h.schema_name IS NOT NULL THEN
            cs.relid
        ELSE
            format('%I.%I', ch.schema_name, ch.table_name)::regclass
    END AS relid,
    CASE
        WHEN h.schema_name IS NOT NULL THEN
            NULL
        ELSE
            cs.relid
    END AS compress_relid,
    cs.segmentby,
    cs.orderby,
    cs.orderby_desc,
    cs.orderby_nullsfirst
FROM
    _timescaledb_catalog.tempsettings cs
INNER JOIN
    pg_class c ON (cs.relid = c.oid)
INNER JOIN
    pg_namespace ns ON (ns.oid = c.relnamespace)
LEFT JOIN
    _timescaledb_catalog.hypertable h ON (h.schema_name = ns.nspname AND h.table_name = c.relname)
LEFT JOIN
    _timescaledb_catalog.chunk cch ON (cch.schema_name = ns.nspname AND cch.table_name = c.relname)
LEFT JOIN
    _timescaledb_catalog.chunk ch ON (cch.id = ch.compressed_chunk_id);

-- Add index on secondary compressed relid key
CREATE INDEX compression_settings_compress_relid_idx ON _timescaledb_catalog.compression_settings (compress_relid);

DROP TABLE _timescaledb_catalog.tempsettings CASCADE;
GRANT SELECT ON _timescaledb_catalog.compression_settings TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_settings', '');


-- New add_continuous_aggregate_policy API for incremental refresh policy
DROP FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    start_offset "any",
    end_offset "any",
    schedule_interval INTERVAL,
    if_not_exists BOOL,
    initial_start TIMESTAMPTZ,
    timezone TEXT,
    include_tiered_data BOOL
);

CREATE FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    start_offset "any",
    end_offset "any",
    schedule_interval INTERVAL,
    if_not_exists BOOL = false,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
	include_tiered_data BOOL = NULL,
    buckets_per_batch INTEGER = NULL,
    max_batches_per_execution INTEGER = NULL
)
RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C VOLATILE;
-- Type for bloom filters used by the sparse indexes on compressed hypertables.
CREATE TYPE _timescaledb_internal.bloom1;

CREATE FUNCTION _timescaledb_functions.bloom1in(cstring) RETURNS _timescaledb_internal.bloom1 AS 'byteain' LANGUAGE INTERNAL STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_functions.bloom1out(_timescaledb_internal.bloom1) RETURNS cstring AS 'byteaout' LANGUAGE INTERNAL STRICT IMMUTABLE PARALLEL SAFE;

CREATE TYPE _timescaledb_internal.bloom1 (
    INPUT = _timescaledb_functions.bloom1in,
    OUTPUT = _timescaledb_functions.bloom1out,
    LIKE = bytea
);

CREATE FUNCTION _timescaledb_functions.bloom1_contains(_timescaledb_internal.bloom1, anyelement)
RETURNS bool
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;



DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_table;
DROP FUNCTION IF EXISTS _timescaledb_functions.create_chunk_table;


-- New option `refresh_newest_first` for incremental cagg refresh policy
DROP FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    start_offset "any",
    end_offset "any",
    schedule_interval INTERVAL,
    if_not_exists BOOL,
    initial_start TIMESTAMPTZ,
    timezone TEXT,
    include_tiered_data BOOL,
    buckets_per_batch INTEGER,
    max_batches_per_execution INTEGER
);

CREATE FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    start_offset "any",
    end_offset "any",
    schedule_interval INTERVAL,
    if_not_exists BOOL = false,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    include_tiered_data BOOL = NULL,
    buckets_per_batch INTEGER = NULL,
    max_batches_per_execution INTEGER = NULL,
    refresh_newest_first BOOL = NULL
)
RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

UPDATE _timescaledb_catalog.hypertable SET chunk_sizing_func_schema = '_timescaledb_functions' WHERE chunk_sizing_func_schema = '_timescaledb_internal' AND chunk_sizing_func_name = 'calculate_chunk_interval';

DROP VIEW IF EXISTS timescaledb_information.hypertables;

-- Rename Columnstore Policy jobs to Compression Policy
UPDATE _timescaledb_config.bgw_job SET application_name = replace(application_name, 'Compression Policy', 'Columnstore Policy') WHERE application_name LIKE '%Compression Policy%';

-- Split chunk
CREATE PROCEDURE @extschema@.split_chunk(
    chunk REGCLASS,
    split_at "any" = NULL
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder';

CREATE FUNCTION _timescaledb_functions.align_to_bucket(width INTERVAL, rng ANYRANGE)
RETURNS ANYRANGE AS
$body$
BEGIN
  RETURN _timescaledb_functions.make_range_from_internal_time(
         rng,
         @extschema@.time_bucket(width, lower(rng)),
         @extschema@.time_bucket(width, upper(rng) - '1 microsecond'::interval) + width
  );
END
$body$
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.make_multirange_from_internal_time(
    base TSTZRANGE, low_usec BIGINT, high_usec BIGINT
) RETURNS TSTZMULTIRANGE AS
$body$
  select multirange(tstzrange(_timescaledb_functions.to_timestamp(low_usec),
			      _timescaledb_functions.to_timestamp(high_usec)));
$body$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.make_multirange_from_internal_time(
    base TSRANGE, low_usec BIGINT, high_usec BIGINT
) RETURNS TSMULTIRANGE AS
$body$
  select multirange(tsrange(_timescaledb_functions.to_timestamp_without_timezone(low_usec),
			    _timescaledb_functions.to_timestamp_without_timezone(high_usec)));
$body$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.make_range_from_internal_time(
    base ANYRANGE, low_usec ANYELEMENT, high_usec ANYELEMENT
) RETURNS anyrange
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_internal_time_min(REGTYPE) RETURNS BIGINT
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_internal_time_max(REGTYPE) RETURNS BIGINT
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

DROP FUNCTION IF EXISTS @extschema@.add_job(
  proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB,
  initial_start TIMESTAMPTZ,
  scheduled BOOL,
  check_config REGPROC,
  fixed_schedule BOOL,
  timezone TEXT
);

CREATE FUNCTION @extschema@.add_job(
  proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB DEFAULT NULL,
  initial_start TIMESTAMPTZ DEFAULT NULL,
  scheduled BOOL DEFAULT true,
  check_config REGPROC DEFAULT NULL,
  fixed_schedule BOOL DEFAULT TRUE,
  timezone TEXT DEFAULT NULL,
  job_name TEXT DEFAULT NULL
)
RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS @extschema@.alter_job(
    job_id INTEGER,
    schedule_interval INTERVAL,
    max_runtime INTERVAL,
    max_retries INTEGER,
    retry_period INTERVAL,
    scheduled BOOL,
    config JSONB,
    next_start TIMESTAMPTZ,
    if_exists BOOL,
    check_config REGPROC,
    fixed_schedule BOOL,
    initial_start TIMESTAMPTZ,
    timezone TEXT
);

CREATE FUNCTION @extschema@.alter_job(
    job_id INTEGER,
    schedule_interval INTERVAL = NULL,
    max_runtime INTERVAL = NULL,
    max_retries INTEGER = NULL,
    retry_period INTERVAL = NULL,
    scheduled BOOL = NULL,
    config JSONB = NULL,
    next_start TIMESTAMPTZ = NULL,
    if_exists BOOL = FALSE,
    check_config REGPROC = NULL,
    fixed_schedule BOOL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT DEFAULT NULL,
    job_name TEXT DEFAULT NULL
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB,
next_start TIMESTAMPTZ, check_config TEXT, fixed_schedule BOOL, initial_start TIMESTAMPTZ, timezone TEXT, application_name name)
AS '$libdir/timescaledb-2.20.3', 'ts_update_placeholder'
LANGUAGE C VOLATILE;
-- Make chunk_id use NULL to mark special entries instead of 0
-- (Invalid chunk) since that doesn't work with the FK constraint on
-- chunk_id.
ALTER TABLE _timescaledb_catalog.chunk_column_stats ALTER COLUMN chunk_id DROP NOT NULL;
UPDATE _timescaledb_catalog.chunk_column_stats SET chunk_id = NULL WHERE chunk_id = 0;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


-- Functions have to be run in 2 places:
-- 1) In pre-install between types.pre.sql and types.post.sql to set up the types.
-- 2) On every update to make sure the function points to the correct versioned.so


-- PostgreSQL composite types do not support constraint checks. That is why any table having a ts_interval column must use the following
-- function for constraint validation.
-- This function needs to be defined before executing pre_install/tables.sql because it is used as
-- validation constraint for columns of type ts_interval.

--the textual input/output is simply base64 encoding of the binary representation
CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_in(CSTRING)
   RETURNS _timescaledb_internal.compressed_data
   AS '$libdir/timescaledb-2.20.3', 'ts_compressed_data_in'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_out(_timescaledb_internal.compressed_data)
   RETURNS CSTRING
   AS '$libdir/timescaledb-2.20.3', 'ts_compressed_data_out'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_send(_timescaledb_internal.compressed_data)
   RETURNS BYTEA
   AS '$libdir/timescaledb-2.20.3', 'ts_compressed_data_send'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_recv(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS '$libdir/timescaledb-2.20.3', 'ts_compressed_data_recv'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_info(_timescaledb_internal.compressed_data)
    RETURNS TABLE (algorithm name, has_nulls bool)
    LANGUAGE C STRICT IMMUTABLE
    AS '$libdir/timescaledb-2.20.3', 'ts_compressed_data_info';

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_data_has_nulls(_timescaledb_internal.compressed_data)
    RETURNS BOOL
    LANGUAGE C STRICT IMMUTABLE
    AS '$libdir/timescaledb-2.20.3', 'ts_compressed_data_has_nulls';

CREATE OR REPLACE FUNCTION _timescaledb_functions.dimension_info_in(cstring)
    RETURNS _timescaledb_internal.dimension_info
    LANGUAGE C STRICT IMMUTABLE
    AS '$libdir/timescaledb-2.20.3', 'ts_dimension_info_in';

CREATE OR REPLACE FUNCTION _timescaledb_functions.dimension_info_out(_timescaledb_internal.dimension_info)
    RETURNS cstring
    LANGUAGE C STRICT IMMUTABLE
    AS '$libdir/timescaledb-2.20.3', 'ts_dimension_info_out';


-- Type for bloom filters used by the sparse indexes on compressed hypertables.
CREATE OR REPLACE FUNCTION _timescaledb_functions.bloom1in(cstring) RETURNS _timescaledb_internal.bloom1 AS 'byteain' LANGUAGE INTERNAL STRICT IMMUTABLE PARALLEL SAFE;
CREATE OR REPLACE FUNCTION _timescaledb_functions.bloom1out(_timescaledb_internal.bloom1) RETURNS cstring AS 'byteaout' LANGUAGE INTERNAL STRICT IMMUTABLE PARALLEL SAFE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION ts_hypercore_handler(internal) RETURNS table_am_handler
AS '$libdir/timescaledb-2.20.3', 'ts_hypercore_handler' LANGUAGE C;

CREATE OR REPLACE FUNCTION ts_hypercore_proxy_handler(internal) RETURNS index_am_handler
AS '$libdir/timescaledb-2.20.3', 'ts_hypercore_proxy_handler' LANGUAGE C;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Trigger that blocks INSERTs on the hypertable's root table
CREATE OR REPLACE FUNCTION _timescaledb_functions.insert_blocker() RETURNS trigger
AS '$libdir/timescaledb-2.20.3', 'ts_hypertable_insert_blocker' LANGUAGE C;

-- Records mutations or INSERTs which would invalidate a continuous aggregate
CREATE OR REPLACE FUNCTION _timescaledb_functions.continuous_agg_invalidation_trigger() RETURNS TRIGGER
AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_invalidation_trigger' LANGUAGE C;

CREATE OR REPLACE FUNCTION @extschema@.set_integer_now_func(hypertable REGCLASS, integer_now_func REGPROC, replace_if_exists BOOL = false) RETURNS VOID
AS '$libdir/timescaledb-2.20.3', 'ts_hypertable_set_integer_now_func'
LANGUAGE C VOLATILE STRICT;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Built-in function for calculating the next chunk interval when
-- using adaptive chunking. The function can be replaced by a
-- user-defined function with the same signature.
--
-- The parameters passed to the function are as follows:
--
-- dimension_id: the ID of the dimension to calculate the interval for
-- dimension_coord: the coordinate / point on the dimensional axis
-- where the tuple that triggered this chunk creation falls.
-- chunk_target_size: the target size in bytes that the chunk should have.
--
-- The function should return the new interval in dimension-specific
-- time (ususally microseconds).
CREATE OR REPLACE FUNCTION _timescaledb_functions.calculate_chunk_interval(
        dimension_id INTEGER,
        dimension_coord BIGINT,
        chunk_target_size BIGINT
) RETURNS BIGINT AS '$libdir/timescaledb-2.20.3', 'ts_calculate_chunk_interval' LANGUAGE C;

-- Get the status of the chunk
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_status(REGCLASS) RETURNS INT
AS '$libdir/timescaledb-2.20.3', 'ts_chunk_status' LANGUAGE C;

--given a chunk's relid, return the id. Error out if not a chunk relid.
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_id_from_relid(relid OID) RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_chunk_id_from_relid' LANGUAGE C STABLE STRICT PARALLEL SAFE;

-- Show the definition of a chunk.
CREATE OR REPLACE FUNCTION _timescaledb_functions.show_chunk(chunk REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB)
AS '$libdir/timescaledb-2.20.3', 'ts_chunk_show' LANGUAGE C VOLATILE;

-- Create a chunk with the given dimensional constraints (slices) as
-- given in the JSONB. If chunk_table is a valid relation, it will be
-- attached to the hypertable and used as the data table for the new
-- chunk. Note that schema_name and table_name need not be the same as
-- the existing schema and name for chunk_table. The provided chunk
-- table will be renamed and/or moved as necessary.
CREATE OR REPLACE FUNCTION _timescaledb_functions.create_chunk(
       hypertable REGCLASS,
       slices JSONB,
       schema_name NAME = NULL,
       table_name NAME = NULL,
	   chunk_table REGCLASS = NULL)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB, created BOOLEAN)
AS '$libdir/timescaledb-2.20.3', 'ts_chunk_create' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.freeze_chunk(
   chunk REGCLASS)
RETURNS BOOL AS '$libdir/timescaledb-2.20.3', 'ts_chunk_freeze_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.unfreeze_chunk(
   chunk REGCLASS)
RETURNS BOOL AS '$libdir/timescaledb-2.20.3', 'ts_chunk_unfreeze_chunk' LANGUAGE C VOLATILE;

--wrapper for ts_chunk_drop
--drops the chunk table and its entry in the chunk catalog
CREATE OR REPLACE FUNCTION _timescaledb_functions.drop_chunk(
   chunk REGCLASS)
RETURNS BOOL AS '$libdir/timescaledb-2.20.3', 'ts_chunk_drop_single_chunk' LANGUAGE C VOLATILE;

-- internal API used by OSM extension to attach a table as a chunk of the hypertable
CREATE OR REPLACE FUNCTION _timescaledb_functions.attach_osm_table_chunk(
   hypertable REGCLASS,
   chunk REGCLASS)
RETURNS BOOL AS '$libdir/timescaledb-2.20.3', 'ts_chunk_attach_osm_table_chunk' LANGUAGE C VOLATILE;

-- internal API used by OSM extension to drop an OSM chunk table from the hypertable
CREATE OR REPLACE FUNCTION _timescaledb_functions.drop_osm_chunk(hypertable REGCLASS)
RETURNS BOOL AS '$libdir/timescaledb-2.20.3', 'ts_chunk_drop_osm_chunk' LANGUAGE C VOLATILE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--documentation of these function located in chunk_index.h
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_index_clone(chunk_index_oid OID) RETURNS OID
AS '$libdir/timescaledb-2.20.3', 'ts_chunk_index_clone' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_index_replace(chunk_index_oid_old OID, chunk_index_oid_new OID) RETURNS VOID
AS '$libdir/timescaledb-2.20.3', 'ts_chunk_index_replace' LANGUAGE C VOLATILE STRICT;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains utilities for time conversion.

-- Return the minimum for the type. For time types, it will be the
-- Unix timestamp in microseconds.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_internal_time_min(REGTYPE) RETURNS BIGINT
AS '$libdir/timescaledb-2.20.3', 'ts_get_internal_time_min' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Return the minimum for the type. For time types, it will be the
-- Unix timestamp in microseconds.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_internal_time_max(REGTYPE) RETURNS BIGINT
AS '$libdir/timescaledb-2.20.3', 'ts_get_internal_time_max' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.to_unix_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
    AS '$libdir/timescaledb-2.20.3', 'ts_pg_timestamp_to_unix_microseconds' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.to_timestamp(unixtime_us BIGINT) RETURNS TIMESTAMPTZ
    AS '$libdir/timescaledb-2.20.3', 'ts_pg_unix_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.to_timestamp_without_timezone(unixtime_us BIGINT)
  RETURNS TIMESTAMP
  AS '$libdir/timescaledb-2.20.3', 'ts_pg_unix_microseconds_to_timestamp'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.to_date(unixtime_us BIGINT)
  RETURNS DATE
  AS '$libdir/timescaledb-2.20.3', 'ts_pg_unix_microseconds_to_date'
  LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.to_interval(unixtime_us BIGINT) RETURNS INTERVAL
    AS '$libdir/timescaledb-2.20.3', 'ts_pg_unix_microseconds_to_interval' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Time can be represented in a hypertable as an int* (bigint/integer/smallint) or as a timestamp type (
-- with or without timezones). In metatables and other internal systems all time values are stored as bigint.
-- Converting from int* columns to internal representation is a cast to bigint.
-- Converting from timestamps to internal representation is conversion to epoch (in microseconds).

CREATE OR REPLACE FUNCTION _timescaledb_functions.interval_to_usec(
       chunk_interval INTERVAL
)
RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (int_sec * 1000000)::bigint from extract(epoch from chunk_interval) as int_sec;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.time_to_internal(time_val ANYELEMENT)
RETURNS BIGINT AS '$libdir/timescaledb-2.20.3', 'ts_time_to_internal' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_watermark(hypertable_id INTEGER)
RETURNS INT8 AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_watermark' LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_watermark_materialized(hypertable_id INTEGER)
RETURNS INT8 AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_watermark_materialized' LANGUAGE C STABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.subtract_integer_from_now( hypertable_relid REGCLASS, lag INT8 )
RETURNS INT8 AS '$libdir/timescaledb-2.20.3', 'ts_subtract_integer_from_now' LANGUAGE C STABLE STRICT;

-- Convert integer UNIX timestamps in microsecond to a timestamp range.
CREATE OR REPLACE FUNCTION _timescaledb_functions.make_multirange_from_internal_time(
    base tstzrange, low_usec bigint, high_usec bigint
) RETURNS TSTZMULTIRANGE AS
$body$
  select multirange(tstzrange(_timescaledb_functions.to_timestamp(low_usec),
			      _timescaledb_functions.to_timestamp(high_usec)));
$body$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

-- Convert integer UNIX timestamps in microsecond to a timestamp range.
CREATE OR REPLACE FUNCTION _timescaledb_functions.make_multirange_from_internal_time(
    base TSRANGE, low_usec bigint, high_usec bigint
) RETURNS TSMULTIRANGE AS
$body$
  select multirange(tsrange(_timescaledb_functions.to_timestamp_without_timezone(low_usec),
			    _timescaledb_functions.to_timestamp_without_timezone(high_usec)));
$body$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

-- Helper function to construct a range given an existing type from
-- UNIX timestamps in microsecond precision.
CREATE OR REPLACE FUNCTION _timescaledb_functions.make_range_from_internal_time(
    base anyrange, low_usec anyelement, high_usec anyelement
) RETURNS anyrange
AS '$libdir/timescaledb-2.20.3', 'ts_make_range_from_internal_time'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains functions associated with creating new
-- hypertables.

-- Outputs the create_hypertable command to recreate the given hypertable.
--
-- This is currently used internally for our single hypertable backup tool
-- so that it knows how to restore the hypertable without user intervention.
--
-- It only works for hypertables with up to 2 dimensions.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_create_command(
    table_name NAME
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    h_id             INTEGER;
    schema_name      NAME;
    time_column      NAME;
    time_interval    BIGINT;
    space_column     NAME;
    space_partitions INTEGER;
    dimension_cnt    INTEGER;
    dimension_row    record;
    ret              TEXT;
BEGIN
    SELECT h.id, h.schema_name
    FROM _timescaledb_catalog.hypertable AS h
    WHERE h.table_name = get_create_command.table_name
    INTO h_id, schema_name;

    IF h_id IS NULL THEN
        RAISE EXCEPTION 'hypertable "%" not found', table_name
        USING ERRCODE = 'TS101';
    END IF;

    SELECT COUNT(*)
    FROM _timescaledb_catalog.dimension d
    WHERE d.hypertable_id = h_id
    INTO STRICT dimension_cnt;

    IF dimension_cnt > 2 THEN
        RAISE EXCEPTION 'get_create_command only supports hypertables with up to 2 dimensions'
        USING ERRCODE = 'TS101';
    END IF;

    FOR dimension_row IN
        SELECT *
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = h_id
        LOOP
        IF dimension_row.interval_length IS NOT NULL THEN
            time_column := dimension_row.column_name;
            time_interval := dimension_row.interval_length;
        ELSIF dimension_row.num_slices IS NOT NULL THEN
            space_column := dimension_row.column_name;
            space_partitions := dimension_row.num_slices;
        END IF;
    END LOOP;

    ret := format($$SELECT create_hypertable('%I.%I', '%s'$$, schema_name, table_name, time_column);
    IF space_column IS NOT NULL THEN
        ret := ret || format($$, '%I', %s$$, space_column, space_partitions);
    END IF;
    ret := ret || format($$, chunk_time_interval => %s, create_default_indexes=>FALSE);$$, time_interval);

    RETURN ret;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- create constraint on newly created chunk based on hypertable constraint
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_constraint_add_table_constraint(
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    constraint_oid OID;
    constraint_type CHAR;
    check_sql TEXT;
    def TEXT;
    indx_tablespace NAME;
    tablespace_def TEXT;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;
    SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.id = chunk_row.hypertable_id;

    IF chunk_constraint_row.dimension_slice_id IS NOT NULL THEN
	    RAISE 'cannot create dimension constraint %', chunk_constraint_row;
    ELSIF chunk_constraint_row.hypertable_constraint_name IS NOT NULL THEN

        SELECT oid, contype INTO STRICT constraint_oid, constraint_type FROM pg_constraint
        WHERE conname=chunk_constraint_row.hypertable_constraint_name AND
              conrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass::oid;

        IF constraint_type IN ('p','u') THEN
          -- since primary keys and unique constraints are backed by an index
          -- they might have an index tablespace assigned
          -- the tablspace is not part of the constraint definition so
          -- we have to append it explicitly to preserve it
          SELECT T.spcname INTO indx_tablespace
          FROM pg_constraint C, pg_class I, pg_tablespace T
          WHERE C.oid = constraint_oid AND C.contype IN ('p', 'u') AND I.oid = C.conindid AND I.reltablespace = T.oid;

          def := pg_get_constraintdef(constraint_oid);

        ELSIF constraint_type = 't' THEN
          -- constraint triggers are copied separately with normal triggers
          def := NULL;
        ELSE
          def := pg_get_constraintdef(constraint_oid);
        END IF;

    ELSE
        RAISE 'unknown constraint type';
    END IF;

    IF def IS NOT NULL THEN
        -- to allow for custom types with operators outside of pg_catalog
        -- we set search_path to @extschema@
        SET LOCAL search_path TO @extschema@, pg_temp;
        EXECUTE pg_catalog.format(
            $$ ALTER TABLE %I.%I ADD CONSTRAINT %I %s $$,
            chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name, def
        );

        -- if constraint (primary or unique) needs a tablespace then add it
        -- via a separate ALTER INDEX SET TABLESPACE command. We cannot append it
        -- to the "def" string above since it leads to a SYNTAX error when
        -- "DEFERRABLE" or "INITIALLY DEFERRED" are used in the constraint
        IF indx_tablespace IS NOT NULL THEN
            EXECUTE pg_catalog.format(
                $$ ALTER INDEX %I.%I SET TABLESPACE %I $$,
                chunk_row.schema_name, chunk_constraint_row.constraint_name, indx_tablespace
            );
        END IF;

    END IF;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Clone fk constraint from a hypertable to a compressed chunk
CREATE OR REPLACE FUNCTION _timescaledb_functions.constraint_clone(
    constraint_oid OID,
    target_oid REGCLASS
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    constraint_name NAME;
    def TEXT;
BEGIN
    def := pg_get_constraintdef(constraint_oid);
    SELECT conname INTO STRICT constraint_name FROM pg_constraint WHERE oid = constraint_oid;

    IF def IS NULL THEN
        RAISE 'constraint not found';
    END IF;

    -- to allow for custom types with operators outside of pg_catalog
    -- we set search_path to @extschema@
    SET LOCAL search_path TO @extschema@, pg_temp;
    EXECUTE pg_catalog.format($$ ALTER TABLE %s ADD CONSTRAINT %I %s $$, target_oid::pg_catalog.text, constraint_name, def);

END
$BODY$ SET search_path TO pg_catalog, pg_temp;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Deprecated partition hash function
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_partition_for_key(val anyelement)
    RETURNS int
    AS '$libdir/timescaledb-2.20.3', 'ts_get_partition_for_key' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_partition_hash(val anyelement)
    RETURNS int
    AS '$libdir/timescaledb-2.20.3', 'ts_get_partition_hash' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file defines DDL functions for adding and manipulating hypertables.

-- Converts a regular postgres table to a hypertable.
--
-- relation - The OID of the table to be converted
-- time_column_name - Name of the column that contains time for a given record
-- partitioning_column - Name of the column to partition data by
-- number_partitions - (Optional) Number of partitions for data
-- associated_schema_name - (Optional) Schema for internal hypertable tables
-- associated_table_prefix - (Optional) Prefix for internal hypertable table names
-- chunk_time_interval - (Optional) Initial time interval for a chunk
-- create_default_indexes - (Optional) Whether or not to create the default indexes
-- if_not_exists - (Optional) Do not fail if table is already a hypertable
-- partitioning_func - (Optional) The partitioning function to use for spatial partitioning
-- migrate_data - (Optional) Set to true to migrate any existing data in the table to chunks
-- chunk_target_size - (Optional) The target size for chunks (e.g., '1000MB', 'estimate', or 'off')
-- chunk_sizing_func - (Optional) A function to calculate the chunk time interval for new chunks
-- time_partitioning_func - (Optional) The partitioning function to use for "time" partitioning
CREATE OR REPLACE FUNCTION @extschema@.create_hypertable(
    relation                REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    chunk_time_interval     ANYELEMENT = NULL::bigint,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    partitioning_func       REGPROC = NULL,
    migrate_data            BOOLEAN = FALSE,
    chunk_target_size       TEXT = NULL,
    chunk_sizing_func       REGPROC = '_timescaledb_functions.calculate_chunk_interval'::regproc,
    time_partitioning_func  REGPROC = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '$libdir/timescaledb-2.20.3', 'ts_hypertable_create' LANGUAGE C VOLATILE;

-- A generalized hypertable creation API that can be used to convert a PostgreSQL table
-- with TIME/SERIAL/BIGSERIAL columns to a hypertable.
--
-- relation - The OID of the table to be converted
-- dimension - The dimension to use for partitioning
-- create_default_indexes (Optional) Whether or not to create the default indexes
-- if_not_exists (Optional) Do not fail if table is already a hypertable
-- migrate_data (Optional) Set to true to migrate any existing data in the table to chunks
CREATE OR REPLACE FUNCTION @extschema@.create_hypertable(
    relation                REGCLASS,
    dimension               _timescaledb_internal.dimension_info,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    migrate_data            BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, created BOOL) AS '$libdir/timescaledb-2.20.3', 'ts_hypertable_create_general' LANGUAGE C VOLATILE;


-- Set adaptive chunking. To disable, set chunk_target_size => 'off'.
CREATE OR REPLACE FUNCTION @extschema@.set_adaptive_chunking(
    hypertable                     REGCLASS,
    chunk_target_size              TEXT,
    INOUT chunk_sizing_func        REGPROC = '_timescaledb_functions.calculate_chunk_interval'::regproc,
    OUT chunk_target_size          BIGINT
) RETURNS RECORD AS '$libdir/timescaledb-2.20.3', 'ts_chunk_adaptive_set' LANGUAGE C VOLATILE;

-- Update chunk_time_interval for a hypertable [DEPRECATED].
--
-- hypertable - The OID of the table corresponding to a hypertable whose time
--     interval should be updated
-- chunk_time_interval - The new time interval. For hypertables with integral
--     time columns, this must be an integral type. For hypertables with a
--     TIMESTAMP/TIMESTAMPTZ/DATE type, it can be integral which is treated as
--     microseconds, or an INTERVAL type.
CREATE OR REPLACE FUNCTION @extschema@.set_chunk_time_interval(
    hypertable              REGCLASS,
    chunk_time_interval     ANYELEMENT,
    dimension_name          NAME = NULL
) RETURNS VOID AS '$libdir/timescaledb-2.20.3', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;

-- Update partition_interval for a hypertable.
--
-- hypertable - The OID of the table corresponding to a hypertable whose
--     partition interval should be updated
-- partition_interval - The new interval. For hypertables with integral/serial/bigserial
--     time columns, this must be an integral type. For hypertables with a
--     TIMESTAMP/TIMESTAMPTZ/DATE type, it can be integral which is treated as
--     microseconds, or an INTERVAL type.
CREATE OR REPLACE FUNCTION @extschema@.set_partitioning_interval(
    hypertable              REGCLASS,
    partition_interval      ANYELEMENT,
    dimension_name          NAME = NULL
) RETURNS VOID AS '$libdir/timescaledb-2.20.3', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.set_number_partitions(
    hypertable              REGCLASS,
    number_partitions       INTEGER,
    dimension_name          NAME = NULL
) RETURNS VOID AS '$libdir/timescaledb-2.20.3', 'ts_dimension_set_num_slices' LANGUAGE C VOLATILE;

-- Drop chunks older than the given timestamp for the specific
-- hypertable or continuous aggregate.
CREATE OR REPLACE FUNCTION @extschema@.drop_chunks(
    relation               REGCLASS,
    older_than             "any" = NULL,
    newer_than             "any" = NULL,
    verbose                BOOLEAN = FALSE,
    created_before         "any" = NULL,
    created_after          "any" = NULL
) RETURNS SETOF TEXT AS '$libdir/timescaledb-2.20.3', 'ts_chunk_drop_chunks'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

-- show chunks older than or newer than a specific time.
-- `relation` must be a valid hypertable or continuous aggregate.
CREATE OR REPLACE FUNCTION @extschema@.show_chunks(
    relation               REGCLASS,
    older_than             "any" = NULL,
    newer_than             "any" = NULL,
    created_before         "any" = NULL,
    created_after          "any" = NULL
) RETURNS SETOF REGCLASS AS '$libdir/timescaledb-2.20.3', 'ts_chunk_show_chunks'
LANGUAGE C STABLE PARALLEL SAFE;

-- Add a dimension (of partitioning) to a hypertable [DEPRECATED]
--
-- hypertable - OID of the table to add a dimension to
-- column_name - NAME of the column to use in partitioning for this dimension
-- number_partitions - Number of partitions, for non-time dimensions
-- chunk_time_interval - Size of intervals for time dimensions (can be integral or INTERVAL)
-- partitioning_func - Function used to partition the column
-- if_not_exists - If set, and the dimension already exists, generate a notice instead of an error
CREATE OR REPLACE FUNCTION @extschema@.add_dimension(
    hypertable              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    chunk_time_interval     ANYELEMENT = NULL::BIGINT,
    partitioning_func       REGPROC = NULL,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, schema_name NAME, table_name NAME, column_name NAME, created BOOL)
AS '$libdir/timescaledb-2.20.3', 'ts_dimension_add' LANGUAGE C VOLATILE;

-- Add a dimension (of partitioning) to a hypertable.
--
-- hypertable - OID of the table to add a dimension to
-- dimension - Dimension to add
-- if_not_exists - If set, and the dimension already exists, generate a notice instead of an error
CREATE OR REPLACE FUNCTION @extschema@.add_dimension(
    hypertable              REGCLASS,
    dimension               _timescaledb_internal.dimension_info,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, created BOOL)
AS '$libdir/timescaledb-2.20.3', 'ts_dimension_add_general' LANGUAGE C VOLATILE;

-- Enable tracking of statistics on a column of a hypertable.
--
-- hypertable - OID of the table to which the column belongs to
-- column_name - The column to track statistics for
-- if_not_exists - If set, and the entry already exists, generate a notice instead of an error
-- Returns the "id" of the entry created. The "enabled" field
-- is set to true if entry is created or exists already.
CREATE OR REPLACE FUNCTION @extschema@.enable_chunk_skipping(
    hypertable              REGCLASS,
    column_name             NAME,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(column_stats_id INT, enabled BOOL)
AS '$libdir/timescaledb-2.20.3', 'ts_chunk_column_stats_enable' LANGUAGE C VOLATILE;

-- Disable tracking of statistics on a column of a hypertable.
--
-- hypertable - OID of the table to remove from
-- column_name - NAME of the column on which the stats are tracked
-- if_not_exists - If set, and the entry does not exist,
-- generate a notice instead of an error. The "disabled" field
-- is set to true if entry is deleted successfully.
CREATE OR REPLACE FUNCTION @extschema@.disable_chunk_skipping(
    hypertable              REGCLASS,
    column_name             NAME,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, column_name NAME, disabled BOOL)
AS '$libdir/timescaledb-2.20.3', 'ts_chunk_column_stats_disable' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.by_hash(column_name NAME, number_partitions INTEGER,
                                               partition_func regproc = NULL)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '$libdir/timescaledb-2.20.3', 'ts_hash_dimension';

CREATE OR REPLACE FUNCTION @extschema@.by_range(column_name NAME,
                                                partition_interval ANYELEMENT = NULL::bigint,
                                                partition_func regproc = NULL)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '$libdir/timescaledb-2.20.3', 'ts_range_dimension';

CREATE OR REPLACE FUNCTION @extschema@.attach_tablespace(
    tablespace NAME,
    hypertable REGCLASS,
    if_not_attached BOOLEAN = false
) RETURNS VOID
AS '$libdir/timescaledb-2.20.3', 'ts_tablespace_attach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.detach_tablespace(
    tablespace NAME,
    hypertable REGCLASS = NULL,
    if_attached BOOLEAN = false
) RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_tablespace_detach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.detach_tablespaces(hypertable REGCLASS) RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_tablespace_detach_all_from_hypertable' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.show_tablespaces(hypertable REGCLASS) RETURNS SETOF NAME
AS '$libdir/timescaledb-2.20.3', 'ts_tablespace_show' LANGUAGE C VOLATILE STRICT;

-- Refresh a continuous aggregate across the given window.
CREATE OR REPLACE PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    window_start             "any",
    window_end               "any",
    force                    BOOLEAN = FALSE
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_refresh';

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_command_end;

CREATE OR REPLACE FUNCTION _timescaledb_functions.process_ddl_event() RETURNS event_trigger
AS '$libdir/timescaledb-2.20.3', 'ts_timescaledb_process_ddl_event' LANGUAGE C;

--EVENT TRIGGER MUST exclude the ALTER EXTENSION tag.
CREATE EVENT TRIGGER timescaledb_ddl_command_end ON ddl_command_end
WHEN TAG IN ('ALTER TABLE','CREATE TRIGGER','CREATE TABLE','CREATE INDEX','ALTER INDEX', 'DROP TABLE', 'DROP INDEX', 'DROP SCHEMA')
EXECUTE FUNCTION _timescaledb_functions.process_ddl_event();

DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_sql_drop;
CREATE EVENT TRIGGER timescaledb_ddl_sql_drop ON sql_drop
EXECUTE FUNCTION _timescaledb_functions.process_ddl_event();
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.first_sfunc(internal, anyelement, "any")
RETURNS internal
AS '$libdir/timescaledb-2.20.3', 'ts_first_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.first_combinefunc(internal, internal)
RETURNS internal
AS '$libdir/timescaledb-2.20.3', 'ts_first_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.last_sfunc(internal, anyelement, "any")
RETURNS internal
AS '$libdir/timescaledb-2.20.3', 'ts_last_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.last_combinefunc(internal, internal)
RETURNS internal
AS '$libdir/timescaledb-2.20.3', 'ts_last_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.bookend_finalfunc(internal, anyelement, "any")
RETURNS anyelement
AS '$libdir/timescaledb-2.20.3', 'ts_bookend_finalfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.bookend_serializefunc(internal)
RETURNS bytea
AS '$libdir/timescaledb-2.20.3', 'ts_bookend_serializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.bookend_deserializefunc(bytea, internal)
RETURNS internal
AS '$libdir/timescaledb-2.20.3', 'ts_bookend_deserializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;


-- We started using CREATE OR REPLACE AGGREGATE for aggregate creation once the syntax was fully supported
-- as it is easier to support idempotent changes this way. This will allow for changes to functions supporting
-- the aggregate, and, for instance, the definition and inclusion of inverse functions for window function
-- support. However, it should still be noted that changes to the data structures used for the internal
-- state of the aggregate must be backwards compatible and the old format must be accepted by any new functions
-- in order for them to continue working with Continuous Aggregates, where old states may have been materialized.

--This aggregate returns the "first" value of the first argument when ordered by the second argument.
--Ex. first(temp, time) returns the temp value for the row with the lowest time
CREATE OR REPLACE AGGREGATE @extschema@.first(anyelement, "any") (
    SFUNC = _timescaledb_functions.first_sfunc,
    STYPE = internal,
    COMBINEFUNC = _timescaledb_functions.first_combinefunc,
    SERIALFUNC = _timescaledb_functions.bookend_serializefunc,
    DESERIALFUNC = _timescaledb_functions.bookend_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = _timescaledb_functions.bookend_finalfunc,
    FINALFUNC_EXTRA
);

--This aggregate returns the "last" value of the first argument when ordered by the second argument.
--Ex. last(temp, time) returns the temp value for the row with highest time
CREATE OR REPLACE AGGREGATE @extschema@.last(anyelement, "any") (
    SFUNC = _timescaledb_functions.last_sfunc,
    STYPE = internal,
    COMBINEFUNC = _timescaledb_functions.last_combinefunc,
    SERIALFUNC = _timescaledb_functions.bookend_serializefunc,
    DESERIALFUNC = _timescaledb_functions.bookend_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = _timescaledb_functions.bookend_finalfunc,
    FINALFUNC_EXTRA
);
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- time_bucket returns the left edge of the bucket where ts falls into.
-- Buckets span an interval of time equal to the bucket_width and are aligned with the epoch.
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
	AS '$libdir/timescaledb-2.20.3', 'ts_timestamp_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- bucketing of timestamptz happens at UTC time
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb-2.20.3', 'ts_timestamptz_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

--bucketing on date should not do any timezone conversion
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts DATE) RETURNS DATE
	AS '$libdir/timescaledb-2.20.3', 'ts_date_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

--bucketing with origin
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP) RETURNS TIMESTAMP
	AS '$libdir/timescaledb-2.20.3', 'ts_timestamp_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb-2.20.3', 'ts_timestamptz_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts DATE, origin DATE) RETURNS DATE
	AS '$libdir/timescaledb-2.20.3', 'ts_date_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

--bucketing with offset
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMP, "offset" INTERVAL) RETURNS TIMESTAMP
	AS '$libdir/timescaledb-2.20.3', 'ts_timestamp_offset_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, "offset" INTERVAL) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb-2.20.3', 'ts_timestamptz_offset_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts DATE, "offset" INTERVAL) RETURNS DATE
	AS '$libdir/timescaledb-2.20.3', 'ts_date_offset_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- bucketing with timezone
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, origin TIMESTAMPTZ DEFAULT NULL, "offset" INTERVAL DEFAULT NULL) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb-2.20.3', 'ts_timestamptz_timezone_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- bucketing of int
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width SMALLINT, ts SMALLINT) RETURNS SMALLINT
	AS '$libdir/timescaledb-2.20.3', 'ts_int16_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INT, ts INT) RETURNS INT
	AS '$libdir/timescaledb-2.20.3', 'ts_int32_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width BIGINT, ts BIGINT) RETURNS BIGINT
	AS '$libdir/timescaledb-2.20.3', 'ts_int64_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- bucketing of int with offset
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width SMALLINT, ts SMALLINT, "offset" SMALLINT) RETURNS SMALLINT
	AS '$libdir/timescaledb-2.20.3', 'ts_int16_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width INT, ts INT, "offset" INT) RETURNS INT
	AS '$libdir/timescaledb-2.20.3', 'ts_int32_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION @extschema@.time_bucket(bucket_width BIGINT, ts BIGINT, "offset" BIGINT) RETURNS BIGINT
	AS '$libdir/timescaledb-2.20.3', 'ts_int64_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- This will align a range to a bucket size. It is similar to
-- time_bucket(), but takes a range and produces a range that starts
-- and ends at bucket boundaries.
CREATE OR REPLACE FUNCTION _timescaledb_functions.align_to_bucket(width interval, rng anyrange)
RETURNS anyrange AS
$body$
BEGIN
  RETURN _timescaledb_functions.make_range_from_internal_time(
         rng,
         @extschema@.time_bucket(width, lower(rng)),
         @extschema@.time_bucket(width, upper(rng) - '1 microsecond'::interval) + width
  );
END
$body$
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
SET search_path TO pg_catalog, pg_temp;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- time_bucket_ng() is an _experimental_ new version of time_bucket().
--
-- Unlike time_bucket(), time_bucket_ng() supports variable-sized buckets,
-- such as months and years, and also timezones. Note that the behavior
-- and the interface of this function are subjects to change. There could
-- be bugs, and the implementation doesn't claim to be complete. Use at
-- your own risk.
--
-- This function may return different results for the same arguments depending
-- on the version of local timezone database. Despite this fact, function is
-- marked as IMMUTABLE. This is consistent with the volatility [1] of the
-- functions provided by PostgreSQL. See discussion [2] for more details.
--
-- We don't forbid users to work with timestamptz's from the future, nor warn
-- about this corner case. This behavior is consistent with PostgreSQL
-- behavior [3].
--
-- [1]: https://www.postgresql.org/docs/current/xfunc-volatility.html
-- [2]: https://postgr.es/m/CAJ7c6TOMG8zSNEZtCn5SPe+cCk3Lfxb71ZaQwT2F4T7PJ_t=KA@mail.gmail.com
-- [3]: https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-TIMEZONES

-- DATE versions of time_bucket_ng().
CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE) RETURNS DATE
    AS '$libdir/timescaledb-2.20.3', 'ts_time_bucket_ng_date' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE, origin DATE) RETURNS DATE
    AS '$libdir/timescaledb-2.20.3', 'ts_time_bucket_ng_date' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- TIMESTAMP versions of time_bucket_ng().
CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
    AS '$libdir/timescaledb-2.20.3', 'ts_time_bucket_ng_timestamp' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP) RETURNS TIMESTAMP
    AS '$libdir/timescaledb-2.20.3', 'ts_time_bucket_ng_timestamp' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- TIMESTAMPTZ versions of time_bucket_ng().
CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT) RETURNS TIMESTAMPTZ
    AS '$libdir/timescaledb-2.20.3', 'ts_time_bucket_ng_timezone' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ, timezone TEXT) RETURNS TIMESTAMPTZ
    AS '$libdir/timescaledb-2.20.3', 'ts_time_bucket_ng_timezone_origin' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;


-- The following two versions of time_bucket_ng() are kept only for the backward
-- compatibility with time_bucket(). They convert 'ts' to UTC instead of treating
-- it in the given timezone, which is almost certainly not something you want.
-- Future versions may WARN you about this fact, and be completely removed
-- eventually.
--
-- These functions are STABLE because their implementation relies on the STABLE
-- function timestamptz_date(). The latest is STABLE because it accounts for the
-- session parameters.
CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
    AS '$libdir/timescaledb-2.20.3', 'ts_time_bucket_ng_timestamptz' LANGUAGE C STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ) RETURNS TIMESTAMPTZ
    AS '$libdir/timescaledb-2.20.3', 'ts_time_bucket_ng_timestamptz' LANGUAGE C STABLE PARALLEL SAFE STRICT;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_git_commit()
    RETURNS TABLE(commit_tag TEXT, commit_hash TEXT, commit_time TIMESTAMPTZ)
    AS '$libdir/timescaledb-2.20.3', 'ts_get_git_commit' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_os_info()
    RETURNS TABLE(sysname TEXT, version TEXT, release TEXT, version_pretty TEXT)
    AS '$libdir/timescaledb-2.20.3', 'ts_get_os_info' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.tsl_loaded() RETURNS BOOLEAN
AS '$libdir/timescaledb-2.20.3', 'ts_tsl_loaded' LANGUAGE C;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains utility functions to get the relation size
-- of hypertables, chunks, and indexes on hypertables.

CREATE OR REPLACE FUNCTION _timescaledb_functions.relation_size(relation REGCLASS)
RETURNS TABLE (total_size BIGINT, heap_size BIGINT, index_size BIGINT, toast_size BIGINT)
AS '$libdir/timescaledb-2.20.3', 'ts_relation_size' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.relation_approximate_size(relation REGCLASS)
RETURNS TABLE (total_size BIGINT, heap_size BIGINT, index_size BIGINT, toast_size BIGINT)
AS '$libdir/timescaledb-2.20.3', 'ts_relation_approximate_size' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE VIEW _timescaledb_internal.hypertable_chunk_local_size AS
SELECT
    h.schema_name AS hypertable_schema,
    h.table_name AS hypertable_name,
    h.id AS hypertable_id,
    c.id AS chunk_id,
    c.schema_name AS chunk_schema,
    c.table_name AS chunk_name,
    COALESCE((relsize).total_size, 0) AS total_bytes,
    COALESCE((relsize).heap_size, 0) AS heap_bytes,
    COALESCE((relsize).index_size, 0) AS index_bytes,
    COALESCE((relsize).toast_size, 0) AS toast_bytes,
    COALESCE((relcompsize).total_size, 0) AS compressed_total_size,
    COALESCE((relcompsize).heap_size, 0) AS compressed_heap_size,
    COALESCE((relcompsize).index_size, 0) AS compressed_index_size,
    COALESCE((relcompsize).toast_size, 0) AS compressed_toast_size
FROM
    _timescaledb_catalog.hypertable h
    JOIN _timescaledb_catalog.chunk c ON h.id = c.hypertable_id
        AND c.dropped IS FALSE
    JOIN pg_class cl ON cl.relname = c.table_name AND cl.relkind = 'r'
    JOIN pg_namespace n ON n.oid = cl.relnamespace
    AND n.nspname = c.schema_name
    JOIN LATERAL _timescaledb_functions.relation_size(cl.oid) AS relsize ON TRUE
    LEFT JOIN _timescaledb_catalog.chunk comp ON comp.id = c.compressed_chunk_id
    LEFT JOIN LATERAL _timescaledb_functions.relation_size(
        CASE WHEN comp.schema_name IS NOT NULL AND comp.table_name IS NOT NULL THEN
            format('%I.%I', comp.schema_name, comp.table_name)::regclass
        ELSE
            NULL::regclass
        END
        ) AS relcompsize ON TRUE;

GRANT SELECT ON  _timescaledb_internal.hypertable_chunk_local_size TO PUBLIC;

CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_local_size(
	schema_name_in name,
	table_name_in name)
RETURNS TABLE (
	table_bytes BIGINT,
	index_bytes BIGINT,
	toast_bytes BIGINT,
	total_bytes BIGINT)
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
    /* get the main hypertable id and sizes */
    WITH _hypertable_sizes AS (
        SELECT
            id,
            COALESCE((relsize).total_size, 0) AS total_bytes,
            COALESCE((relsize).heap_size, 0) AS heap_bytes,
            COALESCE((relsize).index_size, 0) AS index_bytes,
            COALESCE((relsize).toast_size, 0) AS toast_bytes,
            0::BIGINT AS compressed_total_size,
            0::BIGINT AS compressed_index_size,
            0::BIGINT AS compressed_toast_size,
            0::BIGINT AS compressed_heap_size
        FROM
            _timescaledb_catalog.hypertable ht
            JOIN pg_class c ON relname = ht.table_name AND c.relkind = 'r'
            JOIN pg_namespace n ON n.oid = c.relnamespace
            AND n.nspname = ht.schema_name
            JOIN LATERAL _timescaledb_functions.relation_size(c.oid) AS relsize ON TRUE
        WHERE
            schema_name = schema_name_in
            AND table_name = table_name_in
    ),
    /* calculate the size of the hypertable chunks */
    _chunk_sizes AS (
        SELECT
            chunk_id,
            COALESCE(ch.total_bytes, 0) AS total_bytes,
            COALESCE(ch.heap_bytes, 0) AS heap_bytes,
            COALESCE(ch.index_bytes, 0) AS index_bytes,
            COALESCE(ch.toast_bytes, 0) AS toast_bytes,
            COALESCE(ch.compressed_total_size, 0) AS compressed_total_size,
            COALESCE(ch.compressed_index_size, 0) AS compressed_index_size,
            COALESCE(ch.compressed_toast_size, 0) AS compressed_toast_size,
            COALESCE(ch.compressed_heap_size, 0) AS compressed_heap_size
        FROM
            _timescaledb_internal.hypertable_chunk_local_size ch
            JOIN _hypertable_sizes ht ON ht.id = ch.hypertable_id
        WHERE hypertable_schema = schema_name_in
          AND hypertable_name = table_name_in
    )
    /* calculate the SUM of the hypertable and chunk sizes */
	SELECT
		(SUM(heap_bytes)  + SUM(compressed_heap_size))::BIGINT AS heap_bytes,
		(SUM(index_bytes) + SUM(compressed_index_size))::BIGINT AS index_bytes,
		(SUM(toast_bytes) + SUM(compressed_toast_size))::BIGINT AS toast_bytes,
		(SUM(total_bytes) + SUM(compressed_total_size))::BIGINT AS total_bytes
	FROM
		(SELECT * FROM _hypertable_sizes
         UNION ALL
         SELECT * FROM _chunk_sizes) AS sizes;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Get relation size of hypertable
-- like pg_relation_size(hypertable)
--
-- hypertable - hypertable to get size of
--
-- Returns:
-- table_bytes        - Disk space used by hypertable (like pg_relation_size(hypertable))
-- index_bytes        - Disk space used by indexes
-- toast_bytes        - Disk space of toast tables
-- total_bytes        - Total disk space used by the specified table, including all indexes and TOAST data

CREATE OR REPLACE FUNCTION @extschema@.hypertable_detailed_size(
    hypertable              REGCLASS)
RETURNS TABLE (table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               node_name   NAME)
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
        table_name       NAME = NULL;
        schema_name      NAME = NULL;
BEGIN
        SELECT relname, nspname
        INTO table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        INNER JOIN _timescaledb_catalog.hypertable ht ON (ht.schema_name = n.nspname AND ht.table_name = c.relname)
        WHERE c.OID = hypertable;

        IF table_name IS NULL THEN
                SELECT h.schema_name, h.table_name
                INTO schema_name, table_name
                FROM pg_class c
                INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
                INNER JOIN _timescaledb_catalog.continuous_agg a ON (a.user_view_schema = n.nspname AND a.user_view_name = c.relname)
                INNER JOIN _timescaledb_catalog.hypertable h ON h.id = a.mat_hypertable_id
                WHERE c.OID = hypertable;

	        IF table_name IS NULL THEN
                        RETURN;
                END IF;
        END IF;

			RETURN QUERY
			SELECT *, NULL::name
			FROM _timescaledb_functions.hypertable_local_size(schema_name, table_name);
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

--- returns total-bytes for a hypertable (includes table + index)
CREATE OR REPLACE FUNCTION @extschema@.hypertable_size(
    hypertable              REGCLASS)
RETURNS BIGINT
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
   SELECT total_bytes::bigint FROM @extschema@.hypertable_detailed_size(hypertable);
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Get approximate relation size of hypertable
--
-- hypertable - hypertable to get approximate size of
--
-- Returns:
-- table_bytes        - Approximate disk space used by hypertable
-- index_bytes        - Approximate disk space used by indexes
-- toast_bytes        - Approximate disk space of toast tables
-- total_bytes        - Total approximate disk space used by the specified table, including all indexes and TOAST data
CREATE OR REPLACE FUNCTION @extschema@.hypertable_approximate_detailed_size(relation REGCLASS)
RETURNS TABLE (table_bytes BIGINT, index_bytes BIGINT, toast_bytes BIGINT, total_bytes BIGINT)
AS '$libdir/timescaledb-2.20.3', 'ts_hypertable_approximate_size' LANGUAGE C VOLATILE;

--- returns approximate total-bytes for a hypertable (includes table + index)
CREATE OR REPLACE FUNCTION @extschema@.hypertable_approximate_size(
    hypertable              REGCLASS)
RETURNS BIGINT
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
   SELECT sum(total_bytes)::bigint
   FROM @extschema@.hypertable_approximate_detailed_size(hypertable);
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.chunks_local_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    chunk_id    integer,
    chunk_schema NAME,
    chunk_name  NAME,
    table_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint)
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
   SELECT
      ch.chunk_id,
      ch.chunk_schema,
      ch.chunk_name,
      (ch.total_bytes - COALESCE( ch.index_bytes , 0 ) - COALESCE( ch.toast_bytes, 0 ) + COALESCE( ch.compressed_heap_size , 0 ))::bigint  as heap_bytes,
      (COALESCE( ch.index_bytes, 0 ) + COALESCE( ch.compressed_index_size , 0) )::bigint as index_bytes,
      (COALESCE( ch.toast_bytes, 0 ) + COALESCE( ch.compressed_toast_size, 0 ))::bigint as toast_bytes,
      (ch.total_bytes + COALESCE( ch.compressed_total_size, 0 ))::bigint as total_bytes
   FROM
	  _timescaledb_internal.hypertable_chunk_local_size ch
   WHERE
      ch.hypertable_schema = schema_name_in
      AND ch.hypertable_name = table_name_in;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Get relation size of the chunks of an hypertable
-- hypertable - hypertable to get size of
--
-- Returns:
-- chunk_schema                  - schema name for chunk
-- chunk_name                    - chunk table name
-- table_bytes                   - Disk space used by chunk table
-- index_bytes                   - Disk space used by indexes
-- toast_bytes                   - Disk space of toast tables
-- total_bytes                   - Disk space used in total
-- node_name                     - node on which chunk lives if this is
--                              a distributed hypertable.
CREATE OR REPLACE FUNCTION @extschema@.chunks_detailed_size(
    hypertable              REGCLASS
)
RETURNS TABLE (
               chunk_schema NAME,
               chunk_name NAME,
               table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               node_name   NAME)
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        INNER JOIN _timescaledb_catalog.hypertable ht ON (ht.schema_name = n.nspname AND ht.table_name = c.relname)
        WHERE c.OID = hypertable;

        IF table_name IS NULL THEN
            SELECT h.schema_name, h.table_name
            INTO schema_name, table_name
            FROM pg_class c
            INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
            INNER JOIN _timescaledb_catalog.continuous_agg a ON (a.user_view_schema = n.nspname AND a.user_view_name = c.relname)
            INNER JOIN _timescaledb_catalog.hypertable h ON h.id = a.mat_hypertable_id
            WHERE c.OID = hypertable;

            IF table_name IS NULL THEN
                RETURN;
            END IF;
		END IF;

    RETURN QUERY SELECT chl.chunk_schema, chl.chunk_name, chl.table_bytes, chl.index_bytes,
                        chl.toast_bytes, chl.total_bytes, NULL::NAME
            FROM _timescaledb_functions.chunks_local_size(schema_name, table_name) chl;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;
---------- end of detailed size functions ------

CREATE OR REPLACE FUNCTION _timescaledb_functions.range_value_to_pretty(
    time_value      BIGINT,
    column_type     REGTYPE
)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
    IF NOT (time_value > (-9223372036854775808)::bigint AND
	   	    time_value < 9223372036854775807::bigint) THEN
        RETURN '';
    END IF;
    IF time_value IS NULL THEN
        RETURN format('%L', NULL);
    END IF;
    CASE column_type
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%L', time_value); -- scale determined by user.
      WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype THEN
        -- assume time_value is in microsec
        RETURN format('%1$L', _timescaledb_functions.to_timestamp(time_value)); -- microseconds
      WHEN 'DATE'::regtype THEN
        RETURN format('%L', timezone('UTC',_timescaledb_functions.to_timestamp(time_value))::date);
      ELSE
        RETURN time_value;
    END CASE;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Convenience function to return approximate row count
--
-- relation - table or hypertable to get approximate row count for
--
-- Returns:
-- Estimated number of rows according to catalog tables
CREATE OR REPLACE FUNCTION @extschema@.approximate_row_count(relation REGCLASS)
RETURNS BIGINT
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
    mat_ht           REGCLASS = NULL;
    local_table_name       NAME = NULL;
    local_schema_name      NAME = NULL;
    is_compressed    BOOL = FALSE;
    uncompressed_row_count BIGINT = 0;
    compressed_row_count BIGINT = 0;
    local_compressed_hypertable_id INTEGER = 0;
    local_compressed_chunk_id INTEGER = 0;
    compressed_hypertable_oid  OID;
    local_compressed_chunk_oid  OID;
    max_compressed_row_count BIGINT = 1000;
    is_compressed_chunk INTEGER;
BEGIN
    -- Check if input relation is continuous aggregate view then
    -- get the corresponding materialized hypertable and schema name
    SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass
    INTO mat_ht
    FROM pg_class c
    JOIN pg_namespace n ON (n.OID = c.relnamespace)
    JOIN _timescaledb_catalog.continuous_agg a ON (a.user_view_schema = n.nspname AND a.user_view_name = c.relname)
    JOIN _timescaledb_catalog.hypertable ht ON (a.mat_hypertable_id = ht.id)
    WHERE c.OID = relation;

    IF mat_ht IS NOT NULL THEN
        relation = mat_ht;
    END IF;

    SELECT relname, nspname FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    INTO local_table_name, local_schema_name
    WHERE c.OID = relation;

    -- Check for input relation is Hypertable
    IF EXISTS (SELECT 1
               FROM _timescaledb_catalog.hypertable WHERE table_name = local_table_name AND schema_name = local_schema_name) THEN
        SELECT compressed_hypertable_id FROM _timescaledb_catalog.hypertable INTO local_compressed_hypertable_id
        WHERE table_name = local_table_name AND schema_name = local_schema_name;
        IF local_compressed_hypertable_id IS NOT NULL THEN
           uncompressed_row_count = _timescaledb_functions.get_approx_row_count(relation);

           -- use the compression_chunk_size stats to fetch precompressed num rows
           SELECT COALESCE(SUM(numrows_pre_compression), 0) FROM _timescaledb_catalog.chunk srcch,
                _timescaledb_catalog.compression_chunk_size map, _timescaledb_catalog.hypertable srcht
                INTO compressed_row_count
                WHERE map.chunk_id = srcch.id
                AND srcht.id = srcch.hypertable_id AND srcht.table_name = local_table_name
                AND srcht.schema_name = local_schema_name;

           RETURN (uncompressed_row_count + compressed_row_count);
        ELSE
           uncompressed_row_count = _timescaledb_functions.get_approx_row_count(relation);
           RETURN uncompressed_row_count;
        END IF;
    END IF;
    -- Check for input relation is CHUNK
    IF EXISTS (SELECT 1 FROM _timescaledb_catalog.chunk WHERE table_name = local_table_name AND schema_name = local_schema_name) THEN
        with compressed_chunk as (select 1 as is_compressed_chunk from _timescaledb_catalog.chunk c
        inner join _timescaledb_catalog.hypertable h on (c.hypertable_id = h.compressed_hypertable_id)
        where c.table_name = local_table_name and c.schema_name = local_schema_name ),
        chunk_temp as (select compressed_chunk_id from _timescaledb_catalog.chunk c where c.table_name = local_table_name and c.schema_name = local_schema_name)
        select ct.compressed_chunk_id, cc.is_compressed_chunk from chunk_temp ct LEFT OUTER JOIN compressed_chunk cc ON 1 = 1
        INTO local_compressed_chunk_id, is_compressed_chunk;
        -- 'input is chunk #1';
        IF is_compressed_chunk IS NULL AND local_compressed_chunk_id IS NOT NULL THEN
        -- 'Include both uncompressed  and compressed chunk #2';
            -- use the compression_chunk_size stats to fetch precompressed num rows
            SELECT COALESCE(numrows_pre_compression, 0) FROM _timescaledb_catalog.compression_chunk_size
                INTO compressed_row_count
                WHERE compressed_chunk_id = local_compressed_chunk_id;

            uncompressed_row_count = _timescaledb_functions.get_approx_row_count(relation);
            RETURN (uncompressed_row_count + compressed_row_count);
        ELSIF is_compressed_chunk IS NULL AND local_compressed_chunk_id IS NULL THEN
        -- 'input relation is uncompressed chunk #3';
            uncompressed_row_count = _timescaledb_functions.get_approx_row_count(relation);
            RETURN uncompressed_row_count;
        ELSE
        -- 'compressed chunk only #4';
            -- use the compression_chunk_size stats to fetch precompressed num rows
            SELECT COALESCE(SUM(numrows_pre_compression), 0) FROM _timescaledb_catalog.chunk srcch,
                _timescaledb_catalog.compression_chunk_size map INTO compressed_row_count
                WHERE map.compressed_chunk_id = srcch.id
                AND srcch.table_name = local_table_name AND srcch.schema_name = local_schema_name;
            RETURN compressed_row_count;
        END IF;
    END IF;
    -- Check for input relation is Plain RELATION
    uncompressed_row_count = _timescaledb_functions.get_approx_row_count(relation);
    RETURN uncompressed_row_count;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_approx_row_count(relation REGCLASS)
RETURNS BIGINT
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
  WITH RECURSIVE inherited_id(oid) AS
  (
    SELECT relation
    UNION ALL
    SELECT i.inhrelid
    FROM pg_inherits i
    JOIN inherited_id b ON i.inhparent = b.oid
  )
  -- reltuples for partitioned tables is the sum of it's children in pg14 so we need to filter those out
  SELECT COALESCE((SUM(reltuples) FILTER (WHERE reltuples > 0 AND relkind <> 'p')), 0)::BIGINT
  FROM inherited_id
  JOIN pg_class USING (oid);
$BODY$ SET search_path TO pg_catalog, pg_temp;

-------- stats related to compression ------
CREATE OR REPLACE VIEW _timescaledb_internal.compressed_chunk_stats AS
SELECT
    srcht.schema_name AS hypertable_schema,
    srcht.table_name AS hypertable_name,
    srcch.schema_name AS chunk_schema,
    srcch.table_name AS chunk_name,
    CASE WHEN srcch.compressed_chunk_id IS NULL THEN
        'Uncompressed'::text
    ELSE
        'Compressed'::text
    END AS compression_status,
    map.uncompressed_heap_size,
    map.uncompressed_index_size,
    map.uncompressed_toast_size,
    map.uncompressed_heap_size + map.uncompressed_toast_size + map.uncompressed_index_size AS uncompressed_total_size,
    map.compressed_heap_size,
    map.compressed_index_size,
    map.compressed_toast_size,
    map.compressed_heap_size + map.compressed_toast_size + map.compressed_index_size AS compressed_total_size
FROM
    _timescaledb_catalog.hypertable AS srcht
    JOIN _timescaledb_catalog.chunk AS srcch ON srcht.id = srcch.hypertable_id
        AND srcht.compressed_hypertable_id IS NOT NULL
        AND srcch.dropped = FALSE
    LEFT JOIN _timescaledb_catalog.compression_chunk_size map ON srcch.id = map.chunk_id;

GRANT SELECT ON _timescaledb_internal.compressed_chunk_stats TO PUBLIC;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compressed_chunk_local_stats(schema_name_in name, table_name_in name)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint)
    LANGUAGE SQL
    STABLE STRICT
    AS
$BODY$
    SELECT
        ch.chunk_schema,
        ch.chunk_name,
        ch.compression_status,
        ch.uncompressed_heap_size,
        ch.uncompressed_index_size,
        ch.uncompressed_toast_size,
        ch.uncompressed_total_size,
        ch.compressed_heap_size,
        ch.compressed_index_size,
        ch.compressed_toast_size,
        ch.compressed_total_size
    FROM
        _timescaledb_internal.compressed_chunk_stats ch
    WHERE
        ch.hypertable_schema = schema_name_in
        AND ch.hypertable_name = table_name_in;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Get per chunk compression statistics for a hypertable that has
-- compression enabled
CREATE OR REPLACE FUNCTION @extschema@.chunk_compression_stats (hypertable REGCLASS)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE PLPGSQL
    STABLE STRICT
    AS $BODY$
DECLARE
    table_name name;
    schema_name name;
BEGIN
    SELECT
      relname, nspname
    INTO
	    table_name, schema_name
    FROM
        pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        INNER JOIN _timescaledb_catalog.hypertable ht ON (ht.schema_name = n.nspname
                AND ht.table_name = c.relname)
    WHERE
        c.OID = hypertable;

    IF table_name IS NULL THEN
	    RETURN;
	END IF;

  RETURN QUERY
  SELECT
      *,
      NULL::name
  FROM
      _timescaledb_functions.compressed_chunk_local_stats(schema_name, table_name);
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION @extschema@.chunk_columnstore_stats (hypertable REGCLASS)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE SQL
    STABLE STRICT
    AS 'SELECT * FROM @extschema@.chunk_compression_stats($1)'
    SET search_path TO pg_catalog, pg_temp;

-- Get compression statistics for a hypertable that has
-- compression enabled
CREATE OR REPLACE FUNCTION @extschema@.hypertable_compression_stats (hypertable REGCLASS)
    RETURNS TABLE (
        total_chunks bigint,
        number_compressed_chunks bigint,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE SQL
    STABLE STRICT
    AS
$BODY$
	SELECT
        count(*)::bigint AS total_chunks,
        (count(*) FILTER (WHERE ch.compression_status = 'Compressed'))::bigint AS number_compressed_chunks,
        sum(ch.before_compression_table_bytes)::bigint AS before_compression_table_bytes,
        sum(ch.before_compression_index_bytes)::bigint AS before_compression_index_bytes,
        sum(ch.before_compression_toast_bytes)::bigint AS before_compression_toast_bytes,
        sum(ch.before_compression_total_bytes)::bigint AS before_compression_total_bytes,
        sum(ch.after_compression_table_bytes)::bigint AS after_compression_table_bytes,
        sum(ch.after_compression_index_bytes)::bigint AS after_compression_index_bytes,
        sum(ch.after_compression_toast_bytes)::bigint AS after_compression_toast_bytes,
        sum(ch.after_compression_total_bytes)::bigint AS after_compression_total_bytes,
        ch.node_name
    FROM
	    @extschema@.chunk_compression_stats(hypertable) ch
    GROUP BY
        ch.node_name;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION @extschema@.hypertable_columnstore_stats (hypertable REGCLASS)
    RETURNS TABLE (
        total_chunks bigint,
        number_compressed_chunks bigint,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE SQL
    STABLE STRICT
    AS 'SELECT * FROM @extschema@.hypertable_compression_stats($1)'
    SET search_path TO pg_catalog, pg_temp;

-------------Get index size for hypertables -------
--schema_name      - schema_name for hypertable index
-- index_name      - index on hyper table
---note that the query matches against the hypertable's schema name as
-- the input is on the hypertable index and not the chunk index.
CREATE OR REPLACE FUNCTION _timescaledb_functions.indexes_local_size(
    schema_name_in             NAME,
    index_name_in              NAME
)
RETURNS TABLE ( hypertable_id INTEGER,
                total_bytes BIGINT )
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
    WITH chunk_index_size (num_bytes) AS (
        SELECT
		    COALESCE(sum(pg_relation_size(c.oid)), 0)::bigint
        FROM
            pg_class c,
            pg_namespace n,
            _timescaledb_catalog.chunk ch,
            _timescaledb_catalog.chunk_index ci,
			_timescaledb_catalog.hypertable h
         WHERE ch.schema_name = n.nspname
             AND c.relnamespace = n.oid
             AND c.relname = ci.index_name
             AND ch.id = ci.chunk_id
             AND h.id = ci.hypertable_id
             AND h.schema_name = schema_name_in
             AND ci.hypertable_index_name = index_name_in
    ) SELECT
	      h.id,
		  -- Add size of index on all chunks + index size on root table
		  (SELECT num_bytes FROM chunk_index_size) + pg_relation_size(format('%I.%I', schema_name_in, index_name_in)::regclass)::bigint
	  FROM
	      pg_class c, pg_index i, _timescaledb_catalog.hypertable h
	  WHERE
	     i.indexrelid = format('%I.%I', schema_name_in, index_name_in)::regclass
		 AND c.oid = i.indrelid
		 AND h.schema_name = schema_name_in
		 AND h.table_name = c.relname;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Get sizes of indexes on a hypertable
--
-- index_name           - index on hyper table
--
-- Returns:
-- total_bytes          - size of index on disk

CREATE OR REPLACE FUNCTION @extschema@.hypertable_index_size(
    index_name              REGCLASS
)
RETURNS BIGINT
LANGUAGE PLPGSQL VOLATILE STRICT AS
$BODY$
DECLARE
        ht_index_name       NAME;
        ht_schema_name      NAME;
        ht_name      NAME;
        ht_id INTEGER;
        index_bytes BIGINT;
BEGIN
   SELECT c.relname, cl.relname, nsp.nspname
   INTO ht_index_name, ht_name, ht_schema_name
   FROM pg_class c, pg_index cind, pg_class cl,
        pg_namespace nsp, _timescaledb_catalog.hypertable ht
   WHERE c.oid = cind.indexrelid AND cind.indrelid = cl.oid
         AND cl.relnamespace = nsp.oid AND c.oid = index_name
		 AND ht.schema_name = nsp.nspname ANd ht.table_name = cl.relname;

   IF ht_index_name IS NULL THEN
       RETURN NULL;
   END IF;

   -- get the local size or size of access node indexes
   SELECT il.total_bytes
   INTO index_bytes
   FROM _timescaledb_functions.indexes_local_size(ht_schema_name, ht_index_name) il;

   IF index_bytes IS NULL THEN
       index_bytes = 0;
   END IF;

   RETURN index_bytes;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-------------End index size for hypertables -------
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.hist_sfunc (state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
RETURNS INTERNAL
AS '$libdir/timescaledb-2.20.3', 'ts_hist_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.hist_combinefunc(state1 INTERNAL, state2 INTERNAL)
RETURNS INTERNAL
AS '$libdir/timescaledb-2.20.3', 'ts_hist_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.hist_serializefunc(INTERNAL)
RETURNS bytea
AS '$libdir/timescaledb-2.20.3', 'ts_hist_serializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.hist_deserializefunc(bytea, INTERNAL)
RETURNS INTERNAL
AS '$libdir/timescaledb-2.20.3', 'ts_hist_deserializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.hist_finalfunc(state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
RETURNS INTEGER[]
AS '$libdir/timescaledb-2.20.3', 'ts_hist_finalfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- We started using CREATE OR REPLACE AGGREGATE for aggregate creation once the syntax was fully supported
-- as it is easier to support idempotent changes this way. This will allow for changes to functions supporting
-- the aggregate, and, for instance, the definition and inclusion of inverse functions for window function
-- support. However, it should still be noted that changes to the data structures used for the internal
-- state of the aggregate must be backwards compatible and the old format must be accepted by any new functions
-- in order for them to continue working with Continuous Aggregates, where old states may have been materialized.

-- This aggregate partitions the dataset into a specified number of buckets (nbuckets) ranging
-- from the inputted min to max values.
CREATE OR REPLACE AGGREGATE @extschema@.histogram (DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, INTEGER) (
    SFUNC = _timescaledb_functions.hist_sfunc,
    STYPE = INTERNAL,
    COMBINEFUNC = _timescaledb_functions.hist_combinefunc,
    SERIALFUNC = _timescaledb_functions.hist_serializefunc,
    DESERIALFUNC = _timescaledb_functions.hist_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = _timescaledb_functions.hist_finalfunc,
    FINALFUNC_EXTRA
);
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.restart_background_workers()
RETURNS BOOL
AS '$libdir/timescaledb', 'ts_bgw_db_workers_restart'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.stop_background_workers()
RETURNS BOOL
AS '$libdir/timescaledb', 'ts_bgw_db_workers_stop'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.start_background_workers()
RETURNS BOOL
AS '$libdir/timescaledb', 'ts_bgw_db_workers_start'
LANGUAGE C VOLATILE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.generate_uuid() RETURNS UUID
AS '$libdir/timescaledb-2.20.3', 'ts_uuid_generate' LANGUAGE C VOLATILE STRICT;

-- Trigger to change INSERT into UPDATE if key already exists.
--
-- During extension installation we create 3 entries in the metadata table which are
-- included in dumps. To allow loading logical dumps we need this trigger to turn INSERTs
-- into UPDATEs if the key already exists.
CREATE OR REPLACE FUNCTION _timescaledb_functions.metadata_insert_trigger() RETURNS TRIGGER LANGUAGE PLPGSQL
AS $$
BEGIN
  IF EXISTS (SELECT FROM _timescaledb_catalog.metadata WHERE key = NEW.key) THEN
    UPDATE _timescaledb_catalog.metadata SET value = NEW.value WHERE key = NEW.key;
    RETURN NULL;
  END IF;
  RETURN NEW;
END
$$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE TRIGGER metadata_insert_trigger BEFORE INSERT ON _timescaledb_catalog.metadata FOR EACH ROW EXECUTE PROCEDURE _timescaledb_functions.metadata_insert_trigger();

-- Insert uuid and install_timestamp on database creation since the trigger
-- will turn these into UPDATEs on conflicts we can't use ON CONFLICT DO NOTHING.
DO $$
BEGIN
  IF (NOT EXISTS (SELECT FROM _timescaledb_catalog.metadata WHERE key = 'uuid')) THEN
    INSERT INTO _timescaledb_catalog.metadata SELECT 'uuid', _timescaledb_functions.generate_uuid(), TRUE;
  END IF;
  IF (NOT EXISTS (SELECT FROM _timescaledb_catalog.metadata WHERE key = 'install_timestamp')) THEN
    INSERT INTO _timescaledb_catalog.metadata SELECT 'install_timestamp', now(), TRUE;
  END IF;
END
$$;

-- Install catalog version on database installation and upgrade.
-- This allows us to detect catalog mismatches in dump/restore cycle.
INSERT INTO _timescaledb_catalog.metadata (key, value, include_in_telemetry) SELECT 'timescaledb_version', '2.20.3', FALSE;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Convenience view to list all hypertables
CREATE OR REPLACE VIEW timescaledb_information.hypertables AS
WITH
  hypertable_info AS (
    SELECT hypertable_id, schema_name, table_name,
           num_dimensions, compression_state, column_name,
           column_type, interval_length,
           (compression_state = 1) AS compression_enabled,
           row_number() OVER (PARTITION BY hypertable_id ORDER BY di.id) AS dimension_num
      FROM _timescaledb_catalog.hypertable ht
      JOIN _timescaledb_catalog.dimension di ON ht.id = di.hypertable_id
  )
SELECT
  ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  t.tableowner AS owner,
  ht.num_dimensions,
  (
    SELECT count(1)
    FROM _timescaledb_catalog.chunk ch
    WHERE ch.hypertable_id = ht.hypertable_id
      AND ch.dropped IS FALSE
      AND ch.osm_chunk IS FALSE
  ) AS num_chunks,
  ht.compression_enabled,
  srchtbs.tablespace_list AS tablespaces,
  ht.column_name AS primary_dimension,
  ht.column_type AS primary_dimension_type
FROM hypertable_info ht
JOIN pg_tables t ON ht.table_name = t.tablename AND ht.schema_name = t.schemaname
LEFT JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = ht.hypertable_id
LEFT JOIN (
    SELECT hypertable_id,
      array_agg(tablespace_name ORDER BY id) AS tablespace_list
    FROM _timescaledb_catalog.tablespace
    GROUP BY hypertable_id) srchtbs ON ht.hypertable_id = srchtbs.hypertable_id
WHERE ht.compression_state != 2 --> no internal compression tables
  AND ca.mat_hypertable_id IS NULL
  AND ht.interval_length IS NOT NULL
  AND ht.dimension_num = 1;

CREATE OR REPLACE VIEW timescaledb_information.job_stats AS
SELECT ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  j.id AS job_id,
  js.last_start AS last_run_started_at,
  js.last_successful_finish AS last_successful_finish,
  CASE WHEN js.last_finish < '4714-11-24 00:00:00+00 BC' THEN
    NULL
  WHEN js.last_finish IS NOT NULL THEN
    CASE WHEN js.last_run_success = 't' THEN
      'Success'
    WHEN js.last_run_success = 'f' THEN
      'Failed'
    END
  END AS last_run_status,
  CASE WHEN pgs.state = 'active' THEN
    'Running'
  WHEN j.scheduled = FALSE THEN
    'Paused'
  ELSE
    'Scheduled'
  END AS job_status,
  CASE WHEN js.last_finish > js.last_start THEN
  (js.last_finish - js.last_start)
  END AS last_run_duration,
  CASE WHEN j.scheduled THEN
    js.next_start
  END AS next_start,
  js.total_runs,
  js.total_successes,
  js.total_failures
FROM _timescaledb_config.bgw_job j
  INNER JOIN _timescaledb_internal.bgw_job_stat js ON j.id = js.job_id
  LEFT JOIN _timescaledb_catalog.hypertable ht ON j.hypertable_id = ht.id
  LEFT JOIN pg_stat_activity pgs ON pgs.datname = current_database()
    AND pgs.application_name = j.application_name
  ORDER BY ht.schema_name,
    ht.table_name;

-- view for background worker jobs
CREATE OR REPLACE VIEW timescaledb_information.jobs AS
SELECT j.id AS job_id,
  j.application_name,
  j.schedule_interval,
  j.max_runtime,
  j.max_retries,
  j.retry_period,
  j.proc_schema,
  j.proc_name,
  j.owner,
  j.scheduled,
  j.fixed_schedule,
  j.config,
  js.next_start,
  j.initial_start,
  COALESCE(ca.user_view_schema, ht.schema_name) AS hypertable_schema,
  COALESCE(ca.user_view_name, ht.table_name) AS hypertable_name,
  j.check_schema,
  j.check_name
FROM _timescaledb_config.bgw_job j
  LEFT JOIN _timescaledb_catalog.hypertable ht ON ht.id = j.hypertable_id
  LEFT JOIN _timescaledb_internal.bgw_job_stat js ON js.job_id = j.id
  LEFT JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = j.hypertable_id;

-- views for continuous aggregate queries ---
CREATE OR REPLACE VIEW timescaledb_information.continuous_aggregates AS
SELECT ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  cagg.user_view_schema AS view_schema,
  cagg.user_view_name AS view_name,
  viewinfo.viewowner AS view_owner,
  cagg.materialized_only,
  CASE WHEN mat_ht.compressed_hypertable_id IS NOT NULL
       THEN TRUE
       ELSE FALSE
  END AS compression_enabled,
  mat_ht.schema_name AS materialization_hypertable_schema,
  mat_ht.table_name AS materialization_hypertable_name,
  directview.viewdefinition AS view_definition,
  cagg.finalized
FROM _timescaledb_catalog.continuous_agg cagg,
  _timescaledb_catalog.hypertable ht,
  LATERAL (
    SELECT C.oid,
      pg_get_userbyid(C.relowner) AS viewowner
    FROM pg_class C
      LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'v'
      AND C.relname = cagg.user_view_name
      AND N.nspname = cagg.user_view_schema) viewinfo,
  LATERAL (
    SELECT pg_get_viewdef(C.oid) AS viewdefinition
    FROM pg_class C
    LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
  WHERE C.relkind = 'v'
    AND C.relname = cagg.direct_view_name
    AND N.nspname = cagg.direct_view_schema) directview,
  LATERAL (
    SELECT schema_name, table_name, compressed_hypertable_id
    FROM _timescaledb_catalog.hypertable
    WHERE cagg.mat_hypertable_id = id) mat_ht
WHERE cagg.raw_hypertable_id = ht.id;

-- chunks metadata view, shows information about the primary dimension column
-- query plans with CTEs are not always optimized by PG. So use in-line
-- tables.

CREATE OR REPLACE VIEW timescaledb_information.chunks AS
SELECT hypertable_schema,
  hypertable_name,
  schema_name AS chunk_schema,
  chunk_name,
  primary_dimension,
  primary_dimension_type,
  range_start,
  range_end,
  integer_range_start AS range_start_integer,
  integer_range_end AS range_end_integer,
  is_compressed,
  chunk_table_space AS chunk_tablespace,
  creation_time AS chunk_creation_time
FROM (
  SELECT ht.schema_name AS hypertable_schema,
    ht.table_name AS hypertable_name,
    srcch.schema_name AS schema_name,
    srcch.table_name AS chunk_name,
    dim.column_name AS primary_dimension,
    dim.column_type AS primary_dimension_type,
    row_number() OVER (PARTITION BY chcons.chunk_id ORDER BY dim.id) AS chunk_dimension_num,
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      _timescaledb_functions.to_timestamp(dimsl.range_start)
    ELSE
      NULL
    END AS range_start,
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      _timescaledb_functions.to_timestamp(dimsl.range_end)
    ELSE
      NULL
    END AS range_end,
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      NULL
    ELSE
      dimsl.range_start
    END AS integer_range_start,
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      NULL
    ELSE
      dimsl.range_end
    END AS integer_range_end,
    CASE WHEN (srcch.status & 1 = 1) THEN
        TRUE
    ELSE FALSE
    END AS is_compressed,
    pgtab.spcname AS chunk_table_space,
	srcch.creation_time AS creation_time
  FROM _timescaledb_catalog.chunk srcch
    INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id = srcch.hypertable_id
    INNER JOIN _timescaledb_catalog.chunk_constraint chcons ON srcch.id = chcons.chunk_id
    INNER JOIN _timescaledb_catalog.dimension dim ON srcch.hypertable_id = dim.hypertable_id
    INNER JOIN _timescaledb_catalog.dimension_slice dimsl ON dim.id = dimsl.dimension_id
      AND chcons.dimension_slice_id = dimsl.id
    INNER JOIN (
      SELECT relname,
        reltablespace,
        nspname AS schema_name
      FROM pg_class,
        pg_namespace
      WHERE pg_class.relnamespace = pg_namespace.oid) cl ON srcch.table_name = cl.relname
      AND srcch.schema_name = cl.schema_name
    LEFT OUTER JOIN pg_tablespace pgtab ON pgtab.oid = reltablespace
  WHERE srcch.dropped IS FALSE AND srcch.osm_chunk IS FALSE
    AND ht.compression_state != 2 ) finalq
WHERE chunk_dimension_num = 1;

-- hypertable's dimension information
-- CTEs aren't used in the query as PG does not always optimize them
-- as expected.

CREATE OR REPLACE VIEW timescaledb_information.dimensions AS
SELECT ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  rank() OVER (PARTITION BY hypertable_id ORDER BY dim.id) AS dimension_number,
  dim.column_name,
  dim.column_type,
  CASE WHEN dim.interval_length IS NULL THEN
    'Space'
  ELSE
    'Time'
  END AS dimension_type,
  CASE WHEN dim.interval_length IS NOT NULL THEN
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      _timescaledb_functions.to_interval(dim.interval_length)
    ELSE
      NULL
    END
  END AS time_interval,
  CASE WHEN dim.interval_length IS NOT NULL THEN
    CASE WHEN dim.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
      NULL
    ELSE
      dim.interval_length
    END
  END AS integer_interval,
  dim.integer_now_func,
  dim.num_slices AS num_partitions
FROM _timescaledb_catalog.hypertable ht,
  _timescaledb_catalog.dimension dim
WHERE dim.hypertable_id = ht.id;

---compression parameters information ---
CREATE OR REPLACE VIEW timescaledb_information.compression_settings AS
SELECT
	schema_name AS hypertable_schema,
  table_name AS hypertable_name,
  (unnest(cs.segmentby))::name COLLATE "C" AS attname,
  generate_series(1,array_length(cs.segmentby,1))::smallint AS segmentby_column_index,
  NULL::smallint AS orderby_column_index,
  NULL::bool AS orderby_asc,
  NULL::bool AS orderby_nullsfirst
FROM _timescaledb_catalog.hypertable ht
INNER JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I',ht.schema_name,ht.table_name)::regclass AND cs.segmentby IS NOT NULL
WHERE compressed_hypertable_id IS NOT NULL
UNION ALL
SELECT
	schema_name AS hypertable_schema,
  table_name AS hypertable_name,
  (unnest(cs.orderby))::name COLLATE "C" AS attname,
  NULL::smallint AS segmentby_column_index,
  generate_series(1,array_length(cs.orderby,1))::smallint AS orderby_column_index,
  unnest(array_replace(array_replace(array_replace(cs.orderby_desc,false,NULL),true,false),NULL,true)) AS orderby_asc,
  unnest(cs.orderby_nullsfirst) AS orderby_nullsfirst
FROM _timescaledb_catalog.hypertable ht
INNER JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I',ht.schema_name,ht.table_name)::regclass AND cs.orderby IS NOT NULL
WHERE compressed_hypertable_id IS NOT NULL
ORDER BY hypertable_name,
  segmentby_column_index,
  orderby_column_index;

-- Job errors view that adds a security barrier on the bgw_job_stat_history
-- table in _timescaledb_internal. The view only allows users to view
-- log entries belonging to jobs that are owned by any of the users
-- role. A special case is added so that the superuser or the database
-- owner can see all job log entries, even those that do not have an
-- associated job.
--
-- Note that we have to use a sub-select here since pg_database_owner
-- does not exist before PostgreSQL 14.
CREATE OR REPLACE VIEW timescaledb_information.job_errors
WITH (security_barrier = true) AS
SELECT
    h.job_id,
    h.data->'job'->>'proc_schema' as proc_schema,
    h.data->'job'->>'proc_name' as proc_name,
    h.pid,
    h.execution_start AS start_time,
    h.execution_finish AS finish_time,
    h.data->'error_data'->>'sqlerrcode' AS sqlerrcode,
    CASE
      WHEN h.succeeded IS NULL AND h.execution_finish IS NULL AND h.pid IS NULL THEN
        'job crash detected, see server logs'
      WHEN h.data->'error_data'->>'message' IS NOT NULL THEN
        CASE WHEN h.data->'error_data'->>'detail' IS NOT NULL THEN
          CASE WHEN h.data->'error_data'->>'hint' IS NOT NULL THEN concat(h.data->'error_data'->>'message', '. ', h.data->'error_data'->>'detail', '. ', h.data->'error_data'->>'hint')
          ELSE concat(h.data->'error_data'->>'message', ' ', h.data->'error_data'->>'detail')
          END
        ELSE
          CASE WHEN h.data->'error_data'->>'hint' IS NOT NULL THEN concat(h.data->'error_data'->>'message', '. ', h.data->'error_data'->>'hint')
          ELSE h.data->'error_data'->>'message'
          END
        END
    END AS err_message
FROM
    _timescaledb_internal.bgw_job_stat_history h
LEFT JOIN
    _timescaledb_config.bgw_job j ON (j.id = h.job_id)
WHERE
    h.succeeded IS FALSE
    OR h.succeeded IS NULL
    AND (pg_catalog.pg_has_role(current_user,
			   (SELECT pg_catalog.pg_get_userbyid(datdba)
			      FROM pg_catalog.pg_database
			     WHERE datname = current_database()),
			   'MEMBER') IS TRUE
    OR pg_catalog.pg_has_role(current_user, owner, 'MEMBER') IS TRUE);

CREATE OR REPLACE VIEW timescaledb_information.job_history
WITH (security_barrier = true) AS
SELECT
    h.id,
    h.job_id,
    h.succeeded,
    coalesce(h.data->'job'->>'proc_schema', j.proc_schema) as proc_schema,
    coalesce(h.data->'job'->>'proc_name', j.proc_name) as proc_name,
    h.pid,
    h.execution_start AS start_time,
    h.execution_finish AS finish_time,
    h.data->'job'->'config' AS config,
    h.data->'error_data'->>'sqlerrcode' AS sqlerrcode,
    CASE
      WHEN h.succeeded IS NULL AND h.execution_finish IS NULL AND h.pid IS NULL THEN
        'job crash detected, see server logs'
      WHEN h.succeeded IS FALSE AND h.data->'error_data'->>'message' IS NOT NULL THEN
        CASE WHEN h.data->'error_data'->>'detail' IS NOT NULL THEN
          CASE WHEN h.data->'error_data'->>'hint' IS NOT NULL THEN concat(h.data->'error_data'->>'message', '. ', h.data->'error_data'->>'detail', '. ', h.data->'error_data'->>'hint')
          ELSE concat(h.data->'error_data'->>'message', ' ', h.data->'error_data'->>'detail')
          END
        ELSE
          CASE WHEN h.data->'error_data'->>'hint' IS NOT NULL THEN concat(h.data->'error_data'->>'message', '. ', h.data->'error_data'->>'hint')
          ELSE h.data->'error_data'->>'message'
          END
        END
    END AS err_message
FROM
    _timescaledb_internal.bgw_job_stat_history h
LEFT JOIN
    _timescaledb_config.bgw_job j ON (j.id = h.job_id)
WHERE (pg_catalog.pg_has_role(current_user,
			   (SELECT pg_catalog.pg_get_userbyid(datdba)
			      FROM pg_catalog.pg_database
			     WHERE datname = current_database()),
			   'MEMBER') IS TRUE
    OR pg_catalog.pg_has_role(current_user, owner, 'MEMBER') IS TRUE);

CREATE OR REPLACE VIEW timescaledb_information.hypertable_compression_settings AS
	SELECT
		format('%I.%I',ht.schema_name,ht.table_name)::regclass AS hypertable,
		array_to_string(segmentby,',') AS segmentby,
		un.orderby,
    d.compress_interval_length
  FROM _timescaledb_catalog.hypertable ht
  JOIN LATERAL (
    SELECT
      CASE WHEN d.column_type = ANY(ARRAY['timestamp','timestamptz','date']::regtype[]) THEN
        _timescaledb_functions.to_interval(d.compress_interval_length)::text
      ELSE
        d.compress_interval_length::text
      END AS compress_interval_length
    FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = ht.id ORDER BY id LIMIT 1
  ) d ON true
  LEFT JOIN _timescaledb_catalog.compression_settings s ON format('%I.%I',ht.schema_name,ht.table_name)::regclass = s.relid
	LEFT JOIN LATERAL (
		SELECT
			string_agg(
				format('%I%s%s',orderby,
					CASE WHEN "desc" THEN ' DESC' ELSE '' END,
					CASE WHEN nullsfirst AND NOT "desc" THEN ' NULLS FIRST' WHEN NOT nullsfirst AND "desc" THEN ' NULLS LAST' ELSE '' END
				)
			,',') AS orderby
		FROM unnest(s.orderby, s.orderby_desc, s.orderby_nullsfirst) un(orderby, "desc", nullsfirst)
	) un ON true;

CREATE OR REPLACE VIEW timescaledb_information.chunk_compression_settings AS
	SELECT
		format('%I.%I',ht.schema_name,ht.table_name)::regclass AS hypertable,
		format('%I.%I',ch.schema_name,ch.table_name)::regclass AS chunk,
		array_to_string(segmentby,',') AS segmentby,
		un.orderby
	FROM _timescaledb_catalog.hypertable ht
    INNER JOIN _timescaledb_catalog.chunk ch ON ch.hypertable_id = ht.id
    INNER JOIN _timescaledb_catalog.compression_settings s ON (format('%I.%I',ch.schema_name,ch.table_name)::regclass = s.relid)
	LEFT JOIN LATERAL (
		SELECT
			string_agg(
				format('%I%s%s',orderby,
					CASE WHEN "desc" THEN ' DESC' ELSE '' END,
					CASE WHEN nullsfirst AND NOT "desc" THEN ' NULLS FIRST' WHEN NOT nullsfirst AND "desc" THEN ' NULLS LAST' ELSE '' END
				)
			,',') AS orderby
		FROM unnest(s.orderby, s.orderby_desc, s.orderby_nullsfirst) un(orderby, "desc", nullsfirst)
	) un ON true;


CREATE OR REPLACE VIEW timescaledb_information.hypertable_columnstore_settings
AS SELECT * FROM timescaledb_information.hypertable_compression_settings;

CREATE OR REPLACE VIEW timescaledb_information.chunk_columnstore_settings AS
SELECT * FROM timescaledb_information.chunk_compression_settings;

GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO PUBLIC;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE VIEW timescaledb_experimental.policies AS
SELECT ca.user_view_name AS relation_name,
  ca.user_view_schema AS relation_schema,
  j.schedule_interval,
  j.proc_schema,
  j.proc_name,
  j.config,
  ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name
FROM _timescaledb_config.bgw_job j
  JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = j.hypertable_id
  JOIN _timescaledb_catalog.hypertable ht ON ht.id = ca.mat_hypertable_id;

GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_experimental TO PUBLIC;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width SMALLINT, ts SMALLINT, start SMALLINT=NULL, finish SMALLINT=NULL) RETURNS SMALLINT
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_int16_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INT, ts INT, start INT=NULL, finish INT=NULL) RETURNS INT
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_int32_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width BIGINT, ts BIGINT, start BIGINT=NULL, finish BIGINT=NULL) RETURNS BIGINT
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_int64_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts DATE, start DATE=NULL, finish DATE=NULL) RETURNS DATE
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_date_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMP, start TIMESTAMP=NULL, finish TIMESTAMP=NULL) RETURNS TIMESTAMP
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_timestamp_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, start TIMESTAMPTZ=NULL, finish TIMESTAMPTZ=NULL) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_timestamptz_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, start TIMESTAMPTZ=NULL, finish TIMESTAMPTZ=NULL) RETURNS TIMESTAMPTZ
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_timestamptz_timezone_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

-- locf function
CREATE OR REPLACE FUNCTION @extschema@.locf(value ANYELEMENT, prev ANYELEMENT=NULL, treat_null_as_missing BOOL=false) RETURNS ANYELEMENT
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

-- interpolate functions
CREATE OR REPLACE FUNCTION @extschema@.interpolate(value SMALLINT,prev RECORD=NULL,next RECORD=NULL) RETURNS SMALLINT
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.interpolate(value INT,prev RECORD=NULL,next RECORD=NULL) RETURNS INT
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.interpolate(value BIGINT,prev RECORD=NULL,next RECORD=NULL) RETURNS BIGINT
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.interpolate(value REAL,prev RECORD=NULL,next RECORD=NULL) RETURNS REAL
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.interpolate(value FLOAT,prev RECORD=NULL,next RECORD=NULL) RETURNS FLOAT
	AS '$libdir/timescaledb-2.20.3', 'ts_gapfill_marker' LANGUAGE C VOLATILE PARALLEL SAFE;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- chunk - the OID of the chunk to be CLUSTERed
-- index - the OID of the index to be CLUSTERed on, or NULL to use the index
--         last used
CREATE OR REPLACE FUNCTION @extschema@.reorder_chunk(
    chunk REGCLASS,
    index REGCLASS=NULL,
    verbose BOOLEAN=FALSE
) RETURNS VOID AS '$libdir/timescaledb-2.20.3', 'ts_reorder_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.move_chunk(
    chunk REGCLASS,
    destination_tablespace Name,
    index_destination_tablespace Name=NULL,
    reorder_index REGCLASS=NULL,
    verbose BOOLEAN=FALSE
) RETURNS VOID AS '$libdir/timescaledb-2.20.3', 'ts_move_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.create_compressed_chunk(
    chunk REGCLASS,
    chunk_table REGCLASS,
    uncompressed_heap_size BIGINT,
    uncompressed_toast_size BIGINT,
    uncompressed_index_size BIGINT,
    compressed_heap_size BIGINT,
    compressed_toast_size BIGINT,
    compressed_index_size BIGINT,
    numrows_pre_compression BIGINT,
    numrows_post_compression BIGINT
) RETURNS REGCLASS AS '$libdir/timescaledb-2.20.3', 'ts_create_compressed_chunk' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = true,
    recompress BOOLEAN = false,
    hypercore_use_access_method BOOL = NULL
) RETURNS REGCLASS AS '$libdir/timescaledb-2.20.3', 'ts_compress_chunk' LANGUAGE C VOLATILE;

-- Alias for compress_chunk above.
CREATE OR REPLACE PROCEDURE @extschema@.convert_to_columnstore(
    chunk REGCLASS,
    if_not_columnstore BOOLEAN = true,
    recompress BOOLEAN = false,
    hypercore_use_access_method BOOL = NULL
) AS '$libdir/timescaledb-2.20.3', 'ts_compress_chunk' LANGUAGE C;

CREATE OR REPLACE FUNCTION @extschema@.decompress_chunk(
    uncompressed_chunk REGCLASS,
    if_compressed BOOLEAN = true
) RETURNS REGCLASS AS '$libdir/timescaledb-2.20.3', 'ts_decompress_chunk' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE PROCEDURE @extschema@.convert_to_rowstore(
    chunk REGCLASS,
    if_columnstore BOOLEAN = true
) AS '$libdir/timescaledb-2.20.3', 'ts_decompress_chunk' LANGUAGE C;

CREATE OR REPLACE PROCEDURE @extschema@.merge_chunks(
   chunk1 REGCLASS, chunk2 REGCLASS
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_merge_two_chunks';

CREATE OR REPLACE PROCEDURE @extschema@.merge_chunks(
    chunks REGCLASS[]
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_merge_chunks';

CREATE OR REPLACE PROCEDURE @extschema@.split_chunk(
    chunk REGCLASS,
    split_at "any" = NULL
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_split_chunk';

CREATE OR REPLACE FUNCTION _timescaledb_functions.recompress_chunk_segmentwise(
    uncompressed_chunk REGCLASS,
    if_compressed BOOLEAN = true
) RETURNS REGCLASS AS '$libdir/timescaledb-2.20.3', 'ts_recompress_chunk_segmentwise' LANGUAGE C STRICT VOLATILE;

-- find the index on the compressed chunk that can be used to recompress efficiently
-- this index must contain all the segmentby columns and the meta_sequence_number column last
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_compressed_chunk_index_for_recompression(
    uncompressed_chunk REGCLASS
) RETURNS REGCLASS AS '$libdir/timescaledb-2.20.3', 'ts_get_compressed_chunk_index_for_recompression' LANGUAGE C STRICT VOLATILE;
-- Recompress a chunk
--
-- Will give an error if the chunk was not already compressed. In this
-- case, the user should use compress_chunk instead. Note that this
-- function cannot be executed in an explicit transaction since it
-- contains transaction control commands.
--
-- Parameters:
--   chunk: Chunk to recompress.
--   if_not_compressed: Print notice instead of error if chunk is already compressed.

CREATE OR REPLACE PROCEDURE @extschema@.recompress_chunk(chunk REGCLASS, if_not_compressed BOOLEAN = true) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure @extschema@.recompress_chunk(regclass,boolean) is deprecated and the functionality is now included in @extschema@.compress_chunk. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM @extschema@.compress_chunk(chunk, if_not_compressed);
END$$ SET search_path TO pg_catalog,pg_temp;

-- A version of makeaclitem that accepts a comma-separated list of
-- privileges rather than just a single privilege. This is copied from
-- PG16, but since we need to support earlier versions, we provide it
-- with the extension.
--
-- This is intended for internal usage and interface might change.
CREATE OR REPLACE FUNCTION _timescaledb_functions.makeaclitem(regrole, regrole, text, bool)
RETURNS AclItem AS '$libdir/timescaledb-2.20.3', 'ts_makeaclitem'
LANGUAGE C STABLE PARALLEL SAFE STRICT;

-- Repair relation ACL by removing roles that do not exist in pg_authid.
CREATE OR REPLACE PROCEDURE _timescaledb_functions.repair_relation_acls()
LANGUAGE SQL AS $$
  WITH
    badrels AS (
	SELECT oid::regclass
	  FROM (SELECT oid, (aclexplode(relacl)).* FROM pg_class) AS rels
	 WHERE rels.grantee != 0
	   AND rels.grantee NOT IN (SELECT oid FROM pg_authid)
    ),
    pickacls AS (
      SELECT oid::regclass,
	     _timescaledb_functions.makeaclitem(
	         b.grantee,
		 b.grantor,
		 string_agg(b.privilege_type, ','),
		 b.is_grantable
	     ) AS acl
	FROM (SELECT oid, (aclexplode(relacl)).* AS a FROM pg_class) AS b
       WHERE b.grantee IN (SELECT oid FROM pg_authid)
       GROUP BY oid, b.grantee, b.grantor, b.is_grantable
    ),
    cleanacls AS (
      SELECT oid, array_agg(acl) AS acl FROM pickacls GROUP BY oid
    )
  UPDATE pg_class c
     SET relacl = (SELECT acl FROM cleanacls n WHERE c.oid = n.oid)
   WHERE oid IN (SELECT oid FROM badrels)
$$ SET search_path TO pg_catalog, pg_temp;

-- Remove chunk metadata when marked as dropped
CREATE OR REPLACE FUNCTION _timescaledb_functions.remove_dropped_chunk_metadata(_hypertable_id INTEGER)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
  _chunk_id INTEGER;
  _removed INTEGER := 0;
BEGIN
  FOR _chunk_id IN
    SELECT id FROM _timescaledb_catalog.chunk
    WHERE hypertable_id = _hypertable_id
    AND dropped IS TRUE
    AND NOT EXISTS (
        SELECT FROM information_schema.tables
        WHERE tables.table_schema = chunk.schema_name
        AND tables.table_name = chunk.table_name
    )
    AND NOT EXISTS (
        SELECT FROM _timescaledb_catalog.hypertable
        JOIN _timescaledb_catalog.continuous_agg ON continuous_agg.raw_hypertable_id = hypertable.id
        WHERE hypertable.id = chunk.hypertable_id
        -- for the old caggs format we need to keep chunk metadata for dropped chunks
        AND continuous_agg.finalized IS FALSE
    )
  LOOP
    _removed := _removed + 1;
    RAISE INFO 'Removing metadata of chunk % from hypertable %', _chunk_id, _hypertable_id;

    WITH _dimension_slice_remove AS (
        DELETE FROM _timescaledb_catalog.dimension_slice
        USING _timescaledb_catalog.chunk_constraint
        WHERE dimension_slice.id = chunk_constraint.dimension_slice_id
        AND chunk_constraint.chunk_id = _chunk_id
        AND NOT EXISTS (
            SELECT FROM _timescaledb_catalog.chunk_constraint cc
            WHERE cc.chunk_id <> _chunk_id
            AND cc.dimension_slice_id = dimension_slice.id
        )
        RETURNING _timescaledb_catalog.dimension_slice.id
    )
    DELETE FROM _timescaledb_catalog.chunk_constraint
    USING _dimension_slice_remove
    WHERE chunk_constraint.dimension_slice_id = _dimension_slice_remove.id;

    DELETE FROM _timescaledb_catalog.chunk_constraint
    WHERE chunk_constraint.chunk_id = _chunk_id;

    DELETE FROM _timescaledb_internal.bgw_policy_chunk_stats
    WHERE bgw_policy_chunk_stats.chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.chunk_index
    WHERE chunk_index.chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.compression_chunk_size
    WHERE compression_chunk_size.chunk_id = _chunk_id
    OR compression_chunk_size.compressed_chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.chunk
    WHERE chunk.id = _chunk_id
    OR chunk.compressed_chunk_id = _chunk_id;
  END LOOP;

  RETURN _removed;
END;
$$ SET search_path TO pg_catalog, pg_temp;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.partialize_agg(arg ANYELEMENT)
RETURNS BYTEA AS '$libdir/timescaledb-2.20.3', 'ts_partialize_agg' LANGUAGE C STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.finalize_agg_sfunc(
tstate internal, aggfn TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val ANYELEMENT)
RETURNS internal
AS '$libdir/timescaledb-2.20.3', 'ts_finalize_agg_sfunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.finalize_agg_ffunc(
tstate internal, aggfn TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val ANYELEMENT)
RETURNS anyelement
AS '$libdir/timescaledb-2.20.3', 'ts_finalize_agg_ffunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE AGGREGATE _timescaledb_functions.finalize_agg(agg_name TEXT,  inner_agg_collation_schema NAME,  inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val anyelement) (
    SFUNC = _timescaledb_functions.finalize_agg_sfunc,
    STYPE = internal,
    FINALFUNC = _timescaledb_functions.finalize_agg_ffunc,
    FINALFUNC_EXTRA
);
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.timescaledb_pre_restore() RETURNS BOOL AS
$BODY$
DECLARE
    db text;
BEGIN
    SELECT current_database() INTO db;
    EXECUTE format($$ALTER DATABASE %I SET timescaledb.restoring ='on'$$, db);
    SET SESSION timescaledb.restoring = 'on';
    PERFORM _timescaledb_functions.stop_background_workers();
    RETURN true;
END
$BODY$
LANGUAGE PLPGSQL SET search_path TO pg_catalog, pg_temp;


CREATE OR REPLACE FUNCTION @extschema@.timescaledb_post_restore() RETURNS BOOL AS
$BODY$
DECLARE
    db text;
    catalog_version text;
BEGIN
    SELECT m.value INTO catalog_version FROM pg_extension x
    JOIN _timescaledb_catalog.metadata m ON m.key='timescaledb_version'
    WHERE x.extname='timescaledb' AND x.extversion <> m.value;

    -- check that a loaded dump is compatible with the currently running code
    IF FOUND THEN
        RAISE EXCEPTION 'catalog version mismatch, expected "%" seen "%"', '2.20.3', catalog_version;
    END IF;

    SELECT current_database() INTO db;
    EXECUTE format($$ALTER DATABASE %I RESET timescaledb.restoring $$, db);
    -- we cannot use reset here because the reset_val might not be off
    SET timescaledb.restoring TO off;
    PERFORM _timescaledb_functions.restart_background_workers();

    RETURN true;
END
$BODY$
LANGUAGE PLPGSQL SET search_path TO pg_catalog, pg_temp;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.add_job(
  proc REGPROC,
  schedule_interval INTERVAL,
  config JSONB DEFAULT NULL,
  initial_start TIMESTAMPTZ DEFAULT NULL,
  scheduled BOOL DEFAULT true,
  check_config REGPROC DEFAULT NULL,
  fixed_schedule BOOL DEFAULT TRUE,
  timezone TEXT DEFAULT NULL,
  job_name TEXT DEFAULT NULL
) RETURNS INTEGER AS '$libdir/timescaledb-2.20.3', 'ts_job_add' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.delete_job(job_id INTEGER) RETURNS VOID AS '$libdir/timescaledb-2.20.3', 'ts_job_delete' LANGUAGE C VOLATILE STRICT;
CREATE OR REPLACE PROCEDURE @extschema@.run_job(job_id INTEGER) AS '$libdir/timescaledb-2.20.3', 'ts_job_run' LANGUAGE C;

-- Returns the updated job schedule values
CREATE OR REPLACE FUNCTION @extschema@.alter_job(
    job_id INTEGER,
    schedule_interval INTERVAL = NULL,
    max_runtime INTERVAL = NULL,
    max_retries INTEGER = NULL,
    retry_period INTERVAL = NULL,
    scheduled BOOL = NULL,
    config JSONB = NULL,
    next_start TIMESTAMPTZ = NULL,
    if_exists BOOL = FALSE,
    check_config REGPROC = NULL,
    fixed_schedule BOOL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT DEFAULT NULL,
    job_name TEXT DEFAULT NULL
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB,
next_start TIMESTAMPTZ, check_config TEXT, fixed_schedule BOOL, initial_start TIMESTAMPTZ, timezone TEXT, application_name name)
AS '$libdir/timescaledb-2.20.3', 'ts_job_alter'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.alter_job_set_hypertable_id(
    job_id INTEGER,
    hypertable REGCLASS )
RETURNS INTEGER AS '$libdir/timescaledb-2.20.3', 'ts_job_alter_set_hypertable_id'
LANGUAGE C VOLATILE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Add a retention policy to a hypertable or continuous aggregate.
-- The retention_window (typically an INTERVAL) determines the
-- window beyond which data is dropped at the time
-- of execution of the policy (e.g., '1 week'). Note that the retention
-- window will always align with chunk boundaries, thus the window
-- might be larger than the given one, but never smaller. In other
-- words, some data beyond the retention window
-- might be kept, but data within the window will never be deleted.
CREATE OR REPLACE FUNCTION @extschema@.add_retention_policy(
       relation REGCLASS,
       drop_after "any" = NULL,
       if_not_exists BOOL = false,
       schedule_interval INTERVAL = NULL,
       initial_start TIMESTAMPTZ = NULL,
       timezone TEXT = NULL,
       drop_created_before INTERVAL = NULL
)
RETURNS INTEGER AS '$libdir/timescaledb-2.20.3', 'ts_policy_retention_add'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.remove_retention_policy(
    relation REGCLASS,
    if_exists BOOL = false
) RETURNS VOID
AS '$libdir/timescaledb-2.20.3', 'ts_policy_retention_remove'
LANGUAGE C VOLATILE STRICT;

/* reorder policy */
CREATE OR REPLACE FUNCTION @extschema@.add_reorder_policy(
    hypertable REGCLASS,
    index_name NAME,
    if_not_exists BOOL = false,
    initial_start timestamptz = NULL,
    timezone TEXT = NULL
) RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_policy_reorder_add'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.remove_reorder_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS VOID
AS '$libdir/timescaledb-2.20.3', 'ts_policy_reorder_remove'
LANGUAGE C VOLATILE STRICT;

/* compression policy */
CREATE OR REPLACE FUNCTION @extschema@.add_compression_policy(
    hypertable REGCLASS,
    compress_after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    compress_created_before INTERVAL = NULL,
    hypercore_use_access_method BOOL = NULL
)
RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_policy_compression_add'
LANGUAGE C VOLATILE; -- not strict because we need to set different default values for schedule_interval

CREATE OR REPLACE PROCEDURE @extschema@.add_columnstore_policy(
    hypertable REGCLASS,
    after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    created_before INTERVAL = NULL,
    hypercore_use_access_method BOOL = NULL
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_policy_compression_add';

CREATE OR REPLACE FUNCTION @extschema@.remove_compression_policy(hypertable REGCLASS, if_exists BOOL = false) RETURNS BOOL
AS '$libdir/timescaledb-2.20.3', 'ts_policy_compression_remove'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE PROCEDURE @extschema@.remove_columnstore_policy(
       hypertable REGCLASS,
       if_exists BOOL = false
) LANGUAGE C AS '$libdir/timescaledb-2.20.3', 'ts_policy_compression_remove';

/* continuous aggregates policy */
CREATE OR REPLACE FUNCTION @extschema@.add_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    start_offset "any",
    end_offset "any",
    schedule_interval INTERVAL,
    if_not_exists BOOL = false,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    include_tiered_data BOOL = NULL,
    buckets_per_batch INTEGER = NULL,
    max_batches_per_execution INTEGER = NULL,
    refresh_newest_first BOOL = NULL
)
RETURNS INTEGER
AS '$libdir/timescaledb-2.20.3', 'ts_policy_refresh_cagg_add'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.remove_continuous_aggregate_policy(
    continuous_aggregate REGCLASS,
    if_not_exists BOOL = false, -- deprecating this argument, if_exists overrides it
    if_exists BOOL = NULL) -- when NULL get the value from if_not_exists

RETURNS VOID
AS '$libdir/timescaledb-2.20.3', 'ts_policy_refresh_cagg_remove'
LANGUAGE C VOLATILE;

/* 1 step policies */

/* Add policies */
/* Unsupported drop_created_before/compress_created_before in add/alter for caggs */
CREATE OR REPLACE FUNCTION timescaledb_experimental.add_policies(
    relation REGCLASS,
    if_not_exists BOOL = false,
    refresh_start_offset "any" = NULL,
    refresh_end_offset "any" = NULL,
    compress_after "any" = NULL,
    drop_after "any" = NULL,
    hypercore_use_access_method BOOL = NULL)
RETURNS BOOL
AS '$libdir/timescaledb-2.20.3', 'ts_policies_add'
LANGUAGE C VOLATILE;

/* Remove policies */
CREATE OR REPLACE FUNCTION timescaledb_experimental.remove_policies(
    relation REGCLASS,
    if_exists BOOL = false,
    VARIADIC policy_names TEXT[] = NULL)
RETURNS BOOL
AS '$libdir/timescaledb-2.20.3', 'ts_policies_remove'
LANGUAGE C VOLATILE;

/* Remove all policies */
CREATE OR REPLACE FUNCTION timescaledb_experimental.remove_all_policies(
    relation REGCLASS,
    if_exists BOOL = false)
RETURNS BOOL
AS '$libdir/timescaledb-2.20.3', 'ts_policies_remove_all'
LANGUAGE C VOLATILE;

/* Alter policies */
CREATE OR REPLACE FUNCTION timescaledb_experimental.alter_policies(
    relation REGCLASS,
    if_exists BOOL = false,
    refresh_start_offset "any" = NULL,
    refresh_end_offset "any" = NULL,
    compress_after "any" = NULL,
    drop_after "any" = NULL)
RETURNS BOOL
AS '$libdir/timescaledb-2.20.3', 'ts_policies_alter'
LANGUAGE C VOLATILE;

/* Show policies info */
CREATE OR REPLACE FUNCTION timescaledb_experimental.show_policies(
    relation REGCLASS)
RETURNS SETOF JSONB
AS '$libdir/timescaledb-2.20.3', 'ts_policies_show'
LANGUAGE C  VOLATILE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE PROCEDURE _timescaledb_functions.policy_retention(job_id INTEGER, config JSONB)
AS '$libdir/timescaledb-2.20.3', 'ts_policy_retention_proc'
LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_retention_check(config JSONB)
RETURNS void AS '$libdir/timescaledb-2.20.3', 'ts_policy_retention_check'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.policy_reorder(job_id INTEGER, config JSONB)
AS '$libdir/timescaledb-2.20.3', 'ts_policy_reorder_proc'
LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_reorder_check(config JSONB)
RETURNS void AS '$libdir/timescaledb-2.20.3', 'ts_policy_reorder_check'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.policy_recompression(job_id INTEGER, config JSONB)
AS '$libdir/timescaledb-2.20.3', 'ts_policy_recompression_proc'
LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_compression_check(config JSONB)
RETURNS void AS '$libdir/timescaledb-2.20.3', 'ts_policy_compression_check'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.policy_refresh_continuous_aggregate(job_id INTEGER, config JSONB)
AS '$libdir/timescaledb-2.20.3', 'ts_policy_refresh_cagg_proc'
LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_refresh_continuous_aggregate_check(config JSONB)
RETURNS void AS '$libdir/timescaledb-2.20.3', 'ts_policy_refresh_cagg_check'
LANGUAGE C;

CREATE OR REPLACE PROCEDURE
_timescaledb_functions.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN,
  use_creation_time   BOOLEAN,
  useam               BOOLEAN = NULL)
AS $$
DECLARE
  htoid       REGCLASS;
  chunk_rec   RECORD;
  numchunks_compressed   INTEGER := 0;
  _message     text;
  _detail      text;
  _sqlstate    text;
  -- fully compressed chunk status
  status_fully_compressed int := 1;
  -- chunk status bits:
  bit_compressed int := 1;
  bit_compressed_unordered int := 2;
  bit_frozen int := 4;
  bit_compressed_partial int := 8;
  creation_lag INTERVAL := NULL;
  chunks_failure INTEGER := 0;
BEGIN

  -- procedures with SET clause cannot execute transaction
  -- control so we adjust search_path in procedure body
  SET LOCAL search_path TO pg_catalog, pg_temp;

  SELECT format('%I.%I', schema_name, table_name) INTO htoid
  FROM _timescaledb_catalog.hypertable
  WHERE id = htid;

  -- for the integer cases, we have to compute the lag w.r.t
  -- the integer_now function and then pass on to show_chunks
  IF pg_typeof(lag) IN ('BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype) THEN
    -- cannot have use_creation_time set with this
    IF use_creation_time IS TRUE THEN
        RAISE EXCEPTION 'job % cannot use creation time with integer_now function', job_id;
    END IF;
    lag := _timescaledb_functions.subtract_integer_from_now(htoid, lag::BIGINT);
  END IF;

  -- if use_creation_time has been specified then the lag needs to be used with the
  -- "compress_created_before" argument. Otherwise the usual "older_than" argument
  -- is good enough
  IF use_creation_time IS TRUE THEN
    creation_lag := lag;
    lag := NULL;
  END IF;

  FOR chunk_rec IN
    SELECT
      show.oid, ch.schema_name, ch.table_name, ch.status
    FROM
      @extschema@.show_chunks(htoid, older_than => lag, created_before => creation_lag) AS show(oid)
      INNER JOIN pg_class pgc ON pgc.oid = show.oid
      INNER JOIN pg_namespace pgns ON pgc.relnamespace = pgns.oid
      INNER JOIN _timescaledb_catalog.chunk ch ON ch.table_name = pgc.relname AND ch.schema_name = pgns.nspname AND ch.hypertable_id = htid
    WHERE NOT ch.dropped
    AND NOT ch.osm_chunk
    -- Checking for chunks which are not fully compressed and not frozen
    AND ch.status != status_fully_compressed
    AND ch.status & bit_frozen = 0
  LOOP
    BEGIN
      IF chunk_rec.status = bit_compressed OR recompress_enabled IS TRUE THEN
        PERFORM @extschema@.compress_chunk(chunk_rec.oid, hypercore_use_access_method => useam);
        numchunks_compressed := numchunks_compressed + 1;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
          _message = MESSAGE_TEXT,
          _detail = PG_EXCEPTION_DETAIL,
          _sqlstate = RETURNED_SQLSTATE;
      RAISE WARNING 'converting chunk "%" to columnstore failed when columnstore policy is executed', chunk_rec.oid::regclass::text
          USING DETAIL = format('Message: (%s), Detail: (%s).', _message, _detail),
                ERRCODE = _sqlstate;
      chunks_failure := chunks_failure + 1;
    END;
    COMMIT;
    -- SET LOCAL is only active until end of transaction.
    -- While we could use SET at the start of the function we do not
    -- want to bleed out search_path to caller, so we do SET LOCAL
    -- again after COMMIT
    SET LOCAL search_path TO pg_catalog, pg_temp;
    IF verbose_log THEN
       RAISE LOG 'job % completed processing chunk %.%', job_id, chunk_rec.schema_name, chunk_rec.table_name;
    END IF;
    IF maxchunks > 0 AND numchunks_compressed >= maxchunks THEN
         EXIT;
    END IF;
  END LOOP;

  IF chunks_failure > 0 THEN
    RAISE EXCEPTION 'columnstore policy failure'
      USING
        DETAIL = format('Failed to convert %L chunks to columnstore. Successfully converted %L chunks.', chunks_failure, numchunks_compressed),
        ERRCODE = 'data_exception';
  END IF;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE
_timescaledb_functions.policy_compression(job_id INTEGER, config JSONB)
AS $$
DECLARE
  dimtype             REGTYPE;
  dimtypeinput        REGPROC;
  compress_after      TEXT;
  compress_created_before TEXT;
  lag_value           TEXT;
  lag_bigint_value    BIGINT;
  htid                INTEGER;
  htoid               REGCLASS;
  chunk_rec           RECORD;
  verbose_log         BOOL;
  maxchunks           INTEGER := 0;
  numchunks           INTEGER := 1;
  recompress_enabled  BOOL;
  use_creation_time   BOOL := FALSE;
  hypercore_use_access_method   BOOL;
BEGIN

  -- procedures with SET clause cannot execute transaction
  -- control so we adjust search_path in procedure body
  SET LOCAL search_path TO pg_catalog, pg_temp;

  IF config IS NULL THEN
    RAISE EXCEPTION 'job % has null config', job_id;
  END IF;

  htid := jsonb_object_field_text(config, 'hypertable_id')::INTEGER;
  IF htid is NULL THEN
    RAISE EXCEPTION 'job % config must have hypertable_id', job_id;
  END IF;

  verbose_log         := COALESCE(jsonb_object_field_text(config, 'verbose_log')::BOOLEAN, FALSE);
  maxchunks           := COALESCE(jsonb_object_field_text(config, 'maxchunks_to_compress')::INTEGER, 0);
  recompress_enabled  := COALESCE(jsonb_object_field_text(config, 'recompress')::BOOLEAN, TRUE);

  -- find primary dimension type --
  SELECT dim.column_type INTO dimtype
  FROM  _timescaledb_catalog.hypertable ht
        JOIN _timescaledb_catalog.dimension dim ON ht.id = dim.hypertable_id
  WHERE ht.id = htid
  ORDER BY dim.id
  LIMIT 1;

  compress_after      := jsonb_object_field_text(config, 'compress_after');
  IF compress_after IS NULL THEN
    compress_created_before := jsonb_object_field_text(config, 'compress_created_before');
    IF compress_created_before IS NULL THEN
        RAISE EXCEPTION 'job % config must have compress_after or compress_created_before', job_id;
    END IF;
    lag_value := compress_created_before;
    use_creation_time := true;
    dimtype := 'INTERVAL' ::regtype;
  ELSE
    lag_value := compress_after;
  END IF;

  hypercore_use_access_method := jsonb_object_field_text(config, 'hypercore_use_access_method')::bool;

  -- execute the properly type casts for the lag value
  CASE dimtype
    WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype, 'DATE'::regtype, 'INTERVAL' ::regtype  THEN
      CALL _timescaledb_functions.policy_compression_execute(job_id, htid, lag_value::INTERVAL, maxchunks, verbose_log, recompress_enabled, use_creation_time, hypercore_use_access_method);
    WHEN 'BIGINT'::regtype THEN
      CALL _timescaledb_functions.policy_compression_execute(job_id, htid, lag_value::BIGINT, maxchunks, verbose_log, recompress_enabled, use_creation_time, hypercore_use_access_method);
    WHEN 'INTEGER'::regtype THEN
      CALL _timescaledb_functions.policy_compression_execute(job_id, htid, lag_value::INTEGER, maxchunks, verbose_log, recompress_enabled, use_creation_time, hypercore_use_access_method);
    WHEN 'SMALLINT'::regtype THEN
      CALL _timescaledb_functions.policy_compression_execute(job_id, htid, lag_value::SMALLINT, maxchunks, verbose_log, recompress_enabled, use_creation_time, hypercore_use_access_method);
  END CASE;
  COMMIT;
END;
$$ LANGUAGE PLPGSQL;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Get information about the materialization table and bucket width.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_materialization_info(
    continuous_aggregate REGCLASS
) RETURNS RECORD AS
$body$
DECLARE
  info RECORD;
BEGIN
    SELECT mat_hypertable_id AS materialization_id,
           bucket_width::interval AS bucket_width
      INTO info
      FROM _timescaledb_catalog.continuous_agg,
   LATERAL _timescaledb_functions.cagg_get_bucket_function_info(mat_hypertable_id)
     WHERE format('%I.%I', user_view_schema, user_view_name)::regclass = continuous_aggregate;

    IF NOT FOUND THEN
        RAISE '"%" is not a continuous aggregate', continuous_aggregate
        USING ERRCODE = 'wrong_object_type';
    END IF;

    RETURN info;
END
$body$ LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp;

-- Get hypertable id for a hypertable and execute common checks to
-- avoid duplicating them in the overloaded functions below.
--
-- This function is part of the internal API and not intended for
-- public consumption.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_hypertable_id(
       hypertable REGCLASS,
       column_type REGTYPE
) RETURNS integer AS
$body$
DECLARE
   info RECORD;
BEGIN
   SELECT ht.id AS hypertable_id,
          di.column_type::regtype,
          EXISTS(SELECT FROM _timescaledb_catalog.continuous_agg where raw_hypertable_id = ht.id) AS has_cagg
     INTO info
     FROM _timescaledb_catalog.hypertable ht
     JOIN _timescaledb_catalog.dimension di ON ht.id = di.hypertable_id
    WHERE format('%I.%I', schema_name, table_name)::regclass = hypertable
      AND di.interval_length IS NOT NULL;

   IF info IS NULL THEN
      RAISE EXCEPTION 'table "%" is not a hypertable', hypertable
      USING ERRCODE = 'object_not_in_prerequisite_state';
   END IF;

   IF NOT info.has_cagg THEN
      RAISE EXCEPTION 'hypertable "%" has no continuous aggregate', hypertable
      USING HINT = 'Define a continuous aggregate for the hypertable to read invalidations.',
            ERRCODE = 'object_not_in_prerequisite_state';
   END IF;

   IF info.column_type <> get_hypertable_id.column_type THEN
      RAISE EXCEPTION 'wrong column type for hypertable %', hypertable
      USING HINT = format('hypertable type was "%s", but caller expected "%s"',
                          info.column_type, get_hypertable_id.column_type),
	    ERRCODE = 'datatype_mismatch';
   END IF;

   RETURN info.hypertable_id;
END
$body$ LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp;

-- Get hypertable invalidations ranges based on bucket size.
--
-- This will return a multirange for each bucket size passed in and a
-- token that can be used to accept the multirange.
--
-- Note that the token returned is not unique for each bucket size and
-- represents either the LSN or a Snapshot of what data was read to
-- produce the bucket ranges.
--
-- Currently, we only have support for timestamp with and without
-- timezone, but it is straightforward to add similar implementations
-- for integer types.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_hypertable_invalidations(
   hypertable REGCLASS,
   base TIMESTAMPTZ,
   bucket_widths INTERVAL[]
) RETURNS TABLE (bucket_width INTERVAL, token TEXT, invalidations TSTZMULTIRANGE) AS
$body$
DECLARE
   l_hypertable_id INTEGER := _timescaledb_functions.get_hypertable_id(hypertable, 'timestamptz'::regtype);
BEGIN
   RETURN QUERY (
      WITH
         -- Collect ranges from the invalidation log and convert them
         -- to correct type.
         timestamps AS MATERIALIZED (
            SELECT _timescaledb_functions.to_timestamp(lowest_modified_value) AS start,
                   _timescaledb_functions.to_timestamp(greatest_modified_value) AS finish
              FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
             WHERE hypertable_id = l_hypertable_id
         ),
         -- Since a range can end at the end of a bucket and this
         -- range should not include the next bucket, we need to
         -- subtract one microsecond before computing the bucket and
         -- then add the width again.
         ranges AS MATERIALIZED (
           SELECT width,
                  tstzrange(@extschema@.time_bucket(width, start),
                            @extschema@.time_bucket(width, finish - '1 microsecond'::interval) + width) AS bucket
             FROM timestamps CROSS JOIN UNNEST(bucket_widths) w(width)
         )
      SELECT width, pg_current_snapshot()::text, range_agg(bucket) ranges
        FROM ranges GROUP BY width
   );
END
$body$
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_hypertable_invalidations(
   hypertable REGCLASS,
   base TIMESTAMP,
   bucket_widths INTERVAL[]
) RETURNS TABLE (bucket_width INTERVAL, token TEXT, invalidations TSMULTIRANGE) AS
$body$
DECLARE
   l_hypertable_id INTEGER := _timescaledb_functions.get_hypertable_id(hypertable, 'timestamp'::regtype);
BEGIN
   RETURN QUERY (
      WITH
         -- Collect ranges from the invalidation log and convert them
         -- to correct type.
         timestamps AS MATERIALIZED (
            SELECT _timescaledb_functions.to_timestamp_without_timezone(lowest_modified_value) AS start,
                   _timescaledb_functions.to_timestamp_without_timezone(greatest_modified_value) AS finish
              FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
             WHERE hypertable_id = l_hypertable_id
         ),
         -- Compute bucket-aligned ranges from the ranges above. Since
         -- a range can end at the end of a bucket and this range
         -- should not include the next bucket, we need to subtract
         -- one microsecond before computing the bucket and then add
         -- the width again.
         ranges AS MATERIALIZED (
           SELECT width,
                  tsrange(@extschema@.time_bucket(width, start),
                          @extschema@.time_bucket(width, finish - '1 microsecond'::interval) + width) AS bucket
             FROM timestamps CROSS JOIN UNNEST(bucket_widths) w(width)
         )
      SELECT width, pg_current_snapshot()::text, range_agg(bucket)
        FROM ranges GROUP BY width
   );
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;

-- Add new invalidations to the materialization invalidation log.
--
-- This will add the range to the materialization invalidations for
-- the continuous aggregate. The range will automatically be "aligned"
-- to the bucket width to ensure that it covers all buckets that it
-- touches.
CREATE OR REPLACE PROCEDURE _timescaledb_functions.add_materialization_invalidations(
    continuous_aggregate regclass,
    invalidation tsrange
) AS
$body$
DECLARE
    info RECORD := _timescaledb_functions.get_materialization_info(continuous_aggregate);
    aligned TSRANGE := _timescaledb_functions.align_to_bucket(info.bucket_width, invalidation);
BEGIN
    INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    VALUES (info.materialization_id,
            _timescaledb_functions.to_unix_microseconds(lower(aligned)),
            _timescaledb_functions.to_unix_microseconds(upper(aligned)));
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.add_materialization_invalidations(
    continuous_aggregate REGCLASS,
    invalidation TSTZRANGE
) AS
$body$
DECLARE
    info RECORD := _timescaledb_functions.get_materialization_info(continuous_aggregate);
    aligned TSTZRANGE := _timescaledb_functions.align_to_bucket(info.bucket_width, invalidation);
BEGIN
    INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    VALUES (info.materialization_id,
            _timescaledb_functions.to_unix_microseconds(lower(aligned)),
            _timescaledb_functions.to_unix_microseconds(upper(aligned)));
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;

-- Get raw ranges from the materialization invalidation log
--
-- This is a cleaned-up version of the timestamps, still in Unix
-- microseconds, with nulls for '-infinity' and '+infinity' and
-- invalid entries removed.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_raw_materialization_ranges(typ regtype)
RETURNS TABLE (materialization_id integer,
               lowest_modified_value bigint,
               greatest_modified_value bigint)
AS $$
   WITH
     min_max_values AS MATERIALIZED (
       SELECT _timescaledb_functions.get_internal_time_min(typ) AS min,
       _timescaledb_functions.get_internal_time_max(typ) AS max
   )
   SELECT materialization_id,
          NULLIF(lowest_modified_value, min_max_values.min),
          NULLIF(greatest_modified_value, min_max_values.max)
     FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log, min_max_values
    WHERE lowest_modified_value
          BETWEEN min_max_values.min
              AND min_max_values.max
      AND greatest_modified_value
          BETWEEN min_max_values.min
              AND min_max_values.max
$$
LANGUAGE SQL
SET search_path TO pg_catalog, pg_temp;

-- Get materialization invalidations for a continuous aggregate.
--
-- Note that this will modify the materialization invalidation table
-- to be able to extract the restricted range of invalidations.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_materialization_invalidations(
    continuous_aggregate REGCLASS,
    restriction TSTZRANGE
) RETURNS TABLE (invalidations TSTZMULTIRANGE) AS
$body$
DECLARE
    info RECORD := _timescaledb_functions.get_materialization_info(continuous_aggregate);
    aligned TSTZRANGE := _timescaledb_functions.align_to_bucket(info.bucket_width, restriction);
BEGIN
    -- Compute the multirange for the invalidations inside the
    -- restriction passed down to the function and return the ranges.
    RETURN QUERY
    WITH
      ranges AS (
          SELECT materialization_id,
                 range_agg(_timescaledb_functions.make_multirange_from_internal_time(
			null::tstzrange,
			lowest_modified_value,
                        greatest_modified_value)) AS invals
            FROM _timescaledb_functions.get_raw_materialization_ranges('timestamptz'::regtype)
          GROUP BY materialization_id
      )
    SELECT range_agg(invals * multirange(aligned))
      FROM ranges
     WHERE invals && aligned
       AND materialization_id = info.materialization_id;
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.get_materialization_invalidations(
    continuous_aggregate REGCLASS,
    restriction TSRANGE
) RETURNS TABLE (invalidations TSMULTIRANGE) AS
$body$
DECLARE
    info RECORD := _timescaledb_functions.get_materialization_info(continuous_aggregate);
    aligned TSRANGE := _timescaledb_functions.align_to_bucket(info.bucket_width, restriction);
BEGIN
    -- Compute the multirange for the invalidations inside the
    -- restriction passed down to the function and return the ranges.
    RETURN QUERY
    WITH
      ranges AS (
          SELECT materialization_id,
                 range_agg(_timescaledb_functions.make_multirange_from_internal_time(
			null::tsrange,
			lowest_modified_value,
                        greatest_modified_value)) AS invals
            FROM _timescaledb_functions.get_raw_materialization_ranges('timestamp'::regtype)
          GROUP BY materialization_id
      )
    SELECT range_agg(invals * multirange(aligned))
      FROM ranges
     WHERE invals && aligned
       AND materialization_id = info.materialization_id;
END
$body$ LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp;

-- Accept a set of hypertable invalidations for a hypertable.
--
-- This procedure is used to accept all invalidations for a hypertable
-- using the token returned by a previous call of
-- get_hypertable_invalidations().
CREATE OR REPLACE PROCEDURE _timescaledb_functions.accept_hypertable_invalidations(
   hypertable REGCLASS,
   token TEXT
) AS
$body$
DECLARE
   info RECORD;
   errmsg TEXT;
BEGIN
   SELECT ht.id AS hypertable_id,
          (cagg.raw_hypertable_id IS NOT NULL) AS has_cagg
     INTO info
     FROM _timescaledb_catalog.hypertable ht
     LEFT JOIN _timescaledb_catalog.continuous_agg cagg ON cagg.raw_hypertable_id = ht.id
    WHERE format('%I.%I', schema_name, table_name)::regclass = hypertable;

   IF info IS NULL THEN
      RAISE EXCEPTION 'table "%" is not a hypertable', hypertable
      USING ERRCODE = 'object_not_in_prerequisite_state';
   END IF;

   IF NOT info.has_cagg THEN
      RAISE EXCEPTION 'hypertable "%" has no continuous aggregate', hypertable
      USING HINT = 'Define a continuous aggregate for the hypertable to handle invalidations.',
            ERRCODE = 'object_not_in_prerequisite_state';
   END IF;

   DELETE FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
    WHERE hypertable_id = info.hypertable_id
      AND pg_visible_in_snapshot(xmin::text::xid8, token::pg_snapshot);
EXCEPTION
    WHEN invalid_text_representation THEN
       RAISE EXCEPTION '%', SQLERRM
       USING HINT = 'Use the token from the get_hypertable_invalidations() call.',
             ERRCODE = 'invalid_text_representation';
END
$body$
LANGUAGE plpgsql
SET search_path TO pg_catalog, pg_temp;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_validate_query(
    query TEXT,
    OUT is_valid BOOLEAN,
    OUT error_level TEXT,
    OUT error_code TEXT,
    OUT error_message TEXT,
    OUT error_detail TEXT,
    OUT error_hint TEXT
) RETURNS RECORD AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_validate_query' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_get_bucket_function_info(
    mat_hypertable_id INTEGER,
    -- The bucket function
    OUT bucket_func REGPROCEDURE,
    -- `bucket_width` argument of the function, e.g. "1 month"
    OUT bucket_width TEXT,
    -- optional `origin` argument of the function provided by the user
    OUT bucket_origin TEXT,
    -- optional `offset` argument of the function provided by the user
    OUT bucket_offset TEXT,
    -- optional `timezone` argument of the function provided by the user
    OUT bucket_timezone TEXT,
    -- fixed or variable sized bucket
    OUT bucket_fixed_width BOOLEAN
) RETURNS RECORD AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_get_bucket_function_info' LANGUAGE C STRICT VOLATILE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains functions and procedures to migrate old continuous
-- aggregate format to the finals form (without partials).

-- Check if exists a plan for migrationg a given cagg
CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_migrate_plan_exists (
    _hypertable_id INTEGER
)
RETURNS BOOLEAN
LANGUAGE sql AS
$BODY$
    SELECT EXISTS (
        SELECT 1
        FROM _timescaledb_catalog.continuous_agg_migrate_plan
        WHERE mat_hypertable_id = _hypertable_id
        AND end_ts IS NOT NULL
    );
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Execute all pre-validations required to execute the migration
CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_migrate_pre_validation (
    _cagg_schema TEXT,
    _cagg_name TEXT,
    _cagg_name_new TEXT
)
RETURNS _timescaledb_catalog.continuous_agg
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _cagg_data _timescaledb_catalog.continuous_agg;
BEGIN
    SELECT *
    INTO _cagg_data
    FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_schema = _cagg_schema
    AND user_view_name = _cagg_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'continuous aggregate "%.%" does not exist', _cagg_schema, _cagg_name;
    END IF;

    IF _cagg_data.finalized IS TRUE THEN
        RAISE EXCEPTION 'continuous aggregate "%.%" does not require any migration', _cagg_schema, _cagg_name;
    END IF;

    IF _timescaledb_functions.cagg_migrate_plan_exists(_cagg_data.mat_hypertable_id) IS TRUE THEN
        RAISE EXCEPTION 'plan already exists for continuous aggregate %.%', _cagg_schema, _cagg_name;
    END IF;

    IF EXISTS (
        SELECT finalized
        FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_schema = _cagg_schema
        AND user_view_name = _cagg_name_new
    ) THEN
        RAISE EXCEPTION 'continuous aggregate "%.%" already exists', _cagg_schema, _cagg_name_new;
    END IF;

    RETURN _cagg_data;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Create migration plan for given cagg
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_create_plan (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _cagg_name_new TEXT,
    _override BOOLEAN DEFAULT FALSE,
    _drop_old BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _sql TEXT;
    _matht RECORD;
    _time_interval INTERVAL;
    _integer_interval BIGINT;
    _watermark TEXT;
    _policies JSONB;
    _bucket_column_name TEXT;
    _bucket_column_type TEXT;
    _interval_type TEXT;
    _interval_value TEXT;
    _nbuckets INTEGER := 10; -- number of buckets per transaction
BEGIN
    IF _timescaledb_functions.cagg_migrate_plan_exists(_cagg_data.mat_hypertable_id) IS TRUE THEN
        RAISE EXCEPTION 'plan already exists for materialized hypertable %', _cagg_data.mat_hypertable_id;
    END IF;

    -- If exist steps for this migration means that it's resuming the execution
    IF EXISTS (
        SELECT 1
        FROM _timescaledb_catalog.continuous_agg_migrate_plan_step
        WHERE mat_hypertable_id = _cagg_data.mat_hypertable_id
    ) THEN
        RAISE WARNING 'resuming the migration of the continuous aggregate "%.%"',
            _cagg_data.user_view_schema, _cagg_data.user_view_name;
        RETURN;
    END IF;

    INSERT INTO
        _timescaledb_catalog.continuous_agg_migrate_plan (mat_hypertable_id, user_view_definition)
    VALUES (
        _cagg_data.mat_hypertable_id,
        pg_get_viewdef(format('%I.%I', _cagg_data.user_view_schema, _cagg_data.user_view_name)::regclass)
    );

    SELECT schema_name, table_name
    INTO _matht
    FROM _timescaledb_catalog.hypertable
    WHERE id = _cagg_data.mat_hypertable_id;

    SELECT time_interval, integer_interval, column_name, column_type
    INTO _time_interval, _integer_interval, _bucket_column_name, _bucket_column_type
    FROM timescaledb_information.dimensions
    WHERE hypertable_schema = _matht.schema_name
    AND hypertable_name = _matht.table_name
    AND dimension_type = 'Time';

    -- Get the current cagg bucket width
    SELECT bucket_width
    INTO _interval_value
    FROM _timescaledb_functions.cagg_get_bucket_function_info(_cagg_data.mat_hypertable_id);

    IF _integer_interval IS NOT NULL THEN
        _interval_type  := _bucket_column_type;
        IF _bucket_column_type = 'bigint' THEN
            _watermark := COALESCE(_timescaledb_functions.cagg_watermark(_cagg_data.mat_hypertable_id)::bigint, '-9223372036854775808'::bigint)::TEXT;
        ELSIF _bucket_column_type = 'integer' THEN
            _watermark := COALESCE(_timescaledb_functions.cagg_watermark(_cagg_data.mat_hypertable_id)::integer, '-2147483648'::integer)::TEXT;
        ELSE
            _watermark := COALESCE(_timescaledb_functions.cagg_watermark(_cagg_data.mat_hypertable_id)::smallint, '-32768'::smallint)::TEXT;
        END IF;
    ELSE
        _interval_type  := 'interval';

        -- We expect an ISO date later in parsing (i.e., min value has to be '4714-11-24 00:53:28+00:53:28 BC')
        SET LOCAL datestyle = 'ISO, MDY';
        IF _bucket_column_type = 'timestamp with time zone' THEN
            _watermark := COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(_cagg_data.mat_hypertable_id)), '-infinity'::timestamptz)::TEXT;
        ELSE
            _watermark := COALESCE(_timescaledb_functions.to_timestamp_without_timezone(_timescaledb_functions.cagg_watermark(_cagg_data.mat_hypertable_id)), '-infinity'::timestamp)::TEXT;
        END IF;
    END IF;

    -- get all scheduled policies except the refresh
    SELECT jsonb_build_object('policies', array_agg(id ORDER BY id))
    INTO _policies
    FROM _timescaledb_config.bgw_job
    WHERE hypertable_id = _cagg_data.mat_hypertable_id
    AND proc_name IS DISTINCT FROM 'policy_refresh_continuous_aggregate'
    AND scheduled IS TRUE
    AND id >= 1000;

    INSERT INTO
        _timescaledb_catalog.continuous_agg_migrate_plan_step (mat_hypertable_id, type, config)
    VALUES
        (_cagg_data.mat_hypertable_id, 'SAVE WATERMARK', jsonb_build_object('watermark', _watermark)),
        (_cagg_data.mat_hypertable_id, 'CREATE NEW CAGG', jsonb_build_object('cagg_name_new', _cagg_name_new)),
        (_cagg_data.mat_hypertable_id, 'DISABLE POLICIES', _policies),
        (_cagg_data.mat_hypertable_id, 'REFRESH NEW CAGG', jsonb_build_object('cagg_name_new', _cagg_name_new, 'window_start', _watermark, 'window_start_type', _bucket_column_type));

    -- Finish the step because don't require any extra step
    UPDATE _timescaledb_catalog.continuous_agg_migrate_plan_step
    SET status = 'FINISHED', start_ts = now(), end_ts = clock_timestamp()
    WHERE type = 'SAVE WATERMARK';

    _sql := format (
        $$
        WITH boundaries AS (
            SELECT min(%1$I), max(%1$I), %1$L AS bucket_column_name, %2$L AS bucket_column_type, %3$L AS cagg_name_new
            FROM %4$I.%5$I
            WHERE %1$I < CAST(%6$L AS %2$s)
        )
        INSERT INTO
            _timescaledb_catalog.continuous_agg_migrate_plan_step (mat_hypertable_id, type, config)
        SELECT
            %7$L,
            'COPY DATA',
            jsonb_build_object (
                'start_ts', start::text,
                'end_ts', (start + (CAST(%8$L AS %9$s) * %10$s) )::text,
                'bucket_column_name', bucket_column_name,
                'bucket_column_type', bucket_column_type,
                'cagg_name_new', cagg_name_new
            )
        FROM boundaries,
             LATERAL generate_series(min, max, (CAST(%8$L AS %9$s) * %10$s)) AS start;
        $$,
        _bucket_column_name, _bucket_column_type, _cagg_name_new, _matht.schema_name,
        _matht.table_name, _watermark, _cagg_data.mat_hypertable_id, _interval_value,
        _interval_type, _nbuckets
    );

    EXECUTE _sql;

    -- get all scheduled policies
    SELECT jsonb_build_object('policies', array_agg(id ORDER BY id))
    INTO _policies
    FROM _timescaledb_config.bgw_job
    WHERE hypertable_id = _cagg_data.mat_hypertable_id
    AND scheduled IS TRUE
    AND id >= 1000;

    INSERT INTO
        _timescaledb_catalog.continuous_agg_migrate_plan_step (mat_hypertable_id, type, config)
    VALUES
        (_cagg_data.mat_hypertable_id, 'COPY POLICIES', _policies || jsonb_build_object('cagg_name_new', _cagg_name_new)),
        (_cagg_data.mat_hypertable_id, 'OVERRIDE CAGG', jsonb_build_object('cagg_name_new', _cagg_name_new, 'override', _override, 'drop_old', _drop_old)),
        (_cagg_data.mat_hypertable_id, 'DROP OLD CAGG', jsonb_build_object('cagg_name_new', _cagg_name_new, 'override', _override, 'drop_old', _drop_old)),
        (_cagg_data.mat_hypertable_id, 'ENABLE POLICIES', NULL);
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Create new cagg using the new format
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_execute_create_new_cagg (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _view_name              TEXT;
    _view_def               TEXT;
    _compression_enabled    BOOLEAN;
BEGIN
    _view_name := format('%I.%I', _cagg_data.user_view_schema, _plan_step.config->>'cagg_name_new');

    SELECT c.compression_enabled, left(c.view_definition, -1)
    INTO _compression_enabled, _view_def
    FROM timescaledb_information.continuous_aggregates c
    WHERE c.view_schema = _cagg_data.user_view_schema
    AND c.view_name = _cagg_data.user_view_name;

    _view_def := format(
        'CREATE MATERIALIZED VIEW %s WITH (timescaledb.continuous, timescaledb.materialized_only=%L) AS %s WITH NO DATA;',
        _view_name,
        _cagg_data.materialized_only,
        _view_def);

    EXECUTE _view_def;

    IF _compression_enabled IS TRUE THEN
        EXECUTE format('ALTER MATERIALIZED VIEW %s SET (timescaledb.compress=true)', _view_name);
    END IF;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Disable policies
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_execute_disable_policies (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _policies INTEGER[];
BEGIN
    IF _plan_step.config->>'policies' IS NOT NULL THEN
        SELECT array_agg(value::integer)
        INTO _policies
        FROM jsonb_array_elements_text( (_plan_step.config->'policies') );

        PERFORM @extschema@.alter_job(job_id, scheduled => FALSE)
        FROM unnest(_policies) job_id;
    END IF;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Enable policies
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_execute_enable_policies (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _policies INTEGER[];
BEGIN
    IF _plan_step.config->>'policies' IS NOT NULL THEN
        SELECT array_agg(value::integer)
        INTO _policies
        FROM jsonb_array_elements_text( (_plan_step.config->'policies') );

        -- set the `if_exists=>TRUE` because the cagg can be removed if the user
        -- set `drop_old=>TRUE` during the migration
        PERFORM @extschema@.alter_job(job_id, scheduled => TRUE, if_exists => TRUE)
        FROM unnest(_policies) job_id;
    END IF;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Copy policies
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_execute_copy_policies (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _mat_hypertable_id INTEGER;
    _policies INTEGER[];
    _new_policies INTEGER[];
    _bgw_job _timescaledb_config.bgw_job;
    _policy_id INTEGER;
    _config JSONB;
BEGIN
    IF _plan_step.config->>'policies' IS NULL THEN
        RETURN;
    END IF;

    SELECT array_agg(value::integer)
    INTO _policies
    FROM jsonb_array_elements_text( (_plan_step.config->'policies') );

    SELECT h.id
    INTO _mat_hypertable_id
    FROM _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
    WHERE user_view_schema = _cagg_data.user_view_schema
    AND user_view_name = _plan_step.config->>'cagg_name_new';

    -- create a temp table with all policies we'll copy
    CREATE TEMP TABLE bgw_job_temp ON COMMIT DROP AS
        SELECT *
        FROM _timescaledb_config.bgw_job
        WHERE id = ANY(_policies)
        ORDER BY id;

    -- iterate over the policies and update the necessary fields
    FOR _bgw_job IN
        SELECT *
        FROM _timescaledb_config.bgw_job
        WHERE id = ANY(_policies)
        ORDER BY id
    LOOP
        _policy_id := nextval('_timescaledb_config.bgw_job_id_seq');
        _new_policies := _new_policies || _policy_id;
        _config := jsonb_set(_bgw_job.config, '{mat_hypertable_id}', _mat_hypertable_id::text::jsonb, false);
        _config := jsonb_set(_config, '{hypertable_id}', _mat_hypertable_id::text::jsonb, false);
        UPDATE bgw_job_temp
            SET id = _policy_id,
                application_name = replace(application_name::text, _bgw_job.id::text, _policy_id::text)::name,
                config = _config,
                hypertable_id = _mat_hypertable_id
        WHERE id = _bgw_job.id;
    END LOOP;

    -- insert new policies
    INSERT INTO _timescaledb_config.bgw_job
    SELECT * FROM bgw_job_temp ORDER BY id;

    -- update the "ENABLE POLICIES" step with new policies
    UPDATE _timescaledb_catalog.continuous_agg_migrate_plan_step
    SET config = jsonb_build_object('policies', _new_policies || _policies)
    WHERE type = 'ENABLE POLICIES'
    AND mat_hypertable_id = _plan_step.mat_hypertable_id;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_update_watermark(_mat_hypertable_id INTEGER)
LANGUAGE sql AS
$BODY$
    INSERT INTO _timescaledb_catalog.continuous_aggs_watermark
    VALUES (_mat_hypertable_id, _timescaledb_functions.cagg_watermark_materialized(_mat_hypertable_id))
    ON CONFLICT (mat_hypertable_id) DO UPDATE SET watermark = excluded.watermark;
$BODY$ SECURITY DEFINER SET search_path TO pg_catalog, pg_temp;

-- Refresh new cagg created by the migration
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_execute_refresh_new_cagg (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _cagg_name TEXT;
    _override BOOLEAN;
    _mat_hypertable_id INTEGER;
BEGIN
    SELECT (config->>'override')::BOOLEAN
    INTO _override
    FROM _timescaledb_catalog.continuous_agg_migrate_plan_step
    WHERE mat_hypertable_id = _cagg_data.mat_hypertable_id
    AND type = 'OVERRIDE CAGG';

    _cagg_name = _plan_step.config->>'cagg_name_new';

    IF _override IS TRUE THEN
        _cagg_name = _cagg_data.user_view_name;
    END IF;

    --
    -- Update new cagg watermark
    --
    SELECT h.id
    INTO _mat_hypertable_id
    FROM _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
    WHERE user_view_schema = _cagg_data.user_view_schema
    AND user_view_name = _plan_step.config->>'cagg_name_new';

    CALL _timescaledb_functions.cagg_migrate_update_watermark(_mat_hypertable_id);

    --
    -- Since we're still having problems with the `refresh_continuous_aggregate` executed inside procedures
    -- and the issue isn't easy/trivial to fix we decided to skip this step here WARNING users to do it
    -- manually after the migration.
    --
    -- We didn't remove this step to make backward compatibility with potential existing and not finished
    -- migrations.
    --
    -- Related issue: (https://github.com/timescale/timescaledb/issues/4913)
    --
    RAISE WARNING
        'refresh the continuous aggregate after the migration executing this statement: "CALL @extschema@.refresh_continuous_aggregate(%, CAST(% AS %), NULL);"',
        quote_literal(format('%I.%I', _cagg_data.user_view_schema, _cagg_name)),
        quote_literal(_plan_step.config->>'window_start'),
        _plan_step.config->>'window_start_type';
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Copy data from the OLD cagg to the new Materialization Hypertable
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_execute_copy_data (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _stmt TEXT;
    _mat_schema_name TEXT;
    _mat_table_name TEXT;
    _mat_schema_name_old TEXT;
    _mat_table_name_old TEXT;
    _query TEXT;
    _select_columns TEXT;
    _groupby_columns TEXT;
BEGIN
    SELECT h.schema_name, h.table_name
    INTO _mat_schema_name, _mat_table_name
    FROM _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
    WHERE user_view_schema = _cagg_data.user_view_schema
    AND user_view_name = _plan_step.config->>'cagg_name_new';

    -- For realtime CAggs we need to read direct from the materialization hypertable
    IF _cagg_data.materialized_only IS FALSE THEN
        SELECT h.schema_name, h.table_name
        INTO _mat_schema_name_old, _mat_table_name_old
        FROM _timescaledb_catalog.continuous_agg ca
        JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
        WHERE user_view_schema = _cagg_data.user_view_schema
        AND user_view_name = _cagg_data.user_view_name;

        _query :=
            split_part(
                pg_get_viewdef(format('%I.%I', _cagg_data.user_view_schema, _cagg_data.user_view_name)),
                'UNION ALL',
                1);

        _groupby_columns :=
            split_part(
                _query,
                'GROUP BY ',
                2);

        _select_columns :=
            split_part(
                _query,
                format('FROM %I.%I', _mat_schema_name_old, _mat_table_name_old),
                1);

        _stmt := format(
            'INSERT INTO %I.%I %s FROM %I.%I WHERE %I >= %L AND %I < %L GROUP BY %s',
            _mat_schema_name,
            _mat_table_name,
            _select_columns,
            _mat_schema_name_old,
            _mat_table_name_old,
            _plan_step.config->>'bucket_column_name',
            _plan_step.config->>'start_ts',
            _plan_step.config->>'bucket_column_name',
            _plan_step.config->>'end_ts',
            _groupby_columns
        );
    ELSE
        _stmt := format(
            'INSERT INTO %I.%I SELECT * FROM %I.%I WHERE %I >= %L AND %I < %L',
            _mat_schema_name,
            _mat_table_name,
            _mat_schema_name_old,
            _mat_table_name_old,
            _plan_step.config->>'bucket_column_name',
            _plan_step.config->>'start_ts',
            _plan_step.config->>'bucket_column_name',
            _plan_step.config->>'end_ts'
        );
    END IF;

    EXECUTE _stmt;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Rename the new cagg using `_old` suffix and rename the `_new` to the original name
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_execute_override_cagg (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _stmt TEXT;
BEGIN
    IF (_plan_step.config->>'override')::BOOLEAN IS FALSE THEN
        RETURN;
    END IF;

    _stmt := 'ALTER MATERIALIZED VIEW %I.%I RENAME TO %I;';

    EXECUTE format (
        _stmt,
        _cagg_data.user_view_schema, _cagg_data.user_view_name,
        replace(_plan_step.config->>'cagg_name_new', '_new', '_old')
    );

    EXECUTE format (
        _stmt,
        _cagg_data.user_view_schema, _plan_step.config->>'cagg_name_new',
        _cagg_data.user_view_name
    );
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Remove old cagg if the parameter `drop_old` and `override` is true
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_execute_drop_old_cagg (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _stmt TEXT;
BEGIN
    IF (_plan_step.config->>'drop_old')::BOOLEAN IS FALSE THEN
        RETURN;
    END IF;

    _stmt := 'DROP MATERIALIZED VIEW %I.%I;';

    IF (_plan_step.config->>'override')::BOOLEAN IS TRUE THEN
        EXECUTE format (
            _stmt,
            _cagg_data.user_view_schema, replace(_plan_step.config->>'cagg_name_new', '_new', '_old')
        );
    END IF;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Execute the migration plan, step by step
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_execute_plan (
    _cagg_data _timescaledb_catalog.continuous_agg
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step;
    _call_stmt TEXT;
BEGIN
    FOR _plan_step IN
        SELECT *
        FROM _timescaledb_catalog.continuous_agg_migrate_plan_step
        WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _cagg_data.mat_hypertable_id
        AND status OPERATOR(pg_catalog.=) ANY (ARRAY['NOT STARTED', 'STARTED'])
        ORDER BY step_id
    LOOP
        -- change the status of the step
        UPDATE _timescaledb_catalog.continuous_agg_migrate_plan_step
        SET status = 'STARTED', start_ts = pg_catalog.clock_timestamp()
        WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _plan_step.mat_hypertable_id
        AND step_id OPERATOR(pg_catalog.=) _plan_step.step_id;
        COMMIT;

        -- SET LOCAL is only active until end of transaction.
        -- While we could use SET at the start of the function we do not
        -- want to bleed out search_path to caller, so we do SET LOCAL
        -- again after COMMIT
        SET LOCAL search_path TO pg_catalog, pg_temp;

        -- reload the step data for enable policies because the COPY DATA step update it
        IF _plan_step.type OPERATOR(pg_catalog.=) 'ENABLE POLICIES' THEN
            SELECT *
            INTO _plan_step
            FROM _timescaledb_catalog.continuous_agg_migrate_plan_step
            WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _plan_step.mat_hypertable_id
            AND step_id OPERATOR(pg_catalog.=) _plan_step.step_id;
        END IF;

        -- execute step migration
        _call_stmt := pg_catalog.format('CALL _timescaledb_functions.cagg_migrate_execute_%s($1, $2)', pg_catalog.lower(pg_catalog.replace(_plan_step.type, ' ', '_')));
        EXECUTE _call_stmt USING _cagg_data, _plan_step;

        UPDATE _timescaledb_catalog.continuous_agg_migrate_plan_step
        SET status = 'FINISHED', end_ts = pg_catalog.clock_timestamp()
        WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _plan_step.mat_hypertable_id
        AND step_id OPERATOR(pg_catalog.=) _plan_step.step_id;
        COMMIT;

        -- SET LOCAL is only active until end of transaction.
        -- While we could use SET at the start of the function we do not
        -- want to bleed out search_path to caller, so we do SET LOCAL
        -- again after COMMIT
        SET LOCAL search_path TO pg_catalog, pg_temp;
    END LOOP;
END;
$BODY$;

-- Execute the entire migration
CREATE OR REPLACE PROCEDURE @extschema@.cagg_migrate (
    cagg REGCLASS,
    override BOOLEAN DEFAULT FALSE,
    drop_old BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _cagg_schema TEXT;
    _cagg_name TEXT;
    _cagg_name_new TEXT;
    _cagg_data _timescaledb_catalog.continuous_agg;
BEGIN
    -- procedures with SET clause cannot execute transaction
    -- control so we adjust search_path in procedure body
    SET LOCAL search_path TO pg_catalog, pg_temp;

    SELECT nspname, relname
    INTO _cagg_schema, _cagg_name
    FROM pg_catalog.pg_class
    JOIN pg_catalog.pg_namespace ON pg_namespace.oid OPERATOR(pg_catalog.=) pg_class.relnamespace
    WHERE pg_class.oid OPERATOR(pg_catalog.=) cagg::pg_catalog.oid;

    -- maximum size of an identifier in Postgres is 63 characters, se we need to left space for '_new'
    _cagg_name_new := pg_catalog.format('%s_new', pg_catalog.substr(_cagg_name, 1, 59));

    -- pre-validate the migration and get some variables
    _cagg_data := _timescaledb_functions.cagg_migrate_pre_validation(_cagg_schema, _cagg_name, _cagg_name_new);

    -- create new migration plan
    CALL _timescaledb_functions.cagg_migrate_create_plan(_cagg_data, _cagg_name_new, override, drop_old);
    COMMIT;

    -- SET LOCAL is only active until end of transaction.
    -- While we could use SET at the start of the function we do not
    -- want to bleed out search_path to caller, so we do SET LOCAL
    -- again after COMMIT
    SET LOCAL search_path TO pg_catalog, pg_temp;

    -- execute the migration plan
    CALL _timescaledb_functions.cagg_migrate_execute_plan(_cagg_data);

    -- Remove chunk metadata when marked as dropped
    PERFORM _timescaledb_functions.remove_dropped_chunk_metadata(_cagg_data.raw_hypertable_id);

    -- finish the migration plan
    UPDATE _timescaledb_catalog.continuous_agg_migrate_plan
    SET end_ts = pg_catalog.clock_timestamp()
    WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _cagg_data.mat_hypertable_id;
END;
$BODY$;

-- Migrate a CAgg which is using the experimental time_bucket_ng function
-- into a CAgg using the regular time_bucket function
CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_to_time_bucket(cagg REGCLASS)
AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_migrate_to_time_bucket' LANGUAGE C;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- A retention policy is set up for the table _timescaledb_internal.job_errors (Error Log Retention Policy [2])
-- By default, it will run once a month and and drop rows older than a month.
CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_job_stat_history_retention(job_id integer, config JSONB) RETURNS integer
LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    drop_after INTERVAL;
    numrows INTEGER;
BEGIN
    drop_after := config->>'drop_after';

    DELETE
    FROM _timescaledb_internal.bgw_job_stat_history
    WHERE execution_finish < (now() - drop_after);

    GET DIAGNOSTICS numrows = ROW_COUNT;

    RETURN numrows;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_job_stat_history_retention_check(config JSONB) RETURNS VOID
LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF config IS NULL THEN
        RAISE EXCEPTION 'config cannot be NULL, and must contain drop_after';
    END IF;

    IF config->>'drop_after' IS NULL THEN
        RAISE EXCEPTION 'drop_after interval not provided';
    END IF ;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

INSERT INTO _timescaledb_config.bgw_job (
    id,
    application_name,
    schedule_interval,
    max_runtime,
    max_retries,
    retry_period,
    proc_schema,
    proc_name,
    owner,
    scheduled,
    config,
    check_schema,
    check_name,
    fixed_schedule,
    initial_start
)
VALUES
(
    3,
    'Job History Log Retention Policy [3]',
    INTERVAL '1 month',
    INTERVAL '1 hour',
    -1,
    INTERVAL '1h',
    '_timescaledb_functions',
    'policy_job_stat_history_retention',
    pg_catalog.quote_ident(current_role)::regrole,
    true,
    '{"drop_after":"1 month"}',
    '_timescaledb_functions',
    'policy_job_stat_history_retention_check',
    true,
    '2000-01-01 00:00:00+00'::timestamptz
) ON CONFLICT (id) DO NOTHING;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This function updates the dimension slice range stored in the catalog with the min and max
-- values that the OSM chunk contains. Since there is only one OSM chunk per hypertable with
-- only a time dimension, the hypertable is used to determine the corresponding slice
CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_osm_range_update(
    hypertable REGCLASS,
    range_start ANYELEMENT = NULL::bigint,
    range_end ANYELEMENT = NULL,
    empty BOOL = false
) RETURNS BOOL AS '$libdir/timescaledb-2.20.3',
'ts_hypertable_osm_range_update' LANGUAGE C VOLATILE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


-- This function return a jsonb with the following keys:
-- - columns: an array of column names that shold be used for segment by
-- - confidence: a number between 0 and 10 (most confident) indicating how sure we are.
-- - message: a message that should be displayed to the user to evaluate the result.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_segmentby_defaults(
    relation regclass
)
    RETURNS JSONB LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    _table_name NAME;
    _schema_name NAME;
    _hypertable_row _timescaledb_catalog.hypertable;
    _segmentby NAME;
    _cnt int;
BEGIN
    SELECT n.nspname, c.relname INTO STRICT _schema_name, _table_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE c.oid = relation;

    SELECT * INTO STRICT _hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.table_name = _table_name AND h.schema_name = _schema_name;

    --STEP 1 if column stats exist use unique indexes.
    --Pick the column that comes first in any such indexes
    --Select the column such that tuples are segmented evenly across distinct values.
    --Note: this will only pick a column that is NOT unique in a multi-column unique index.
    with index_attr as (
      SELECT
        a.attnum, min(a.pos) as pos
      FROM (
        SELECT indkey, indnkeyatts
        FROM pg_catalog.pg_index
        WHERE indisunique AND indrelid = relation
      ) i
      INNER JOIN LATERAL (
        SELECT * FROM unnest(i.indkey) WITH ORDINALITY
      ) a(attnum, pos) ON TRUE
      WHERE a.pos <= i.indnkeyatts
      GROUP BY a.attnum
    ),
    stats_with_stddev as (
      SELECT
        a.attname,
        i.pos,
        ROUND(stddev_pop(freqs)::numeric, 5) as freq_stddev
      FROM index_attr i
      INNER JOIN pg_attribute a ON a.attnum = i.attnum AND a.attrelid = relation
      INNER JOIN pg_stats s ON s.attname = a.attname
                            AND s.schemaname = _schema_name
                            AND s.tablename = _table_name
                            AND s.inherited = true
      LEFT JOIN LATERAL unnest(s.most_common_freqs) as freqs ON TRUE
      WHERE a.attname NOT IN (
        SELECT column_name
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = _hypertable_row.id
      )
      AND s.n_distinct > 1
      GROUP BY a.attname, i.pos
    )
    SELECT attname
    INTO _segmentby
    FROM stats_with_stddev
    ORDER BY pos ASC, freq_stddev ASC NULLS LAST
    LIMIT 1;

    IF FOUND THEN
        return json_build_object('columns', json_build_array(_segmentby), 'confidence', 10);
    END IF;


    --STEP 2 if column stats exist and no unique indexes use non-unique indexes.
    --Pick the column that comes first in any such indexes
    --Select the column such that tuples are segmented evenly across distinct values.
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where NOT indisunique and indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    ),
    stats_with_stddev as (
      SELECT
        a.attname,
        i.pos,
        ROUND(stddev_pop(freqs)::numeric, 5) as freq_stddev
      FROM index_attr i
      INNER JOIN pg_attribute a ON a.attnum = i.attnum AND a.attrelid = relation
      INNER JOIN pg_stats s ON s.attname = a.attname
                            AND s.schemaname = _schema_name
                            AND s.tablename = _table_name
                            AND s.inherited = true
      LEFT JOIN LATERAL unnest(s.most_common_freqs) as freqs ON TRUE
      WHERE a.attname NOT IN (
        SELECT column_name
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = _hypertable_row.id
      )
      AND s.n_distinct > 1
      GROUP BY a.attname, i.pos
    )
    SELECT attname
    INTO _segmentby
    FROM stats_with_stddev
    ORDER BY pos ASC, freq_stddev ASC NULLS LAST
    LIMIT 1;

    IF FOUND THEN
        return json_build_object('columns', json_build_array(_segmentby), 'confidence', 8);
    END IF;

    --STEP 3 if column stats exist but there are no indexes
    --Select the column such that tuples are segmented evenly across distinct values.
    with stats_with_stddev as (
      SELECT
        a.attname,
        ROUND(stddev_pop(freqs)::numeric, 5) as freq_stddev
      FROM pg_attribute a
      INNER JOIN pg_stats s ON s.attname = a.attname
                            AND s.schemaname = _schema_name
                            AND s.tablename = _table_name
                            AND s.inherited = true
      LEFT JOIN LATERAL unnest(s.most_common_freqs) as freqs ON TRUE
      WHERE a.attrelid = relation
        AND a.attname NOT IN (
          SELECT column_name
          FROM _timescaledb_catalog.dimension d
          WHERE d.hypertable_id = _hypertable_row.id
        )
      AND s.n_distinct > 1
      GROUP BY a.attname
    )
    SELECT attname
    INTO _segmentby
    FROM stats_with_stddev
    ORDER BY freq_stddev ASC NULLS LAST
    LIMIT 1;

    IF FOUND THEN
        return json_build_object('columns', json_build_array(_segmentby), 'confidence', 7);
    END IF;

    --STEP 4 if column stats do not exist use non-unique indexes. Pick the column that comes first in any such indexes. Ties are broken arbitrarily.
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where NOT indisunique and indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    )
    SELECT
      a.attname INTO _segmentby
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    LEFT JOIN
      pg_catalog.pg_attrdef ad ON (ad.adrelid = relation AND ad.adnum = a.attnum)
    LEFT JOIN pg_stats s ON s.attname = a.attname
                          AND s.schemaname = _schema_name
                          AND s.tablename = _table_name
                          AND s.inherited = true
    WHERE
      a.attname NOT IN (SELECT column_name FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = _hypertable_row.id)
      AND s.n_distinct is null
      AND a.attidentity = '' AND (ad.adbin IS NULL OR pg_get_expr(adbin, adrelid) not like 'nextval%')
    ORDER BY i.pos
    LIMIT 1;

    IF FOUND THEN
        return json_build_object(
            'columns', json_build_array(_segmentby),
            'confidence', 5,
            'message',  'Please make sure '|| _segmentby||' is not a unique column and appropriate for a segment by');
    END IF;

    --STEP 5 if column stats do not exist and no non-unique indexes, use unique indexes. Pick the column that comes first in any such indexes. Ties are broken arbitrarily.
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where indisunique and indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    )
    SELECT
      a.attname INTO _segmentby
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    LEFT JOIN
      pg_catalog.pg_attrdef ad ON (ad.adrelid = relation AND ad.adnum = a.attnum)
    LEFT JOIN pg_stats s ON s.attname = a.attname
                          AND s.schemaname = _schema_name
                          AND s.tablename = _table_name
                          AND s.inherited = true
    WHERE
      a.attname NOT IN (SELECT column_name FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = _hypertable_row.id)
      AND s.n_distinct is null
      AND a.attidentity = '' AND (ad.adbin IS NULL OR pg_get_expr(adbin, adrelid) not like 'nextval%')
    ORDER BY i.pos
    LIMIT 1;

    IF FOUND THEN
            return json_build_object(
            'columns', json_build_array(_segmentby),
            'confidence', 5,
            'message',  'Please make sure '|| _segmentby||' is not a unique column and appropriate for a segment by');
    END IF;


    --are there any indexed columns that are not dimemsions and are not serial/identity?
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where indisunique and indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    )
    SELECT
      count(*) INTO STRICT _cnt
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    LEFT JOIN
      pg_catalog.pg_attrdef ad ON (ad.adrelid = relation AND ad.adnum = a.attnum)
    WHERE
      a.attname NOT IN (SELECT column_name FROM _timescaledb_catalog.dimension d WHERE d.hypertable_id = _hypertable_row.id)
      AND a.attidentity = '' AND (ad.adbin IS NULL OR pg_get_expr(adbin, adrelid) not like 'nextval%');

    IF _cnt > 0 THEN
        --there are many potential candidates. We do not have enough information to choose one.
        return json_build_object(
            'columns', json_build_array(),
            'confidence', 0,
            'message',  'Several columns are potential segment by candidates and we do not have enough information to choose one. Please use the segment_by option to explicitly specify the segment_by column');
    ELSE
        --there are no potential candidates. There is a good chance no segment by is the correct choice.
        return json_build_object(
            'columns', json_build_array(),
            'confidence', 5,
            'message',  'You do not have any indexes on columns that can be used for segment_by and thus we are not using segment_by for converting to columnstore. Please make sure you are not missing any indexes');
    END IF;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- This function return a jsonb with the following keys:
-- - clauses: an array of column names and sort order key words that shold be used for order by.
-- - confidence: a number between 0 and 10 (most confident) indicating how sure we are.
-- - message: a message that should be shown to the user to evaluate the result.
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_orderby_defaults(
    relation regclass, segment_by_cols text[]
)
    RETURNS JSONB LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    _table_name NAME;
    _schema_name NAME;
    _hypertable_row _timescaledb_catalog.hypertable;
    _orderby_names NAME[];
    _dimension_names NAME[];
    _first_index_attrs NAME[];
    _orderby_clauses text[];
    _confidence int;
BEGIN
    SELECT n.nspname, c.relname INTO STRICT _schema_name, _table_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE c.oid = relation;

    SELECT * INTO STRICT _hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.table_name = _table_name AND h.schema_name = _schema_name;

    --start with the unique index columns minus the segment by columns
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
             --is there a better way to pick the right unique index if there are multiple?
            (select indkey, indnkeyatts from pg_catalog.pg_index where indisunique and indrelid = relation limit 1) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos <= i.indnkeyatts
        GROUP BY 1
    )
    SELECT
      array_agg(a.attname ORDER BY i.pos) INTO _orderby_names
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    WHERE
      NOT(a.attname::text = ANY (segment_by_cols));

    if _orderby_names is null then
        _orderby_names := array[]::name[];
        _confidence := 5;
    else
        _confidence := 8;
    end if;

    --add dimension colomns to the end. A dimension column like time should probably always be part of the order by.
    SELECT
      array_agg(d.column_name) INTO _dimension_names
    FROM _timescaledb_catalog.dimension d
    WHERE
      d.hypertable_id = _hypertable_row.id
      AND NOT(d.column_name::text = ANY (_orderby_names))
      AND NOT(d.column_name::text = ANY (segment_by_cols));
    _orderby_names := _orderby_names || _dimension_names;

    --add the first attribute of any index
    with index_attr as (
        SELECT
        a.attnum, min(a.pos) as pos
        FROM
            (select indkey, indnkeyatts from pg_catalog.pg_index where indrelid = relation) i
        INNER JOIN LATERAL
            (select * from unnest(i.indkey) with ordinality) a(attnum, pos) ON (TRUE)
        WHERE a.pos = 1
        GROUP BY 1
    )
    SELECT
      array_agg(a.attname ORDER BY i.pos) INTO _first_index_attrs
    FROM
      index_attr i
    INNER JOIN
      pg_attribute a on (a.attnum = i.attnum AND a.attrelid = relation)
    WHERE
          NOT(a.attname::text = ANY (_orderby_names))
      AND NOT(a.attname::text = ANY (segment_by_cols));

    _orderby_names := _orderby_names || _first_index_attrs;

    --add DESC to any dimensions
    SELECT
      coalesce(array_agg(
      CASE WHEN d.column_name IS NULL THEN
        format('%I', a.colname)
      ELSE
        format('%I DESC', a.colname)
      END ORDER BY pos), array[]::text[]) INTO STRICT _orderby_clauses
    FROM unnest(_orderby_names) WITH ORDINALITY as a(colname, pos)
    LEFT JOIN _timescaledb_catalog.dimension d ON (d.column_name = a.colname AND d.hypertable_id = _hypertable_row.id);


    return json_build_object('clauses', _orderby_clauses, 'confidence', _confidence);
END
$BODY$ SET search_path TO pg_catalog, pg_temp;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.bloom1_contains(_timescaledb_internal.bloom1, anyelement)
RETURNS bool
AS '$libdir/timescaledb-2.20.3', 'ts_bloom1_contains'
LANGUAGE C IMMUTABLE PARALLEL SAFE;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains utility functions and views that are used for
-- debugging in release builds. These are all placed in the schema
-- _timescaledb_debug.

CREATE OR REPLACE FUNCTION _timescaledb_debug.extension_state() RETURNS TEXT
AS '$libdir/timescaledb-2.20.3', 'ts_extension_get_state' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_debug.is_compressed_tid(tid) RETURNS BOOL
AS '$libdir/timescaledb-2.20.3', 'ts_is_compressed_tid' LANGUAGE C STRICT;

GRANT EXECUTE ON FUNCTION _timescaledb_debug.is_compressed_tid TO PUBLIC;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.get_telemetry_report()
       RETURNS jsonb AS '$libdir/timescaledb-2.20.3', 'ts_telemetry_get_report_jsonb'
       LANGUAGE C STABLE PARALLEL SAFE;

INSERT INTO _timescaledb_config.bgw_job (id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled, fixed_schedule) VALUES
(1, 'Telemetry Reporter [1]', INTERVAL '24h', INTERVAL '100s', -1, INTERVAL '1h', '_timescaledb_functions', 'policy_telemetry', pg_catalog.quote_ident(current_role)::regrole, true, false)
ON CONFLICT (id) DO NOTHING;
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- TimescaleDB 2.12 moved all functions present in _timescaledb_internal into
-- _timescaledb_functions. This file contains a compatibility layer to allow
-- for more flexibility when migrating for any users calling these internal
-- functions.
-- This compatibility layer will be removed in a future versions.


CREATE OR REPLACE FUNCTION _timescaledb_internal.alter_job_set_hypertable_id(job_id integer, hypertable regclass) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.alter_job_set_hypertable_id(integer,regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.alter_job_set_hypertable_id($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.attach_osm_table_chunk(hypertable regclass, chunk regclass) RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.attach_osm_table_chunk(regclass,regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.attach_osm_table_chunk($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_migrate_plan_exists(_hypertable_id integer) RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.cagg_migrate_plan_exists(integer) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.cagg_migrate_plan_exists($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_migrate_pre_validation(_cagg_schema text,_cagg_name text,_cagg_name_new text) RETURNS _timescaledb_catalog.continuous_agg LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.cagg_migrate_pre_validation(text,text,text) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.cagg_migrate_pre_validation($1,$2,$3);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_watermark(hypertable_id integer) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.cagg_watermark(integer) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.cagg_watermark($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_watermark_materialized(hypertable_id integer) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.cagg_watermark_materialized(integer) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.cagg_watermark_materialized($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.calculate_chunk_interval(dimension_id integer,dimension_coord bigint,chunk_target_size bigint) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.calculate_chunk_interval(integer,bigint,bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.calculate_chunk_interval($1,$2,$3);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_constraint_add_table_constraint(chunk_constraint_row _timescaledb_catalog.chunk_constraint) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.chunk_constraint_add_table_constraint(_timescaledb_catalog.chunk_constraint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.chunk_constraint_add_table_constraint($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_id_from_relid(relid oid) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.chunk_id_from_relid(oid) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.chunk_id_from_relid($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_clone(chunk_index_oid oid) RETURNS oid LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.chunk_index_clone(oid) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.chunk_index_clone($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_replace(chunk_index_oid_old oid,chunk_index_oid_new oid) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.chunk_index_replace(oid,oid) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.chunk_index_replace($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_status(regclass) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.chunk_status(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.chunk_status($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunks_local_size(schema_name_in name,table_name_in name) RETURNS TABLE (chunk_id integer, chunk_schema NAME, chunk_name  NAME, table_bytes bigint, index_bytes bigint, toast_bytes bigint, total_bytes bigint) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.chunks_local_size(name,name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.chunks_local_size($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_chunk_local_stats(schema_name_in name,table_name_in name) RETURNS TABLE (chunk_schema name, chunk_name name, compression_status text, before_compression_table_bytes bigint, before_compression_index_bytes bigint, before_compression_toast_bytes bigint, before_compression_total_bytes bigint, after_compression_table_bytes bigint, after_compression_index_bytes bigint, after_compression_toast_bytes bigint, after_compression_total_bytes bigint) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.compressed_chunk_local_stats(name,name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.compressed_chunk_local_stats($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.compressed_chunk_remote_stats(schema_name_in name,table_name_in name) RETURNS TABLE ( chunk_schema name, chunk_name name, compression_status text, before_compression_table_bytes bigint, before_compression_index_bytes bigint, before_compression_toast_bytes bigint, before_compression_total_bytes bigint, after_compression_table_bytes bigint, after_compression_index_bytes bigint, after_compression_toast_bytes bigint, after_compression_total_bytes bigint, node_name name) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.compressed_chunk_remote_stats(name,name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.compressed_chunk_remote_stats($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger() RETURNS TRIGGER
AS '$libdir/timescaledb-2.20.3', 'ts_continuous_agg_invalidation_trigger' LANGUAGE C;

-- we have to prefix slices, schema_name and table_name parameter with _ here to not clash with output names otherwise plpgsql will complain
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk(hypertable regclass,_slices jsonb,_schema_name name=NULL,_table_name name=NULL,chunk_table regclass=NULL) RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB, created BOOLEAN) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.create_chunk(regclass,jsonb,name,name,regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.create_chunk($1,$2,$3,$4,$5);
END$$
SET search_path TO pg_catalog,pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_internal.create_compressed_chunk(chunk regclass,chunk_table regclass,uncompressed_heap_size bigint,uncompressed_toast_size bigint,uncompressed_index_size bigint,compressed_heap_size bigint,compressed_toast_size bigint,compressed_index_size bigint,numrows_pre_compression bigint,numrows_post_compression bigint) RETURNS regclass LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.create_compressed_chunk(regclass,regclass,bigint,bigint,bigint,bigint,bigint,bigint,bigint,bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.create_compressed_chunk($1,$2,$3,$4,$5,$6,$7,$8,$9,$10);
END$$
SET search_path TO pg_catalog,pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk(chunk regclass) RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.drop_chunk(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.drop_chunk($1);
END$$
SET search_path TO pg_catalog,pg_temp;


-- We cannot create a wrapper function in plpgsql for the aggregate transition
-- functions because plpgsql cannot deal with datatype internal but since these
-- are used in an aggregation context and cannot be called directly and will
-- be used in conjunction with partialize_agg it is sufficient to have the
-- warning there.
CREATE OR REPLACE FUNCTION _timescaledb_internal.finalize_agg_ffunc(
tstate internal, aggfn TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val ANYELEMENT)
RETURNS anyelement
AS '$libdir/timescaledb-2.20.3', 'ts_finalize_agg_ffunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.finalize_agg_sfunc(
tstate internal, aggfn TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val ANYELEMENT)
RETURNS internal
AS '$libdir/timescaledb-2.20.3', 'ts_finalize_agg_sfunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE AGGREGATE _timescaledb_internal.finalize_agg(agg_name TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val anyelement) (
    SFUNC = _timescaledb_functions.finalize_agg_sfunc,
    STYPE = internal,
    FINALFUNC = _timescaledb_functions.finalize_agg_ffunc,
    FINALFUNC_EXTRA
);

CREATE OR REPLACE FUNCTION _timescaledb_internal.freeze_chunk(chunk regclass) RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.freeze_chunk(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.freeze_chunk($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.generate_uuid() RETURNS uuid LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.generate_uuid() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.generate_uuid();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_approx_row_count(relation regclass) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_approx_row_count(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.get_approx_row_count($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_compressed_chunk_index_for_recompression(uncompressed_chunk regclass) RETURNS regclass LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_compressed_chunk_index_for_recompression(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.get_compressed_chunk_index_for_recompression($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_create_command(table_name name) RETURNS text LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_create_command(name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.get_create_command($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_git_commit() RETURNS TABLE(commit_tag TEXT, commit_hash TEXT, commit_time TIMESTAMPTZ) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_git_commit() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.get_git_commit();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_os_info() RETURNS TABLE(sysname TEXT, version TEXT, release TEXT, version_pretty TEXT) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_os_info() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.get_os_info();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_for_key(val anyelement) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_partition_for_key(anyelement) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.get_partition_for_key($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_hash(val anyelement) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.get_partition_hash(anyelement) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.get_partition_hash($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.hypertable_local_size(schema_name_in name,table_name_in name) RETURNS TABLE ( table_bytes BIGINT, index_bytes BIGINT, toast_bytes BIGINT, total_bytes BIGINT) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.hypertable_local_size(name,name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.hypertable_local_size($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.indexes_local_size(schema_name_in name,table_name_in name) RETURNS TABLE (hypertable_id INTEGER, total_bytes BIGINT) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.indexes_local_size(name,name) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.indexes_local_size($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.insert_blocker() RETURNS trigger LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.insert_blocker() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.insert_blocker();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.interval_to_usec(chunk_interval interval) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.interval_to_usec(interval) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.interval_to_usec($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.partialize_agg(arg anyelement) RETURNS bytea LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.partialize_agg(anyelement) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.partialize_agg($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_compression_check(config jsonb) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_compression_check(jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.policy_compression_check($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_job_stat_history_retention(job_id integer,config jsonb) RETURNS integer LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_job_stat_history_retention(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.policy_job_stat_history_retention($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_job_stat_history_retention_check(config jsonb) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_job_stat_history_retention_check(jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.policy_job_stat_history_retention_check($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_refresh_continuous_aggregate_check(config jsonb) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_refresh_continuous_aggregate_check(jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.policy_refresh_continuous_aggregate_check($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_reorder_check(config jsonb) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_reorder_check(jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.policy_reorder_check($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_retention_check(config jsonb) RETURNS void LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.policy_retention_check(jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM _timescaledb_functions.policy_retention_check($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.process_ddl_event() RETURNS event_trigger LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.process_ddl_event() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.process_ddl_event();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.range_value_to_pretty(time_value bigint,column_type regtype) RETURNS text LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.range_value_to_pretty(bigint,regtype) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.range_value_to_pretty($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.recompress_chunk_segmentwise(uncompressed_chunk regclass,if_compressed boolean=false) RETURNS regclass LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.recompress_chunk_segmentwise(regclass,boolean) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.recompress_chunk_segmentwise($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.relation_size(relation regclass) RETURNS TABLE (total_size BIGINT, heap_size BIGINT, index_size BIGINT, toast_size BIGINT) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.relation_size(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.relation_size($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.restart_background_workers() RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.restart_background_workers() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.restart_background_workers();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.show_chunk(chunk regclass) RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.show_chunk(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN QUERY SELECT * FROM _timescaledb_functions.show_chunk($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.start_background_workers() RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.start_background_workers() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.start_background_workers();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.stop_background_workers() RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.stop_background_workers() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.stop_background_workers();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.subtract_integer_from_now(hypertable_relid regclass,lag bigint) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.subtract_integer_from_now(regclass,bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.subtract_integer_from_now($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.time_to_internal(time_val anyelement) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.time_to_internal(anyelement) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.time_to_internal($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.to_date(unixtime_us bigint) RETURNS date LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.to_date(bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.to_date($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.to_interval(unixtime_us bigint) RETURNS interval LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.to_interval(bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.to_interval($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp(unixtime_us bigint) RETURNS timestamp with time zone LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.to_timestamp(bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.to_timestamp($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp_without_timezone(unixtime_us bigint) RETURNS timestamp without time zone LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.to_timestamp_without_timezone(bigint) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.to_timestamp_without_timezone($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.to_unix_microseconds(ts timestamp with time zone) RETURNS bigint LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.to_unix_microseconds(timestamp with time zone) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.to_unix_microseconds($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_loaded() RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.tsl_loaded() is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.tsl_loaded();
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE FUNCTION _timescaledb_internal.unfreeze_chunk(chunk regclass) RETURNS boolean LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'function _timescaledb_internal.unfreeze_chunk(regclass) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  RETURN _timescaledb_functions.unfreeze_chunk($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_create_plan(_cagg_data _timescaledb_catalog.continuous_agg,_cagg_name_new text,_override boolean=false,_drop_old boolean=false) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_create_plan(_timescaledb_catalog.continuous_agg,text,boolean,boolean) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_create_plan($1,$2,$3,$4);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_copy_data(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_copy_data(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_copy_data($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_copy_policies(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_copy_policies(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_copy_policies($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_create_new_cagg(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_create_new_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_create_new_cagg($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_disable_policies(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_disable_policies(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_disable_policies($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_drop_old_cagg(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_drop_old_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_drop_old_cagg($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_enable_policies(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_enable_policies(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_enable_policies($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_override_cagg(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_override_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_override_cagg($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_plan(_cagg_data _timescaledb_catalog.continuous_agg) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_plan(_timescaledb_catalog.continuous_agg) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_plan($1);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg(_cagg_data _timescaledb_catalog.continuous_agg,_plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.cagg_migrate_execute_refresh_new_cagg($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_compression(job_id integer,config jsonb) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_compression(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_compression($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_compression_execute(job_id integer,htid integer,lag anyelement,maxchunks integer,verbose_log boolean,recompress_enabled boolean,use_creation_time boolean) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_compression_execute(integer,integer,anyelement,integer,boolean,boolean,boolean) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_compression_execute($1,$2,$3,$4,$5,$6,$7);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_recompression(job_id integer,config jsonb) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_recompression(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_recompression($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_refresh_continuous_aggregate(job_id integer,config jsonb) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_refresh_continuous_aggregate(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_refresh_continuous_aggregate($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_reorder(job_id integer,config jsonb) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_reorder(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_reorder($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


CREATE OR REPLACE PROCEDURE _timescaledb_internal.policy_retention(job_id integer,config jsonb) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure _timescaledb_internal.policy_retention(integer,jsonb) is deprecated and has been moved to _timescaledb_functions schema. this compatibility function will be removed in a future version.';
  END IF;
  CALL _timescaledb_functions.policy_retention($1,$2);
END$$
SET search_path TO pg_catalog,pg_temp;


-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT _timescaledb_functions.restart_background_workers();
-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

COMMENT ON EXTENSION timescaledb IS 'Enables scalable inserts and complex queries for time-series data (Community Edition)';
set timescaledb.update_script_stage = 'post';
-- needed post 1.7.0 to fixup continuous aggregates created in 1.7.0 ---
DO
$$
DECLARE
 vname regclass;
 mat_ht_id INTEGER;
 materialized_only bool;
 finalized bool;
 ts_major INTEGER;
 ts_minor INTEGER;
BEGIN
    -- procedures with SET clause cannot execute transaction
    -- control so we adjust search_path in procedure body
    SET LOCAL search_path TO pg_catalog, pg_temp;

    SELECT ((string_to_array(extversion,'.'))[1])::int, ((string_to_array(extversion,'.'))[2])::int
    INTO ts_major, ts_minor
    FROM pg_extension WHERE extname = 'timescaledb';

    IF ts_major >= 2 AND ts_minor >= 7 THEN
      CREATE PROCEDURE _timescaledb_functions.post_update_cagg_try_repair(
        cagg_view REGCLASS, force_rebuild BOOLEAN
      ) AS '$libdir/timescaledb-2.20.3', 'ts_cagg_try_repair' LANGUAGE C;
    END IF;

    FOR vname, mat_ht_id, materialized_only, finalized IN
      SELECT format('%I.%I', cagg.user_view_schema, cagg.user_view_name)::regclass, cagg.mat_hypertable_id, cagg.materialized_only, cagg.finalized
      FROM _timescaledb_catalog.continuous_agg cagg
    LOOP
      IF ts_major < 2 THEN
        EXECUTE format('ALTER VIEW %s SET (timescaledb.materialized_only=%L) ', vname::text, materialized_only);

      ELSIF ts_major = 2 AND ts_minor < 7 THEN
        EXECUTE format('ALTER MATERIALIZED VIEW %s SET (timescaledb.materialized_only=%L) ', vname::text, materialized_only);

      ELSIF ts_major = 2 AND ts_minor >= 7 THEN
        SET log_error_verbosity TO VERBOSE;
        CALL _timescaledb_functions.post_update_cagg_try_repair(vname, false);

      END IF;
    END LOOP;

    IF ts_major >= 2 AND ts_minor >= 7 THEN
      DROP PROCEDURE IF EXISTS _timescaledb_functions.post_update_cagg_try_repair(REGCLASS, BOOLEAN);
    END IF;
END
$$ LANGUAGE PLPGSQL;

-- can only be dropped after views have been rebuilt
DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_watermark(oid);

-- For objects that are newly created, we need to set the initprivs to
-- the initprivs for some table that was created in the installation
-- of the TimescaleDB extension and not as part of any update.
--
-- We chose the "chunk" catalog table for this since that is created
-- in the first version of TimescaleDB and should have the correct
-- initprivs, but we could use any other table that existed in the
-- first installation.
INSERT INTO _timescaledb_internal.saved_privs
     SELECT nspname, relname, relacl,
       (SELECT tmpini FROM _timescaledb_internal.saved_privs
        WHERE tmpnsp = '_timescaledb_catalog' AND tmpname = 'chunk')
       FROM pg_class JOIN pg_namespace ns ON ns.oid = relnamespace
         LEFT JOIN _timescaledb_internal.saved_privs ON tmpnsp = nspname AND tmpname = relname
      WHERE relkind IN ('r', 'v') AND nspname IN ('_timescaledb_catalog', '_timescaledb_config')
        OR nspname = '_timescaledb_internal'
        AND relname IN ('hypertable_chunk_local_size', 'compressed_chunk_stats',
                        'bgw_job_stat', 'bgw_policy_chunk_stats', 'job_errors')
ON CONFLICT DO NOTHING;

-- The above is good enough for tables and views. However sequences need to
-- use the "chunk_id_seq" catalog sequence as a template
INSERT INTO _timescaledb_internal.saved_privs
     SELECT nspname, relname, relacl,
        (SELECT tmpini FROM _timescaledb_internal.saved_privs
	     WHERE tmpnsp = '_timescaledb_catalog' AND tmpname = 'chunk_id_seq')
        FROM pg_class JOIN pg_namespace ns ON ns.oid = relnamespace
		    LEFT JOIN _timescaledb_internal.saved_privs ON tmpnsp = nspname AND tmpname = relname
      WHERE relkind IN ('S') AND nspname IN ('_timescaledb_catalog', '_timescaledb_config')
        OR nspname = '_timescaledb_internal'
        AND relname IN ('hypertable_chunk_local_size', 'compressed_chunk_stats',
                        'bgw_job_stat', 'bgw_policy_chunk_stats')
ON CONFLICT DO NOTHING;

-- We can now copy back saved initprivs.
WITH to_update AS (
     SELECT objoid, tmpini
     FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace
        JOIN pg_init_privs ip ON ip.objoid = cl.oid AND ip.objsubid = 0
        JOIN _timescaledb_internal.saved_privs ON tmpnsp = nspname AND tmpname = relname)
UPDATE pg_init_privs
   SET initprivs = tmpini
  FROM to_update
 WHERE to_update.objoid = pg_init_privs.objoid
   AND classoid = 'pg_class'::regclass
   AND objsubid = 0;

-- Can only restore permissions on views after they have been rebuilt,
-- so we restore for all types of objects here.
WITH to_update AS (
     SELECT cl.oid, tmpacl
     FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace
                      JOIN _timescaledb_internal.saved_privs ON tmpnsp = nspname AND tmpname = relname)
UPDATE pg_class cl SET relacl = tmpacl
  FROM to_update WHERE cl.oid = to_update.oid;

DROP TABLE _timescaledb_internal.saved_privs;

-- warn about partial storage format change for numeric
DO $$
DECLARE
  cagg_name text;
  cagg_column text;
  cnt int := 0;
BEGIN
  IF current_setting('server_version_num')::int <  140000 THEN
    FOR cagg_name, cagg_column IN
      SELECT
        attrelid::regclass::text,
        att.attname
      FROM _timescaledb_catalog.continuous_agg cagg
      INNER JOIN pg_attribute att ON (
        att.attrelid = format('%I.%I',cagg.user_view_schema,cagg.user_view_name)::regclass AND
        atttypid = 'numeric'::regtype)
      WHERE cagg.finalized = false
    LOOP
      RAISE WARNING 'Continuous Aggregate: % column: %', cagg_name, cagg_column;
      cnt := cnt + 1;
    END LOOP;
    IF cnt > 0 THEN
      RAISE WARNING 'The aggregation state format for numeric changed between PG13 and PG14. You should upgrade the above mentioned caggs to the new format before upgrading to PG14';
    END IF;
  END IF;
END $$;

-- Report warning when partial aggregates are used
DO $$
DECLARE
  cagg_name text;
BEGIN
    FOR cagg_name IN
      SELECT
        format('%I.%I', user_view_schema, user_view_name)
      FROM _timescaledb_catalog.continuous_agg
      WHERE finalized IS FALSE
      ORDER BY 1
    LOOP
      RAISE WARNING 'Continuous Aggregate "%" with old format will not be supported in the next version. You should use `cagg_migrate` procedure to migrate to the new format.', cagg_name;
    END LOOP;
END $$;

-- Create watermark record when required
DO
$$
DECLARE
  ts_version TEXT;
BEGIN
    SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';
    IF ts_version >= '2.11.0' THEN
      INSERT INTO _timescaledb_catalog.continuous_aggs_watermark (mat_hypertable_id, watermark)
      SELECT a.mat_hypertable_id, _timescaledb_functions.cagg_watermark_materialized(a.mat_hypertable_id)
      FROM _timescaledb_catalog.continuous_agg a
      LEFT JOIN _timescaledb_catalog.continuous_aggs_watermark b ON b.mat_hypertable_id = a.mat_hypertable_id
      WHERE b.mat_hypertable_id IS NULL
      ORDER BY 1;
    END IF;
END;
$$;

-- Repair relations that have relacl entries for users that do not
-- exist in pg_authid
CALL _timescaledb_functions.repair_relation_acls();

-- Cleanup orphaned compression settings
WITH orphaned_settings AS (
     SELECT cs.relid, cl.relname
     FROM _timescaledb_catalog.compression_settings cs
     LEFT JOIN pg_class cl ON (cs.relid = cl.oid)
     WHERE cl.relname IS NULL
)
DELETE FROM _timescaledb_catalog.compression_settings AS cs
USING orphaned_settings AS os WHERE cs.relid = os.relid;
set timescaledb.update_script_stage = '';
