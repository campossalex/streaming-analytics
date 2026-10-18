-- =====================================================================
-- Sources, the product dimension, and the PostgreSQL sinks.
-- DDL only: loaded into every job's session with `sql-client.sh -i`.
-- Placeholders (${...}) are filled in by submit.sh / the SQL editor.
-- =====================================================================


-- ------------------------------------------------------------ Kafka: purchases
-- transaction_time is the event time: it drives the temporal join and the window.

CREATE TABLE purchase_stream (
  transaction_time TIMESTAMP(3),
  transaction_id   STRING,
  product_id       STRING,
  price            FLOAT,
  quantity         INT,
  state            STRING,
  is_member        BOOLEAN,
  member_discount  FLOAT,
  add_supplements  BOOLEAN,
  supplement_price FLOAT,
  total_purchase   FLOAT,
  WATERMARK FOR transaction_time AS transaction_time - INTERVAL '1' SECOND
) WITH (
  'connector'                    = 'kafka',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id'          = 'flink-coffee-purchases',
  'scan.startup.mode'            = 'earliest-offset',
  'topic'                        = 'store.purchases'
);


-- ------------------------------------------------------------ Kafka: feedback
-- Product name and category come with every review, so no join is needed.

CREATE TABLE feedback (
  review_id          STRING,
  transaction_id     STRING,
  customer_id        STRING,
  store_state        STRING,
  product_name       STRING,
  product_category   STRING,
  overall_score      INT,
  product_quality    INT,
  service_speed      INT,
  staff_friendliness INT,
  cleanliness        INT,
  value_for_money    INT,
  review_text        STRING,
  visit_type         STRING,
  would_return       BOOLEAN,
  event_time         STRING,
  event_ts  AS TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss.SSS'),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id'          = 'flink-coffee-feedback',
  'scan.startup.mode'            = 'earliest-offset',
  'topic'                        = 'store.feedback'
);


-- ------------------------------- PostgreSQL: the product dimension (CDC, versioned)
-- A snapshot of master_product, then every change. The primary key and watermark make
-- it a versioned table, so a purchase is joined with the product as it was at that time.
-- Each running query holds the replication slot: one reader at a time.

CREATE TABLE master_product (
  product_id        STRING,
  category          STRING,
  item              STRING,
  size              STRING,
  cogs              DECIMAL(10,2),
  price             DECIMAL(10,2),
  inventory_level   INT,
  contains_fruit    BOOLEAN,
  contains_veggies  BOOLEAN,
  contains_nuts     BOOLEAN,
  contains_caffeine BOOLEAN,
  op_ts             TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  update_time       AS CAST(op_ts AS TIMESTAMP(3)),
  PRIMARY KEY (product_id) NOT ENFORCED,
  WATERMARK FOR update_time AS update_time
) WITH (
  'connector'                         = 'postgres-cdc',
  'hostname'                          = '${PG_HOST}',
  'port'                              = '${PG_PORT}',
  'username'                          = '${PG_USER}',
  'password'                          = '${PG_PASSWORD}',
  'database-name'                     = 'warehouse',
  'schema-name'                       = 'public',
  'table-name'                        = 'master_product',
  'slot.name'                         = 'flink_master_product',
  'decoding.plugin.name'              = 'pgoutput',
  'debezium.publication.name'         = 'master_product_pub',
  'debezium.publication.autocreate.mode' = 'disabled',
  'scan.incremental.snapshot.enabled' = 'true'
);


-- ------------------------------------------ PostgreSQL catalog (Lab 3, Step 1)
-- Reflects the PostgreSQL tables, so dwh.sales_report.purchase_report needs no DDL.
-- The registration survives sessions (file CatalogStore), hence IF NOT EXISTS.
CREATE CATALOG IF NOT EXISTS dwh WITH (
  'type'             = 'jdbc',
  'base-url'         = 'jdbc:postgresql://postgres:5432',
  'default-database' = 'sales_report',
  'username'         = '${PG_USER}',
  'password'         = '${PG_PASSWORD}'
);


-- -------------------------------------------- PostgreSQL: the model output store

CREATE TABLE feedback_summary_sink (
  review_id          STRING,
  customer_id        STRING,
  store_state        STRING,
  product_name       STRING,
  product_category   STRING,
  overall_score      INT,
  product_quality    INT,
  service_speed      INT,
  staff_friendliness INT,
  cleanliness        INT,
  value_for_money    INT,
  visit_type         STRING,
  would_return       BOOLEAN,
  review_text        STRING,
  summary            STRING,
  improvement_1      STRING,
  improvement_2      STRING,
  processed_at       TIMESTAMP(3),
  PRIMARY KEY (review_id) NOT ENFORCED
) WITH (
  'connector'  = 'jdbc',
  'url'        = '${PG_URL}',
  'table-name' = 'public.feedback_summary',
  'username'   = '${PG_USER}',
  'password'   = '${PG_PASSWORD}'
);
