-- Sales report database — sink for the Flink aggregation job.
-- Runs automatically via docker-entrypoint-initdb.d on first container start.
--
-- The `root` user and the sales_report database are created by the postgres image from POSTGRES_USER /
-- POSTGRES_DB in docker-compose.yml, so this file only creates the tables.

\c sales_report

CREATE TABLE IF NOT EXISTS purchase_report (
  dim_item                  VARCHAR(255),
  dim_category              VARCHAR(255),
  dim_state                 VARCHAR(255),
  purchase_time             TIMESTAMP,
  fact_count_transactions   FLOAT,
  fact_sum_quantity         FLOAT,
  fact_sum_price            FLOAT,
  fact_sum_member_discount  FLOAT,
  fact_sum_supplement_price FLOAT,
  fact_sum_total_purchase   FLOAT,
  fact_avg_total_purchase   FLOAT,
  PRIMARY KEY (dim_item, dim_category, dim_state, purchase_time)
);

CREATE INDEX IF NOT EXISTS idx_timestamp
  ON purchase_report (dim_item, dim_category, dim_state, purchase_time);

ALTER TABLE purchase_report OWNER TO root;
