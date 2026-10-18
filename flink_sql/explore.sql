-- =====================================================================
-- Exploration queries (Labs 1 & 2). Not submitted by start.sh: run them one at a
-- time in the SQL editor, or in a SQL Client session started with the DDL (-i).
-- =====================================================================


-- ------------------------------------- What survives a session, and what does not
-- In a session with no DDL: dwh survives (CatalogStore), the default catalog's tables do not.

SHOW CATALOGS;   -- default_catalog, dwh
SHOW TABLES;     -- empty: purchase_stream and friends are gone with the last session

SET 'execution.runtime-mode' = 'batch';
SELECT count(*) FROM dwh.sales_report.purchase_report;
SELECT * FROM dwh.sales_report.feedback_summary LIMIT 5;
SET 'execution.runtime-mode' = 'streaming';


-- ------------------------------------------- Lab 1, Step 3: raw purchase events
SELECT * FROM purchase_stream;


-- -------------------------------------------- Lab 1, Step 5: the product dimension
-- A snapshot of the products, then every change. Leave it running and, in a terminal:
--   docker compose exec postgres psql -U root -d warehouse \
--     -c "UPDATE master_product SET price = 9.99 WHERE product_id = 'CS01';"
-- Cancel it before the next query: it holds the replication slot.

SELECT product_id, item, category, price, update_time FROM master_product;


-- ----------------------------------------- Lab 2, Step 1: enrich the purchase stream
-- Each purchase with its product as it was at transaction_time.

SET 'table.exec.source.idle-timeout' = '5 s';

SELECT
  p.transaction_time,
  mp.item,
  mp.category,
  p.quantity,
  p.total_purchase
FROM purchase_stream AS p
JOIN master_product FOR SYSTEM_TIME AS OF p.transaction_time AS mp
  ON p.product_id = mp.product_id;


-- ------------------------------------------ Lab 2, Step 2: TUMBLE, 10 seconds
-- Windowing TVF: window_start / window_end become ordinary columns.

CREATE TEMPORARY VIEW enriched AS
SELECT p.transaction_time, p.state, p.quantity, p.total_purchase, mp.item, mp.category
FROM purchase_stream AS p
JOIN master_product FOR SYSTEM_TIME AS OF p.transaction_time AS mp
  ON p.product_id = mp.product_id;

SELECT
  item,
  SUM(total_purchase) AS sum_total_purchase,
  window_start        AS purchase_window
FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(transaction_time), INTERVAL '10' SECONDS))
GROUP BY window_start, window_end, item;


-- ------------------------- Lab 2, Step 3: HOP — last 60s, reported every 10s
-- HOP takes slide, then size: a 60 s window every 10 s.

SELECT
  item,
  SUM(total_purchase) AS sum_total_purchase,
  window_start        AS purchase_window
FROM TABLE(
  HOP(TABLE enriched, DESCRIPTOR(transaction_time), INTERVAL '10' SECONDS, INTERVAL '60' SECONDS)
)
WHERE category = 'Superfoods Smoothies'
GROUP BY window_start, window_end, item;


-- --------------------- Lab 2, Step 4: the full report shape (what job 1 sinks)
SELECT
  item,
  category,
  state,
  COUNT(*)            AS count_transactions,
  SUM(quantity)       AS sum_quantity,
  SUM(total_purchase) AS sum_total_purchase,
  AVG(total_purchase) AS avg_total_purchase,
  window_start        AS purchase_window
FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(transaction_time), INTERVAL '30' SECONDS))
GROUP BY window_start, window_end, item, category, state;


-- --------------------------------------------- The model, called interactively
-- ML_PREDICT on a few bad reviews: see what the model returns before the job parses it.

CREATE TEMPORARY VIEW quick_reviews AS
SELECT
  review_id, overall_score, product_name,
  CONCAT('Product: ', product_name, ' | Overall: ', CAST(overall_score AS STRING),
         ' | Review: ', review_text) AS review_summary
FROM feedback
WHERE overall_score < 6;

SELECT review_id, overall_score, product_name, `output`
FROM ML_PREDICT(TABLE quick_reviews, MODEL ai_analyze_review, DESCRIPTOR(review_summary));
