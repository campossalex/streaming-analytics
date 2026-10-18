-- =====================================================================
-- JOB 1 of 2: sales
--   store.purchases + master_product (CDC) -> TUMBLE 30s -> purchase_report
-- Submit on its own with:  ./submit.sh sales
-- =====================================================================

SET 'pipeline.name' = 'coffee-1-sales';

-- master_product's watermark rarely moves: mark it idle so the join can progress.
SET 'table.exec.source.idle-timeout' = '5 s';

-- Each purchase is joined with its product as it was at transaction_time, then summed
-- in 30 s windows and upserted into PostgreSQL through the dwh catalog.
INSERT INTO dwh.sales_report.purchase_report
WITH purchases_enriched AS (
  SELECT
    p.transaction_time,
    p.state,
    p.quantity,
    p.price,
    p.member_discount,
    p.supplement_price,
    p.total_purchase,
    mp.item,
    mp.category
  FROM purchase_stream AS p
  JOIN master_product /*+ OPTIONS('slot.name' = 'coffee_sales') */
    FOR SYSTEM_TIME AS OF p.transaction_time AS mp
    ON p.product_id = mp.product_id
)
SELECT
  item                                AS dim_item,
  category                            AS dim_category,
  state                               AS dim_state,
  window_start                        AS purchase_time,
  CAST(COUNT(*)                AS DOUBLE) AS fact_count_transactions,
  CAST(SUM(quantity)           AS DOUBLE) AS fact_sum_quantity,
  CAST(SUM(price)              AS DOUBLE) AS fact_sum_price,
  CAST(SUM(member_discount)    AS DOUBLE) AS fact_sum_member_discount,
  CAST(SUM(supplement_price)   AS DOUBLE) AS fact_sum_supplement_price,
  CAST(SUM(total_purchase)     AS DOUBLE) AS fact_sum_total_purchase,
  CAST(AVG(total_purchase)     AS DOUBLE) AS fact_avg_total_purchase
FROM TABLE(
  TUMBLE(TABLE purchases_enriched, DESCRIPTOR(transaction_time), INTERVAL '30' SECONDS)
)
GROUP BY window_start, window_end, item, category, state
ON CONFLICT DO NOTHING;
