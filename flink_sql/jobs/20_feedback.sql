-- =====================================================================
-- JOB 2 of 2: feedback
--   store.feedback -> WHERE overall_score < 6 -> ML_PREDICT -> feedback_summary
-- One LLM call per bad review. Submit on its own with:  ./submit.sh feedback
-- =====================================================================

SET 'pipeline.name' = 'coffee-2-feedback';

-- Bad reviews, flattened into one prompt string; the model's 3-line answer is split
-- into summary and two improvements.
INSERT INTO feedback_summary_sink
WITH bad_reviews AS (
  SELECT
    review_id,
    customer_id,
    store_state,
    product_name,
    product_category,
    overall_score,
    product_quality,
    service_speed,
    staff_friendliness,
    cleanliness,
    value_for_money,
    visit_type,
    would_return,
    review_text,
    event_ts,
    CONCAT(
      'Product: ',           product_name, ' (', product_category, ')',
      ' | State: ',          store_state,
      ' | Visit type: ',     visit_type,
      ' | Overall: ',        CAST(overall_score      AS STRING),
      ', Product quality: ', CAST(product_quality    AS STRING),
      ', Service speed: ',   CAST(service_speed      AS STRING),
      ', Staff: ',           CAST(staff_friendliness AS STRING),
      ', Cleanliness: ',     CAST(cleanliness        AS STRING),
      ', Value for money: ', CAST(value_for_money    AS STRING),
      ' | Would return: ',   CAST(would_return       AS STRING),
      ' | Review: ',         review_text
    ) AS review_summary
  FROM feedback
  WHERE overall_score < 6
)
SELECT
  review_id,
  customer_id,
  store_state,
  product_name,
  product_category,
  overall_score,
  product_quality,
  service_speed,
  staff_friendliness,
  cleanliness,
  value_for_money,
  visit_type,
  would_return,
  review_text,
  TRIM(REGEXP_EXTRACT(`output`, '^([^\n]+)',           1)) AS summary,
  TRIM(REGEXP_EXTRACT(`output`, 'Improvement 1: (.+)', 1)) AS improvement_1,
  TRIM(REGEXP_EXTRACT(`output`, 'Improvement 2: (.+)', 1)) AS improvement_2,
  CAST(event_ts AS TIMESTAMP(3))                           AS processed_at
FROM ML_PREDICT(
  TABLE bad_reviews,
  MODEL ai_analyze_review,
  DESCRIPTOR(review_summary)
)
ON CONFLICT DO NOTHING;
