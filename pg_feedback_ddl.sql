-- PostgreSQL sink table for the LLM feedback summaries.
-- Appended to the sales_report database created at container init.
--
-- The three text columns are produced by ML_PREDICT inside Flink and parsed out of
-- the model's raw `output` by flink_sql/jobs/20_feedback.sql.

\c sales_report

CREATE TABLE IF NOT EXISTS public.feedback_summary (
  review_id          VARCHAR(50)  PRIMARY KEY,
  customer_id        VARCHAR(50),
  store_state        VARCHAR(5),
  product_name       VARCHAR(100),
  product_category   VARCHAR(100),
  overall_score      INT,
  product_quality    INT,
  service_speed      INT,
  staff_friendliness INT,
  cleanliness        INT,
  value_for_money    INT,
  visit_type         VARCHAR(20),
  would_return       BOOLEAN,
  review_text        TEXT,
  summary            TEXT,
  improvement_1      TEXT,
  improvement_2      TEXT,
  processed_at       TIMESTAMP
);

ALTER TABLE public.feedback_summary OWNER TO root;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO root;
