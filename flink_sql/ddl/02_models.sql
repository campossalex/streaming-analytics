-- =====================================================================
-- The review-analysis model. CREATE MODEL is lazy: a wrong endpoint or key
-- only shows up at the first ML_PREDICT. Needs the flink-model-openai JAR.
-- OpenAI gpt-4o-mini: only the API key comes from .env (OPENAI_API_KEY).
-- =====================================================================

-- Exactly 3 lines, so jobs/20_feedback.sql can parse the answer with REGEXP_EXTRACT.
-- One word per improvement feeds the Grafana word cloud.
CREATE MODEL ai_analyze_review
INPUT  (review_summary STRING)
OUTPUT (`output`       STRING)
WITH (
  'provider'      = 'openai',
  'endpoint'      = 'https://api.openai.com/v1/chat/completions',
  'api-key'       = '${OPENAI_API_KEY}',
  'model'         = 'gpt-4o-mini',
  'system-prompt' = 'You are a coffee shop quality analyst. Given a customer review with scores, respond with exactly 3 lines and no other text. Line 1: one-sentence summary of the main issue. Line 2 starts with "Improvement 1: " followed by a specific actionable improvement for the store. Line 3 starts with "Improvement 2: " followed by a second specific actionable improvement. Be concise, direct and return one single word for each improvement.'
);
