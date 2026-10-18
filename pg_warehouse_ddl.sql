-- Product dimension — the warehouse database, read by Flink with postgres-cdc.
-- Runs automatically via docker-entrypoint-initdb.d on first container start.
--
-- A database of its own, apart from sales_report: it stands in for an operational
-- system the pipeline reads from, while sales_report is where the pipeline writes.
--
-- What postgres-cdc needs, all set up here or in docker-compose.yml:
--   wal_level = logical          the postgres command line in docker-compose.yml
--   REPLICA IDENTITY FULL        so an UPDATE carries the whole old row, which Flink
--                                needs to retract the previous version
--   a publication                pgoutput streams only the tables a publication lists;
--                                created here so Flink needs no rights to create one

CREATE DATABASE warehouse;

\c warehouse

CREATE TABLE IF NOT EXISTS master_product (
  product_id        VARCHAR(64) PRIMARY KEY,
  category          VARCHAR(128),
  item              VARCHAR(255),
  size              VARCHAR(32),
  cogs              DECIMAL(10, 2),
  price             DECIMAL(10, 2),
  inventory_level   INT,
  contains_fruit    BOOLEAN,
  contains_veggies  BOOLEAN,
  contains_nuts     BOOLEAN,
  contains_caffeine BOOLEAN
);

ALTER TABLE master_product REPLICA IDENTITY FULL;
ALTER TABLE master_product OWNER TO root;

CREATE PUBLICATION master_product_pub FOR TABLE master_product;

INSERT INTO master_product
  (product_id, category, item, size, cogs, price, inventory_level,
   contains_fruit, contains_veggies, contains_nuts, contains_caffeine)
VALUES
  ('CS01', 'Classic Smoothies',     'Sunrise Sunset',        '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('CS02', 'Classic Smoothies',     'Kiwi Quencher',         '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('CS03', 'Classic Smoothies',     'Paradise Point',        '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('CS04', 'Classic Smoothies',     'Sunny Day',             '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('CS05', 'Classic Smoothies',     'Mango Magic',           '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('CS06', 'Classic Smoothies',     'Blimey Limey',          '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('CS07', 'Classic Smoothies',     'Blueberry Bliss',       '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('CS08', 'Classic Smoothies',     'Rockin'' Raspberry',    '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('CS09', 'Classic Smoothies',     'Strawberry Limeade',    '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('CS10', 'Classic Smoothies',     'Peaches ''n Silk',      '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('CS11', 'Classic Smoothies',     'Jetty Punch',           '24 oz.', 1.50, 4.99, 75, TRUE, FALSE, FALSE, FALSE),
  ('SF01', 'Superfoods Smoothies',  'Island Green',          '24 oz.', 2.10, 5.99, 50, TRUE, TRUE, FALSE, FALSE),
  ('SF02', 'Superfoods Smoothies',  'Totally Green',         '24 oz.', 2.10, 5.99, 50, TRUE, TRUE, FALSE, FALSE),
  ('SF03', 'Superfoods Smoothies',  'Acai Berry Boost',      '24 oz.', 2.10, 5.99, 50, TRUE, FALSE, FALSE, FALSE),
  ('SF04', 'Superfoods Smoothies',  'Pomegranate Plunge',    '24 oz.', 2.10, 5.99, 50, TRUE, FALSE, FALSE, FALSE),
  ('SF05', 'Superfoods Smoothies',  'Caribbean C-Burst',     '24 oz.', 2.10, 5.99, 50, TRUE, FALSE, FALSE, FALSE),
  ('SF06', 'Superfoods Smoothies',  'Get Up and Goji',       '24 oz.', 2.10, 5.99, 50, TRUE, TRUE, FALSE, FALSE),
  ('SF07', 'Superfoods Smoothies',  'Detox Island Green',    '24 oz.', 2.10, 5.99, 50, TRUE, TRUE, FALSE, FALSE),
  ('SC01', 'Supercharged Smoothies','Triple Berry Oat',      '24 oz.', 2.70, 5.99, 35, TRUE, FALSE, FALSE, FALSE),
  ('SC02', 'Supercharged Smoothies','Peanut Paradise',       '24 oz.', 2.70, 5.99, 35, FALSE, FALSE, FALSE, FALSE),
  ('SC03', 'Supercharged Smoothies','Health Nut',            '24 oz.', 2.70, 5.99, 35, FALSE, FALSE, TRUE, FALSE),
  ('SC04', 'Supercharged Smoothies','Lean Machine',          '24 oz.', 2.70, 5.99, 35, FALSE, TRUE, TRUE, TRUE),
  ('SC05', 'Supercharged Smoothies','Muscle Blaster',        '24 oz.', 2.70, 5.99, 35, FALSE, FALSE, TRUE, TRUE),
  ('IS01', 'Indulgent Smoothies',   'Bahama Mama',           '24 oz.', 2.20, 5.49, 60, TRUE, FALSE, FALSE, FALSE),
  ('IS02', 'Indulgent Smoothies',   'Peanut Butter Cup',     '24 oz.', 2.20, 5.49, 60, FALSE, FALSE, TRUE, FALSE),
  ('IS03', 'Indulgent Smoothies',   'Beach Bum',             '24 oz.', 2.20, 5.49, 60, TRUE, TRUE, FALSE, FALSE),
  ('IS04', 'Indulgent Smoothies',   'Mocha Madness',         '24 oz.', 2.20, 5.49, 60, FALSE, FALSE, TRUE, TRUE)
ON CONFLICT (product_id) DO NOTHING;
