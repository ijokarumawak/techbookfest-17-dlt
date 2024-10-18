-- Databricks notebook source
CREATE OR REFRESH MATERIALIZED VIEW loan_contracts
AS SELECT * FROM read_files("${input_path}/loan_contracts", format => "csv", inferSchema => true);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE credit_card_accounts_raw
AS SELECT * FROM STREAM read_files("${input_path}/credit_card_accounts", format => "csv", inferSchema => true);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE credit_card_accounts;

APPLY CHANGES INTO live.credit_card_accounts
FROM STREAM(live.credit_card_accounts_raw)
KEYS(id)
SEQUENCE BY date
COLUMNS * EXCEPT(_rescued_data);

-- COMMAND ----------

-- 件数か列数が少なすぎてスキーマ推測できないっぽい
CREATE OR REFRESH STREAMING TABLE borrower_events
AS SELECT * FROM STREAM read_files("${input_path}/borrower_events", format => "csv", header => true, schema => "date DATE, id STRING, event STRING");

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW delinquents AS
SELECT id, COUNT(1) as count FROM LIVE.borrower_events
WHERE event = "delinq" AND `date` >= CURRENT_DATE() - INTERVAL 24 MONTH
GROUP BY id

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW bankruptcies AS
SELECT id, COUNT(1) as count FROM LIVE.borrower_events
WHERE event = "bankruptcy"
GROUP BY id

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW loan_features AS
SELECT L.* EXCEPT(_rescued_data),
D.count as delinq_2yrs, B.count as pub_rec, B.count AS pub_rec_bankruptcies,
C.revol_bal, C.revol_util
FROM LIVE.loan_contracts AS L
LEFT OUTER JOIN LIVE.delinquents AS D ON L.id = D.id
LEFT OUTER JOIN LIVE.bankruptcies AS B ON L.id = B.id
LEFT OUTER JOIN LIVE.credit_card_accounts AS C ON L.id = C.id
