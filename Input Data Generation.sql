-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # ローン情報最新化入力データ生成
-- MAGIC
-- MAGIC 元のサンプルデータから、業務システムで生成されそうな項目を検討。
-- MAGIC
-- MAGIC ## 発生しそうなイベント
-- MAGIC - chargeoff
-- MAGIC - delinq (amount)
-- MAGIC - pub_rec
-- MAGIC - bankruptcies
-- MAGIC
-- MAGIC
-- MAGIC ## クレジットカード利用状況
-- MAGIC To calculate open_acc
-- MAGIC - id
-- MAGIC - member_id
-- MAGIC - revol_bal
-- MAGIC - revol_util
-- MAGIC
-- MAGIC ## 初期契約情報
-- MAGIC - id
-- MAGIC - member_id
-- MAGIC - annual_inc
-- MAGIC - zip_code
-- MAGIC - addr_state
-- MAGIC - dti
-- MAGIC - emp_title
-- MAGIC - home_ownership
-- MAGIC - int_rate
-- MAGIC - installment
-- MAGIC - loan_amnt
-- MAGIC - purpose
-- MAGIC - verification (source verified, verified)
-- MAGIC - hist_acc (to simulate total_acc = hist_acc + num_of_accounts)
-- MAGIC - grade
-- MAGIC - sub_grade
-- MAGIC
-- MAGIC ## 集計項目
-- MAGIC - chargeoff_within_12_mths
-- MAGIC - delinq_2yrs
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 「それぞれの loan_status で契約金額の高いN件を引っ張ってくれば面白いデータセットになりそう」という仮説のもとデータを生成してみる。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # データ出力先のカタログ、スキーマ
-- MAGIC dbutils.widgets.text("catalog_name", "")
-- MAGIC dbutils.widgets.text("schema_name", "")

-- COMMAND ----------

USE CATALOG ${catalog_name};
-- Runtime 15.2からはIDENTIFIERとパラメータ参照を利用
-- USE SCHEMA IDENTIFIER(:schema_name);
USE SCHEMA ${schema_name};

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog_name = dbutils.widgets.get("catalog_name")
-- MAGIC schema_name = dbutils.widgets.get("schema_name")

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS loan_inputs;

-- COMMAND ----------

-- 現在進行形のローン (Current)、完済、不良債権のそれぞれでサンプルを抽出
CREATE OR REPLACE TEMP VIEW ranked_loan_samples AS
WITH ranked_loans AS (
  SELECT *,
         ROW_NUMBER() OVER(PARTITION BY loan_status ORDER BY installment DESC) AS rn
  FROM read_files("/databricks-datasets/samples/lending_club/parquet/")
  WHERE loan_status IN ("Current", "Fully Paid", "Default")
  AND addr_state is not null
  AND annual_inc >= 120000
)
SELECT *
FROM ranked_loans
WHERE rn <= 3
ORDER BY loan_status, annual_inc DESC;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # ローン契約申請情報をCSVで出力
-- MAGIC df = spark.sql("""SELECT concat(loan_status, ' ', rn) as id, annual_inc, zip_code, addr_state, dti, emp_title, home_ownership, int_rate, installment, loan_amnt, purpose, (total_acc - open_acc) AS hist_acc, grade, sub_grade, verification_status, loan_status FROM ranked_loan_samples ORDER BY id;""")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite").csv(f"/Volumes/{catalog_name}/{schema_name}/loan_inputs/loan_contracts", header=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # クレジット利用状況をCSVで出力
-- MAGIC df = spark.sql("""SELECT concat(loan_status, ' ', rn) as id, revol_bal, revol_util, open_acc, ADD_MONTHS(CURRENT_DATE(), -3) AS date FROM ranked_loan_samples;""")
-- MAGIC
-- MAGIC display(df)
-- MAGIC df.coalesce(1).write.mode("overwrite").csv(f"/Volumes/{catalog_name}/{schema_name}/loan_inputs/credit_card_accounts", header=True)

-- COMMAND ----------

-- このSQLはどんなデータをイベントとして生成すれば良いかの確認用
SELECT concat(loan_status, ' ', rn) as id, chargeoff_within_12_mths, delinq_2yrs, delinq_amnt, pub_rec, pub_rec_bankruptcies FROM ranked_loan_samples;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import datetime
-- MAGIC import os
-- MAGIC
-- MAGIC def date_before(days):
-- MAGIC   return (datetime.date.today() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
-- MAGIC
-- MAGIC # これを volume に書き出す
-- MAGIC # そしてイベントデータとして追加されていく感じ
-- MAGIC # で、前のセルの出力結果みたいに集計する
-- MAGIC df = pd.DataFrame([
-- MAGIC     [date_before(365), "Current 3", "delinq"],
-- MAGIC     [date_before(600), "Current 2", "bankruptcy"],
-- MAGIC     [date_before(840), "Fully Paid 3", "bankruptcy"],
-- MAGIC ], columns=["date", "id", "event"])
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC directory_path = f"/Volumes/{catalog_name}/{schema_name}/loan_inputs/borrower_events"
-- MAGIC
-- MAGIC if not os.path.exists(directory_path):
-- MAGIC     os.makedirs(directory_path)
-- MAGIC
-- MAGIC df.to_csv(f"{directory_path}/initial.csv", index=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## データ更新取り込みのテスト
-- MAGIC
-- MAGIC 新しい入力ファイルを作成して更新のテストを行う際に利用

-- COMMAND ----------

SELECT id, revol_bal, revol_util, pub_rec_bankruptcies FROM loan_features

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # クレジット情報の更新
-- MAGIC df = pd.DataFrame([
-- MAGIC     # 前の値: Current 2, 13232, 10.5%, 18, 可変日付
-- MAGIC     ["Current 2", 25124, "20.5%", 18, date_before(7)]
-- MAGIC ], columns=["id", "revol_bal", "revol_util", "open_acc", "date"])
-- MAGIC
-- MAGIC display(df)
-- MAGIC df.to_csv(f"/Volumes/{catalog_name}/{schema_name}/loan_inputs/credit_card_accounts/upd_1.csv", index=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 新規イベントの追加
-- MAGIC df = pd.DataFrame([
-- MAGIC     [date_before(7), "Current 2", "bankruptcy"]
-- MAGIC ], columns=["date", "id", "event"])
-- MAGIC
-- MAGIC display(df)
-- MAGIC df.to_csv(f"/Volumes/{catalog_name}/{schema_name}/loan_inputs/borrower_events/new_1.csv", index=False)

-- COMMAND ----------

-- 更新後に再度確認
SELECT id, revol_bal, revol_util, pub_rec_bankruptcies FROM loan_features
