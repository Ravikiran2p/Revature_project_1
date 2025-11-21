import os
import sys
import shutil

# ============================
# ENVIRONMENT SETTINGS (WINDOWS)
# ============================
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["SPARK_HOME"] = r"C:\Users\hp\Desktop\Project\.venv\Lib\site-packages\pyspark"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# ============================
# IMPORTS
# ============================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, sum as _sum,
    round as rnd, trim, sum
)

# ============================
# START SPARK SESSION
# ============================
spark = SparkSession.builder.appName("Financial_Reporting").getOrCreate()

# ============================
# PATH SETUP
# ============================
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
COA_PATH = os.path.join(BASE, "data", "chart_of_accounts.csv")
GL_PATH  = os.path.join(BASE, "data", "general_ledger.csv")
OUTPUT_DIR = os.path.join(BASE, "outputs", "csv")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================
# LOAD CSV FILES
# ============================
coa_df = spark.read.csv(COA_PATH, header=True, inferSchema=True)
gl_df  = spark.read.csv(GL_PATH, header=True, inferSchema=True)

print("COA Sample:")
coa_df.show(5)

print("GL Sample:")
gl_df.show(5)

# ============================
# NULL CHECK FUNCTION
# ============================
def check_nulls(df, name):
    print(f"\nNULL COUNT IN {name}:")
    df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show()

check_nulls(coa_df, "CHART OF ACCOUNTS")
check_nulls(gl_df, "GENERAL LEDGER")

# ============================
# RENAME COLUMNS
# ============================
coa_df = coa_df.withColumnRenamed("Account_ID", "Account_Code") \
               .withColumnRenamed("Account_Name", "Account_Name") \
               .withColumnRenamed("Account_Type", "Account_Class")

gl_df = gl_df.withColumnRenamed("Account_ID", "Account_Code")

# ============================
# DATE PARSING
# ============================
gl_df = gl_df.withColumn(
    "txn_date",
    to_date(col("Transaction_Date"), "yyyy-MM-dd")
)

# ============================
# AMOUNT FIELD
# ============================
gl_df = gl_df.withColumn("Amount_signed", col("Amount").cast("double"))

# ============================
# CLEAN JOIN KEYS
# ============================
coa_df = coa_df.withColumn("Account_Code", trim(col("Account_Code")))
gl_df  = gl_df.withColumn("Account_Code", trim(col("Account_Code")))

# ============================
# JOIN GL WITH COA
# ============================
joined = gl_df.join(coa_df, on="Account_Code", how="left")
joined = joined.fillna({"Account_Class": "Unknown", "Account_Name": "Unknown"})

# ============================
# AGGREGATIONS
# ============================
monthly_tb = joined.groupBy(
    year(col("txn_date")).alias("Year"),
    month(col("txn_date")).alias("Month"),
    "Account_Code", "Account_Name", "Account_Class"
).agg(rnd(_sum("Amount_signed"), 2).alias("Total_Amount"))

pl_summary = joined.groupBy("Account_Class") \
                   .agg(rnd(_sum("Amount_signed"), 2).alias("Total"))

monthly_by_class = joined.groupBy(
    year(col("txn_date")).alias("Year"),
    month(col("txn_date")).alias("Month"),
    "Account_Class"
).agg(rnd(_sum("Amount_signed"), 2).alias("Total"))

# ============================
# SAVE OUTPUT CSV FILES (WINDOWS SAFE)
# ============================
paths = {
    "monthly_tb": monthly_tb,
    "pl_summary": pl_summary,
    "monthly_by_class": monthly_by_class
}

for name, df in paths.items():
    output_file = os.path.join(OUTPUT_DIR, name + ".csv")

    try:
        pdf = df.toPandas()
        pdf.to_csv(output_file, index=False)
        print(f"Saved CSV: {output_file}")
    except Exception as e:
        print(f"Failed to save {name}: {e}")

spark.stop()
print("ETL Completed Successfully")
