# Databricks notebook source
secret_scope = 'testingkv-sit'

td_user = dbutils.secrets.get(secret_scope, "sta-teradata-ibmsit-prod-user")
td_password = dbutils.secrets.get(secret_scope, "sta-teradata-ibmsit-prod-pass")
td_url = dbutils.secrets.get(secret_scope, "sta-teradata-ibmsit-prod-url")

# COMMAND ----------

# Teradata connection options
teradata_options = {
    "driver": "com.teradata.jdbc.TeraDriver",
    "url": td_url,
    "user": td_user,
    "password": td_password,
    "client_charset": "ISO8859_1"
}

# COMMAND ----------

# Gets the table row count
query = """SELECT CAST(COUNT(*) AS BIGINT) AS ROW_COUNT FROM PRIMO_DB.ABK"""
df = spark.read.format("jdbc").options(**teradata_options).option("query", query).load()
df.display()

# COMMAND ----------

# Get all indexes from the table
query = """
select * from dbc.IndicesV
where upper(trim(DatabaseName)) = 'PRIMO_DB'
and upper(trim(TableName)) = 'ABK'
"""
df = spark.read.format("jdbc").options(**teradata_options).option("query", query).load()
df.display()

# COMMAND ----------

# Gets all columns from the table
query = """
select * from dbc.columnsV
where upper(trim(DatabaseName)) = 'PRIMO_DB'
and upper(trim(TableName)) = 'ABK'
"""
df = spark.read.format("jdbc").options(**teradata_options).option("query", query).load()
df.display()

# COMMAND ----------

### Verify if combination of keys returns only 1 result
query = """
select count(*) as TOTAL,LEG_DEP_DATE,ALLOC_FOR_CABIN_Y,ORIG,FLT_DEP_DATE,DEST,BKG_FOR_O,CARRIER_CODE,TOT_ALLOC,LEG_SEG_INDIC,DAYS_PRIOR_OUT,ALLOC_FOR_D,AVAIL_FOR_C,AVAIL_FOR_A,AVAIL_FOR_B,AVAIL_FOR_E,AVAIL_FOR_F,AVAIL_FOR_G,AVAIL_FOR_H,AVAIL_FOR_L,AVAIL_FOR_M,AVAIL_FOR_O,AVAIL_FOR_P,AVAIL_FOR_S
from PRIMO_DB.ABK
group by LEG_DEP_DATE,ALLOC_FOR_CABIN_Y,ORIG,FLT_DEP_DATE,DEST,BKG_FOR_O,CARRIER_CODE,TOT_ALLOC,LEG_SEG_INDIC,DAYS_PRIOR_OUT,ALLOC_FOR_D,AVAIL_FOR_C,AVAIL_FOR_A,AVAIL_FOR_B,AVAIL_FOR_E,AVAIL_FOR_F,AVAIL_FOR_G,AVAIL_FOR_H,AVAIL_FOR_L,AVAIL_FOR_M,AVAIL_FOR_O,AVAIL_FOR_P,AVAIL_FOR_S
having TOTAL > 1
"""
df = spark.read.format("jdbc").options(**teradata_options).option("query", query).load()
df.display()

# COMMAND ----------

# Run specific query filtering by values to see which columns is different
query = """
SELECT * FROM PEDW_STG.CREW_HTL_INFO
WHERE EXTC_CRT_DTE = '2021-12-03'
AND HTL_ID = 16001
AND POSTDATE = '2022-01-14'
AND CDE_AIRCAN = 'D49'
AND DW_SRC_SYS_ID = 'IFS_RG'
AND CNTC_BEG = '2020-01-01'
AND CNTC_END = '2022-03-10'
"""
df = spark.read.format("jdbc").options(**teradata_options).option("query", query).load()
df.display()

# COMMAND ----------

