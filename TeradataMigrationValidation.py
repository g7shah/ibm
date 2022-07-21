# Databricks notebook source
# MAGIC %md
# MAGIC ## Dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# DBTITLE 1,------------------------------------------------------Import all dependencies---------------------------------------------
import datacompy

from datetime import datetime, timezone
from enum import Enum
from pyspark.sql.types import *
from pyspark.sql.functions import md5, concat_ws, trim, col
# from pyspark.sql.types import _parse_datatype_string

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

# DBTITLE 1,------------------------------------------------------Parameters for execution---------------------------------------------
dbutils.widgets.dropdown(name="lob", defaultValue="Flight Ops: FOP", choices=["Cargo: CGO","Maintenance: MNT","Flight Ops: FOP","Operations: OPS","Commercials: COM","Customer Relations: CRM","Rev Acct: RAC","Tax: TAX","Finance: FIN","Mkt Ops: MOP","Security: SCR","Loyalty: LTY"], label="LOB")
dbutils.widgets.text(name="configFileName", defaultValue="hist_config.csv", label="Config File Name")
dbutils.widgets.multiselect(name="validationsToRun", defaultValue="All", choices=["All", "ETL execution (only Historical)", "Table Row Count", "Table Data Type", "Aggregation", "Content Comparison", "File Comparison", "Business Queries", "Future Dates Conversion"], label="Validations to Run")

dbutils.widgets.dropdown(name="environment", defaultValue="SIT", choices=["DEV", "SIT", "UAT"], label="Enviroment")
dbutils.widgets.dropdown(name="exportResults", defaultValue="No", choices=["Yes", "No"], label="Export results to Storage Account?")
dbutils.widgets.dropdown(name="showReportSamples", defaultValue="Yes", choices=["Yes", "No"], label="Show report samples?")

config_file_name = dbutils.widgets.get("configFileName").strip()

# Get all validations that need to be executed and remove white spaces
if(len(dbutils.widgets.get("validationsToRun").strip()) > 0):
  validations = dbutils.widgets.get("validationsToRun").strip()
  validations = [val.strip() for val in validations.split(',')]
else:
  validations = None
    
environment = dbutils.widgets.get("environment").lower()

export_results = False if(dbutils.widgets.get("exportResults") == 'No') else True
show_samples = False if(dbutils.widgets.get("showReportSamples") == 'No') else True

# Gets the LOB name and extract last 3 characters for building the filepath for source and result files
lob = dbutils.widgets.get("lob")
lob_code = (lob[-3:]).lower()

execution_start_time = datetime.now(timezone.utc)
execution_start_time_formatted = execution_start_time.strftime("%a_%b_%d_%H_%M_%S_%Z_%Y")

# COMMAND ----------

# DBTITLE 1,------------------------------------------------------Verify all mandatory parameters---------------------------------------------
def verify_mandatory_parameters():  
  if((not config_file_name) or (config_file_name.isspace())):
    return False
  elif(not validations):
    return False
  else:
#     print("LOB = ", lob)
#     print("CONFIG FILE NAME = ", config_file_name)
#     print("VALIDATIONS TO EXECUTE = ", validations)
    return True
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storage Account

# COMMAND ----------

# TODO: update the secret scope name for UAT to be testingkv-uat
secret_scope = ("testingkv-" + environment)
container = "ibm" + environment

# Configuration for Storage account
storage_container = "ibm" + environment
storage_account = "adlseus2tlsdp" + environment + "02"
# storage_account = "adlseus2tlsdp" + environment + "01"
# storage_account_access_key = dbutils.secrets.get(secret_scope,"sta-adls2-ibmuat02-sas-token")
storage_account_access_key = dbutils.secrets.get(secret_scope, f"sta-adls2-{storage_container}02-access-key")
storage_connection_str = "wasbs://" + storage_container + "@" + storage_account + ".blob.core.windows.net/" + "TestFramework"
storage_conf_key = "fs.azure.account.key." + storage_account + ".blob.core.windows.net"
# storage_conf_key = "fs.azure.sas." + storage_container + "." + storage_account + ".blob.core.windows.net"

# Mount configuration
dbfs_mount_point = "/mnt/test-framework/" + environment
dbfs_input_folder = dbfs_mount_point + "/wave1/" + lob_code + "/config/"
dbfs_output_folder = dbfs_mount_point + "/wave1/" + lob_code + "/output/"
storage_account_config_folder = "/TestFramework/wave1/" + lob_code + "/config/"
storage_account_output_folder = "/TestFramework/wave1/" + lob_code + "/output/"



# COMMAND ----------

# Mount the Storage account to use DBFS only if it's not already mounted
if([item.mountPoint for item in dbutils.fs.mounts()].count(dbfs_mount_point) == 0):
  dbutils.fs.mount(
    source = storage_connection_str,
    mount_point = dbfs_mount_point,
    extra_configs = {storage_conf_key:storage_account_access_key})
  
# Check if input and output directories exists
# display(dbutils.fs.mounts())
# display(dbutils.fs.ls(dbfs_input_folder))
# display(dbutils.fs.ls(dbfs_output_folder))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test config input

# COMMAND ----------

# DBTITLE 1,Used only for testing without reading CSV from Storage Account
# from pyspark.sql import *

# config_df_schema = 'ACTIVE string, LOB string, TYPE string, WF_NAME string, SOURCE_SCHEMA_NAME string, SOURCE_TABLE_NAME string, TARGET_SCHEMA_NAME string, TARGET_TABLE_NAME string, SOURCE_COLUMNS string, TARGET_COLUMNS string, UNIQUE_KEYS string, SKIP_COLUMNS string, DELTA_COLUMNS string, SOURCE_FILTER string, TARGET_FILTER string, AGGREGATE_COLUMNS string, AGGREGATE_FUNCTIONS string'

# config = [
#   ('N', 'Flight Ops', 'table', '', 'PEDW_DB', 'LEG_FLOWN', 'PEDW_DB', 'LEG_FLOWN', '*', '*', 'LEG_ID', '', '', "WHERE POSTDATE='2022-05-11'", "WHERE POSTDATE='2022-05-09'", 'SCHED_GROUND_TME,ACTUAL_GROUND_TME,LEG_DEP_EQUIP_DELAY_MINS,LEG_DEP_EQUIP_DELAY_MINS,CARRIER_CDE', 'SUM,AVG,MAX,MIN,COUNT')
# ]

# config_df = spark.createDataFrame(config, schema=config_df_schema)
# config_df.display()

# # Filter to have only the "ACTIVE" configurations to run validation
# active_config_df = config_df[config_df["ACTIVE"] == 'Y']
# active_config_df.display()

# COMMAND ----------

def retrieve_dataframe_csv(file_path, delimiter, header, schema):
  # Read CSV file and update None/Null values with ""
  return spark.read.format("csv").option("delimiter", delimiter).option("header", header).schema(schema).load(file_path).na.fill(value="")

# COMMAND ----------

# DBTITLE 1,Read configuration file from Storage Account
config_df_schema = 'ACTIVE string, LOB string, TYPE string, WF_NAME string, SOURCE_SCHEMA_NAME string, SOURCE_TABLE_NAME string, TARGET_SCHEMA_NAME string, TARGET_TABLE_NAME string, SOURCE_COLUMNS string, TARGET_COLUMNS string, UNIQUE_KEYS string, SKIP_COLUMNS string, DELTA_COLUMNS string, SOURCE_FILTER string, TARGET_FILTER string, AGGREGATE_COLUMNS string, AGGREGATE_FUNCTIONS string'

delimiter = ","
dbfs_file_path = dbfs_input_folder + config_file_name
config_df = retrieve_dataframe_csv(dbfs_file_path, delimiter, True, config_df_schema)
# config_df.display()

active_config_df = config_df[config_df["ACTIVE"] == 'Y']
active_config_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load config file variables

# COMMAND ----------

# HTML report list of strings
html_report_list = []

def load_config_row(config_row):  
  # General variables
  global validation_object_type
  global wf_name
  global unique_keys
  global skip_columns
  global delta_columns
  global aggregate_columns
  global aggregate_functions
   # Teradata variables
  global source_schema_name
  global source_table_name
  global source_columns
  global source_filter
  global td_full_table_name
  # Snowflake variables
  global target_schema_name
  global target_table_name
  global target_columns
  global target_filter
  global sf_full_table_name
  
  validation_object_type = config_row["TYPE"].strip().upper()
  
  wf_name = config_row["WF_NAME"].strip()
  
  # Load unique keys and remove white spaces their names
  if(len(config_row["UNIQUE_KEYS"].strip()) > 0):
    keys = config_row["UNIQUE_KEYS"].strip()
    unique_keys = [col.strip() for col in keys.split(',')]
#     unique_keys = ','.join(unique_keys)
  else:
    unique_keys = None 
  
  skip_columns = config_row["SKIP_COLUMNS"].strip().split(",") if (len(config_row["SKIP_COLUMNS"].strip()) > 0) else None
  delta_columns = config_row["DELTA_COLUMNS"].strip().split(",") if (len(config_row["DELTA_COLUMNS"].strip()) > 0) else None
  aggregate_columns = config_row["AGGREGATE_COLUMNS"].strip().split(",") if (len(config_row["AGGREGATE_COLUMNS"].strip()) > 0) else None
  aggregate_functions = config_row["AGGREGATE_FUNCTIONS"].strip().split(",") if (len(config_row["AGGREGATE_FUNCTIONS"].strip()) > 0) else None
  
  print("============== General variables ==============")
  print(f"type = {validation_object_type}")
  print(f"keys = {unique_keys}")
  print(f"skip = {skip_columns}")
  print(f"agg cols = {aggregate_columns}")
  print(f"agg func = {aggregate_functions}")
  
  source_schema_name = config_row["SOURCE_SCHEMA_NAME"].strip()
  source_table_name = config_row["SOURCE_TABLE_NAME"].strip()
  source_columns = config_row["SOURCE_COLUMNS"].strip()
  source_filter = config_row["SOURCE_FILTER"].strip()
  td_full_table_name = f"{source_schema_name}.{source_table_name}"
  print(f"Teradata variables: {source_schema_name} | {source_table_name} | {source_columns} | {source_filter} | {td_full_table_name}")
  
  target_schema_name = config_row["TARGET_SCHEMA_NAME"].strip()
  target_table_name = config_row["TARGET_TABLE_NAME"].strip()
  target_columns = config_row["TARGET_COLUMNS"].strip()
  target_filter = config_row["TARGET_FILTER"].strip()
  sf_full_table_name = f"{target_schema_name}.{target_table_name}"
  print(f"Snowflake variables: {target_schema_name} | {target_table_name} | {target_columns} | {target_filter} | {sf_full_table_name}")
  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Snowflake and Teradata configuration

# COMMAND ----------

# DBTITLE 0,------------------------------------------------------Snowflake and Teradata credentials---------------------------------------------
if(environment == 'dev'):
  dbutils.notebook.exit(f"Missing configuration on Key Vault for this environment: {environment}")
elif(environment == 'uat'):
  # User and password
  sf_user = dbutils.secrets.get(secret_scope, f"sta-snowflake-ibmuat-common-user")
  sf_password = dbutils.secrets.get(secret_scope, f"sta-snowflake-ibmuat-common-password")

  # Snowflake connection
  sf_url = dbutils.secrets.get(secret_scope, f"sta-snowflake-ibmuat-cargo-url")
  sf_database_name = dbutils.secrets.get(secret_scope, f"sta-snowflake-ibmuat-cargo-db")
  sf_warehouse_name = dbutils.secrets.get(secret_scope, f"sta-snowflake-ibmuat-cargo-wh")
  
  # Teradata user, password and connection
  td_user = dbutils.secrets.get(secret_scope, f"sta-teradata-ibmuat-user")
  td_password = dbutils.secrets.get(secret_scope, f"sta-teradata-ibmuat-pass")
  td_url = dbutils.secrets.get(secret_scope, f"sta-teradata-ibmuat-url")
elif(environment == 'sit'):
  # User and password
  sf_user = dbutils.secrets.get(secret_scope, f"sta-snowflake-ibmsit-common-user")
  sf_password = dbutils.secrets.get(secret_scope, f"sta-snowflake-ibmsit-common-password")
  # Snowflake connection
  sf_url = dbutils.secrets.get(secret_scope, f"sta-snowflake-ibmsit-cargo-url")
  sf_database_name = dbutils.secrets.get(secret_scope, f"sta-snowflake-ibmsit-cargo-db")
  sf_warehouse_name = dbutils.secrets.get(secret_scope, f"sta-snowflake-ibmsit-cargo-wh")

  # Teradata user, password and connection
  td_user = dbutils.secrets.get(secret_scope, f"sta-teradata-ibmsit-prod-user")
  td_password = dbutils.secrets.get(secret_scope, f"sta-teradata-ibmsit-prod-pass")
  td_url = dbutils.secrets.get(secret_scope, f"sta-teradata-ibmsit-prod-url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read data from Snowflake

# COMMAND ----------

# DBTITLE 0,------------------------------------------------------Connect Databricks with Snowflake and query data------------------------------------------------------
def read_data_from_snowflake(query, custom_schema=None):
  # snowflake connection options
  snowflake_options = {
    "sfUrl": sf_url,
    "sfUser": sf_user,
    "sfPassword": sf_password,
    "sfDatabase": sf_database_name,
    "sfSchema": target_schema_name,
    "sfWarehouse": sf_warehouse_name
  }
  
  
  if(custom_schema is not None):
    print("Read Snowflake data with custom schema")
    print(custom_schema)
    return spark.read.format("snowflake").options(**snowflake_options).option("query", query).load(schema=custom_schema)
#     return spark.read.format("snowflake").options(**snowflake_options).option("query", query).option("customSchema", custom_schema).load()
  else:
    print("Read Snowflake data inferring table schema")
    return spark.read.format("snowflake").options(**snowflake_options).option("query", query).load()

# COMMAND ----------

# DBTITLE 0,------------------------------------------------------Read table from Snowflake------------------------------------------------------
def load_table_from_snowflake(custom_schema=None):
  query = f"SELECT {target_columns} FROM {sf_full_table_name} {target_filter}"
  print(f"Snowflake: {query}")
  df = read_data_from_snowflake(query, custom_schema)
  
#   df.display()
#   df.count()
  
  return df, query

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read data from Teradata

# COMMAND ----------

# DBTITLE 0,------------------------------------------------------Connecting Databricks with Teradata and query data------------------------------------------------------
def read_data_from_teradata(query, custom_schema=None):
  # Teradata connection options
  teradata_options = {
    "driver": "com.teradata.jdbc.TeraDriver",
    "url": td_url,
    "user": td_user,
    "password": td_password,
    # sets client charset to ISO 8859-1 due to mismatches with returned texts from Snowflake
    "client_charset": "ISO8859_1"
    #"charset": "utf8"
  #   "dbtable": td_full_table_name
  }
  
  if(custom_schema is not None):
    print("Read Teradata data with custom schema")
    print(custom_schema)
    return spark.read.format("jdbc").options(**teradata_options).option("query", query).option("customSchema", custom_schema).load()
  else:
    print("Read Teradata data inferring table schema")
    return spark.read.format("jdbc").options(**teradata_options).option("query", query).load()



# COMMAND ----------

# DBTITLE 0,------------------------------------------------------Read table from Teradata------------------------------------------------------
def load_table_from_teradata(custom_schema=None):
  query = f"SELECT {source_columns} FROM {td_full_table_name} {source_filter}"
  print(f"Teradata: {query}")
  df = read_data_from_teradata(query, custom_schema)
#   df.display()
#   df.count()
  
  return df, query

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data from CSV

# COMMAND ----------

def read_data_from_csv(file_location):
  # CSV options
  infer_schema = "true"
  first_row_is_header = "true"
  delimiter = ","

  df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
  
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility functions

# COMMAND ----------

class MatchType(Enum):
  MISMATCH = 0
  MATCH = 1
  UNKNOWN = 2
    
class CompressionType(Enum):
  NONE = '.csv'
  GZIP = '.csv.gz'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse types from Snowflake

# COMMAND ----------

def parse_types_from_snowflake_to_spark(sf_types_df, to_ddl=False):
  print("====== PARSING SNOWFLAKE TYPES TO SPARK TYPES ======")
  spark_schema = StructType()
  spark_ddl = list()
  
  iter = sf_types_df.toLocalIterator()  
  for row in iter:
    column_name = "`" + row['COLUMN_NAME'] + "`"
    data_type = row['DATA_TYPE']
    numeric_precision = row['NUMERIC_PRECISION']
    numeric_scale = row['NUMERIC_SCALE']
    
    if(data_type in ['NUMBER', 'DECIMAL', 'BIGINT', 'INTEGER']):
      spark_schema.add(column_name, DecimalType(numeric_precision, numeric_scale))
      spark_ddl.append(f"{column_name} decimal({numeric_precision},{numeric_scale})")
    elif(data_type in ['TIME', 'TEXT', 'CHAR', 'CLOB', 'OBJECT', 'ARRAY', 'VARIANT']):
      spark_schema.add(column_name, StringType())
      spark_ddl.append(f"{column_name} string")
    elif(data_type in ['TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'TIMESTAMP_LTZ']):
      spark_schema.add(column_name, TimestampType())
      spark_ddl.append(f"{column_name} timestamp")
    elif(data_type in ['FLOAT', 'DOUBLE']):
      spark_schema.add(column_name, DoubleType())
      spark_ddl.append(f"{column_name} double")
    elif(data_type == 'DATE'):
      spark_schema.add(column_name, DateType())
      spark_ddl.append(f"{column_name} date")
    elif(data_type == 'BOOLEAN'):
      spark_schema.add(column_name, BooleanType())
      spark_ddl.append(f"{column_name} boolean")
  
  if(to_ddl):
    return ", ".join(spark_ddl)
  else: 
    return spark_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save CSV file

# COMMAND ----------

def get_report_file_name(validation_type, table_name, output_folder, file_extension):  
  # Using the same timestamp of the folder created to store the details of comparison  
  time = execution_start_time.strftime("%Y%m%d_%H_%M_%S_%Z")
  file_name = f"TM_{validation_type}_{table_name}_{time}{file_extension}"
  data_location = f"/dbfs{output_folder}{execution_start_time_formatted}/{table_name}/"
  directory_location = f"dbfs:{output_folder}{execution_start_time_formatted}/{table_name}/"
  print(data_location)
  return data_location, directory_location, file_name

def get_csv_file_path(validation_type, table_name, output_folder):
  time = datetime.now(timezone.utc).strftime("%Y%m%d_%H_%M_%S_%Z")
  file_name = f"TM_{validation_type}_{table_name}_{time}"
  data_location = f"{output_folder}{execution_start_time_formatted}/{table_name}/"
  tmp_location = f"/tmp/testing/{execution_start_time_formatted}/"
  return data_location, tmp_location, file_name

def save_csv_from_dataframe(spark_df, validation_type, table_name, output_folder, compression=CompressionType.NONE):
  data_location, tmp_location, file_name = get_csv_file_path(validation_type, table_name, output_folder)
  print(data_location, tmp_location, file_name)
  file_path = data_location + file_name
  
  csv_options = {
    "header": True,
    "compression": compression.name.lower(),
    "quoteAll": True,
    "encoding": "UTF-8"
  }
  
  spark_df.coalesce(1).write.format("csv").options(**csv_options).mode("overwrite").save(tmp_location + file_name)
  
  files = dbutils.fs.ls(tmp_location + file_name)
  print(files)
  csv_file_path = [x.path for x in files if x.path.endswith(compression.value)][0]
  dbutils.fs.mv(csv_file_path, file_path + compression.value)
  dbutils.fs.rm(tmp_location, recurse = True)
  
  # Save the CSV file using Pandas to_csv() method, which has limitations of memory usage
#   spark_df.toPandas().to_csv(file_path)
  
def clean_tmp_folder():
  for i in dbutils.fs.ls("/tmp/testing"):
    dbutils.fs.rm(i.path, True)
    
def clean_output_folder():
  for i in dbutils.fs.ls(dbfs_output_folder):
    dbutils.fs.rm(i.path, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Hash Code

# COMMAND ----------

def generate_hashed_unique_key(table_df, skip_columns_list):
  # Remove skipped columns from the generation of hash code
  columns_list = table_df.columns
  if(skip_columns_list is not None):
    columns_list = [i for i in columns_list if i not in skip_columns_list]
  
  # Returns a new Dataframe with the MD5 hash column
  return table_df.withColumn('HASHED_UNIQUE_KEY', md5(concat_ws('||', *columns_list)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export dataframe to HTML

# COMMAND ----------

# Converts a Spark Dataframe to Pandas Dataframe and generate table sample in HTML
def dataframe_to_html(df, show_samples, max_rows=10):
  if(show_samples):
    # Create a new dataframe with a sample of the rows before converting to Pandas, due to memory restrictions
    return spark.createDataFrame(df.head(max_rows), schema=df.schema).toPandas().to_html(justify='left', max_rows=max_rows)
  else:
    return ""

# COMMAND ----------

# Generate table sample in HTML base on comparison result
def display_html_report_sample(comp_result, matched_all, has_mismatch, has_only_source, has_only_target):
  if(matched_all):
    html_report_list.append(f"""
      <div>
        <h3>Matched rows ({comp_result.rows_both_all.count()})</h3>        
      </div>
      <div>
        {dataframe_to_html(comp_result.rows_both_all, show_samples)}
      </div>
    """)
  else:
    if(has_mismatch):
      html_report_list.append(f"""
      <div>
        <h3>Mismatched rows ({comp_result.rows_both_mismatch.count()})</h3>        
      </div>
      <div>
        {dataframe_to_html(comp_result.rows_both_mismatch, show_samples)}
      </div>
      """)
    if(has_only_source):
      html_report_list.append(f"""
      <div>
        <h3>Rows only in Teradata ({comp_result.rows_only_base.count()})</h3>        
      </div>
      <div>
        {dataframe_to_html(comp_result.rows_only_base, show_samples)}
      </div>
      """)
    if(has_only_target):
      html_report_list.append(f"""
      <div>
        <h3>Rows only in Snowflake ({comp_result.rows_only_compare.count()})</h3>        
      </div>
      <div>
        {dataframe_to_html(comp_result.rows_only_compare, show_samples)}
      </div>
      """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate summary report

# COMMAND ----------

# Generate summary table for all validations executed for each table present on input config file
def generate_summary_report(report_list):  
  schema = StructType([ \
    StructField("STATUS",StringType(),True), \
    StructField("SOURCE_TABLE",StringType(),True), \
    StructField("TARGET_TABLE",StringType(),True), \
    StructField("ROW_COUNT_STATUS", StringType(), True), \
    StructField("DATA_TYPE_STATUS", StringType(), True), \
    StructField("DATA_CONTENT_STATUS", StringType(), True), \
    StructField("AGG_STATUS", StringType(), True), \
    StructField("SOURCE_ROW_COUNT", StringType(), True), \
    StructField("TARGET_ROW_COUNT", StringType(), True), \
    StructField("SOURCE_FILTER", StringType(), True), \
    StructField("TARGET_FILTER", StringType(), True), \
    StructField("VALIDATION_TIME", StringType(), True) \
  ])
  
  summary_table_html = ""
  if(report_list):
    print(report_list)
    df = spark.createDataFrame(report_list, schema=schema)
    summary_table_html = dataframe_to_html(df, show_samples)
    
    if(export_results):      
      # Saves summary report CSV file
      save_csv_from_dataframe(df, "Summary", "Report", dbfs_output_folder)
      
      # Copies the config file used to the same directory
      report_location = f"{dbfs_output_folder}{execution_start_time_formatted}/Report/{config_file_name}"
      dbutils.fs.cp(dbfs_file_path, report_location)
      
    
  report = f"""
  <div>
    <h2>Summary</h2>
    {summary_table_html}
  </div>
  """
  
  return report
  
  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate report header

# COMMAND ----------

def generate_report_header():
  report_header = f"""
    <div>
      <p><b>Execution date: </b>{execution_start_time.strftime("%b %d %Y %H:%M:%S %Z")}</p>
      <p><b>Environment: </b>{environment.upper()}</p>
      <p><b>LOB: </b>{lob}</p>
      <p><b>Configuration file: </b><em>/{storage_container}{storage_account_config_folder}{config_file_name}</em></p>
      <p><b>Output folder: </b><em>/{storage_container}{storage_account_output_folder}</em></p>
      <p><b>Validations executed: </b>{validations}</p>
    </div>
    <div>
      </br>
      <h2>Input</h2>
      {dataframe_to_html(active_config_df, True)}
    </div>
    """
  
  return report_header

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trim all columns from Dataframe

# COMMAND ----------

def trim_all_string_columns(df):
  trimColumns = list(filter(lambda x: isinstance(x.dataType, StringType), df.schema.fields))
  print("trimmed columns = ", trimColumns)
  for x in trimColumns:
    df = df.withColumn(x.name,trim(col(x.name)))
    
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change column names to UPPER CASE

# COMMAND ----------

def rename_column_names_to_uppercase(df):  
  for col in df.columns:
    df = df.withColumnRenamed(col, col.upper())
    
#   df.printSchema()
  return df

# COMMAND ----------

# DBTITLE 1,-------------------------------------------Testing connection port from Databricks to Teradata-----------------------------------------------
#%sh nc -vz "10.43.13.221" '1025'

# COMMAND ----------

# MAGIC %md
# MAGIC # Validations

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1: Validate Execution logs of the ETL process

# COMMAND ----------

# DBTITLE 0,1: Validate Execution logs of the ETL process
def validate_etl_execution_snowflake(summary, job_name=None):
#     query = f"""SELECT source_table_name
#           , start_time
#           , load_count
#           , load_type
#           , extract_query
#         FROM job_management.data_audit_table
#         WHERE job_name IN (SELECT job_name FROM job_management.data_migration_inventory WHERE wf_name = '{job_name}')
#           AND STATUS='SUCCESS'"""
    
#     job_execution_df = read_data_from_snowflake(query)
#     # TODO: assert that execution was completed successfully, maybe verifying if the date it was run matches what we expect
#     job_execution_df.display()
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2: Validate the Source and Target Tables counts

# COMMAND ----------

# Generate summary report file and save
def generate_table_row_count_report(source_count, target_count, matched, source_query, target_query):
  if(export_results):
    data_location, directory_location, file_name = get_report_file_name("RowCountSummary", sf_full_table_name, dbfs_output_folder, ".txt")
    
    dbutils.fs.mkdirs(directory_location)
    
    match_str = MatchType(matched).name
    with open(data_location + file_name, 'w+', encoding='utf-8') as report_file:
      report_file.write("****** VALIDATION OF SOURCE AND TARGET TABLE ROW COUNT ******\n\n")
      report_file.write(f"Executed on: {execution_start_time_formatted}\n")
      report_file.write(f"Table: {sf_full_table_name}\n")
      report_file.write(f"Result = {match_str}\n\n")
      report_file.write(f"TERADATA TABLE ROW COUNT = {source_count}\n\n")
      report_file.write(f"SNOWFLAKE TABLE ROW COUNT = {target_count}\n\n")
      report_file.write(f"DIFF (TERADATA - SNOWFLAKE) = {source_count - target_count}\n\n")
      report_file.write(f"TERADATA SQL query:\n{source_query}\n\n")
      report_file.write(f"SNOWFLAKE SQL query:\n{target_query}\n")
  
  
def display_table_row_count_report(summary, source_count, target_count, matched):
  match_str = MatchType(matched).name
  
  summary['ROW_COUNT_STATUS'] = match_str
  summary['SOURCE_ROW_COUNT'] = str(source_count)
  summary['TARGET_ROW_COUNT'] = str(target_count)
  
  html_report_list.append(f"""
  <div>
    </br>
    <h1>Validation of Source and Target Table row count</h1>
    <p><b>Table: </b>{sf_full_table_name}</p>
    <p><b>Result: </b><mark>{match_str}</mark></p>
    <p><b>TERADATA TABLE ROW COUNT = </b>{source_count}</p>
    <p><b>SNOWFLAKE TABLE ROW COUNT = </b>{target_count}</p>
    <p><b>DIFF (TERADATA - SNOWFLAKE) = </b>{source_count - target_count}</p>
  </div>
    """)

# COMMAND ----------

def validate_table_row_count(summary):
  print("======= Starting TABLE ROW COUNT validation =======")
  td_query = f"SELECT CAST(COUNT(*) AS BIGINT) AS ROW_COUNT FROM {td_full_table_name} {source_filter}"
  sf_query = f"SELECT CAST(COUNT(*) AS BIGINT) AS ROW_COUNT FROM {sf_full_table_name} {target_filter}"

  td_count_df = read_data_from_teradata(td_query, "ROW_COUNT bigint")
  sf_count_df = read_data_from_snowflake(sf_query, "ROW_COUNT bigint")
  
  td_count = td_count_df.select("ROW_COUNT").collect()[0][0]
  print(f"Teradata row count = {td_count}")
  sf_count = sf_count_df.select("ROW_COUNT").collect()[0][0]
  print(f"Snowflake row count = {sf_count}")
  
  matched = (td_count == sf_count)
  
  generate_table_row_count_report(td_count, sf_count, matched, td_query, sf_query)
  display_table_row_count_report(summary, td_count, sf_count, matched)  
  
  print("======= TABLE ROW COUNT validation complete =======")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Table Data Type and View Data Type Conversion between Source and Target

# COMMAND ----------

# Generate summary report file and save
def generate_table_data_type_report(result, source_query, target_query):
  if(export_results):
    # TODO: export file with the columns data types
    has_mismatch = result.rows_both_mismatch.count() > 0
    has_only_source = result.rows_only_base.count() > 0
    has_only_target = result.rows_only_compare.count() > 0
    matched_all = not (has_mismatch or has_only_source or has_only_target)
    
    match_str = MatchType(matched_all).name

    if(has_mismatch):
      save_csv_from_dataframe(result.rows_both_mismatch, "DataTypeMismatch", sf_full_table_name, dbfs_output_folder, CompressionType.GZIP)
    if(has_only_source):
      save_csv_from_dataframe(result.rows_only_base, "DataTypeOnlyTD", td_full_table_name, dbfs_output_folder, CompressionType.GZIP)
    if(has_only_target):
      save_csv_from_dataframe(result.rows_only_compare, "DataTypeOnlySF", sf_full_table_name, dbfs_output_folder, CompressionType.GZIP)

    data_location, directory_location, file_name = get_report_file_name("DataTypeSummary", sf_full_table_name, dbfs_output_folder, ".txt")
    
    dbutils.fs.mkdirs(directory_location)
    
    with open(data_location + file_name, 'w+', encoding='utf-8') as report_file:
      report_file.write("****** VALIDATION OF SOURCE AND TARGET TABLE DATA TYPE ******\n\n")
      report_file.write(f"Executed on: {execution_start_time_formatted}\n")
      report_file.write(f"Table: {sf_full_table_name}\n")
      report_file.write(f"Result = {match_str}\n\n")
      result.report(file=report_file)
      report_file.write("\n************\n\n")
      report_file.write(f"TERADATA SQL query:\n{source_query}\n\n")
      report_file.write(f"SNOWFLAKE SQL query:\n{target_query}\n")
    
  else:
    result.report()
  
def display_table_data_type_report(summary, result):
  has_mismatch = result.rows_both_mismatch.count() > 0
  has_only_source = result.rows_only_base.count() > 0
  has_only_target = result.rows_only_compare.count() > 0
  matched_all = not (has_mismatch or has_only_source or has_only_target)

  match_str = MatchType(matched_all).name
  
  summary['DATA_TYPE_STATUS'] = match_str
  
  html_report_list.append(f"""
 <div>
   </br>
   <h1>Validation of Source and Target Table Data Type</h1>
   <p><b>Table: </b>{sf_full_table_name}</p>
   <p><b>Result: </b><mark>{match_str}</mark></p>
   <p><b>Columns compared: </b>{result.columns_compared}</p>
   <p><b>Common row count: </b>{result.common_row_count}</p>
</div>
  """)
  display_html_report_sample(result, matched_all, has_mismatch, has_only_source, has_only_target)
  

# COMMAND ----------

# DBTITLE 0,3. Validate Table Data Type and View Data Type Conversion between Source and Target

def get_original_snowflake_schema(db_name, schema_name, table_name, columns):
  schema_str = "SCHEMA_NAME string, TABLE_NAME string, COLUMN_NAME string, COLUMN_POSITION int, DATA_TYPE string, CHARACTER_MAX_LENGTH int, NUMERIC_PRECISION int, NUMERIC_SCALE int"
  
  # Get the list of columns and build filter to be used in SQL query
  if(columns == "*"):
    filter_cols = ""
  else:
    cols = ["'" + col.strip() + "'" for col in columns.split(',')]
    columns_list = ','.join(cols)
    filter_cols = f"AND column_name IN ({columns_list})"
  
  print(filter_cols)
  sf_query = f"""SELECT table_schema AS SCHEMA_NAME
          , table_name AS TABLE_NAME
          , column_name AS COLUMN_NAME
          , ordinal_position AS COLUMN_POSITION
          , data_type AS DATA_TYPE
          , nvl(character_maximum_length, 0) AS CHARACTER_MAX_LENGTH
          , nvl(numeric_precision, 0) AS NUMERIC_PRECISION
          , nvl(numeric_scale, 0) AS NUMERIC_SCALE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE table_catalog = '{db_name}'
          AND table_schema ='{schema_name}'
          AND table_name = '{table_name}'
          {filter_cols}
      ORDER BY COLUMN_POSITION"""

#   print(f"Snowflake: {sf_query}")
  sf_original_schema_df = read_data_from_snowflake(sf_query, custom_schema=schema_str)
#   sf_original_schema_df.printSchema()
#   sf_original_schema_df.display()
  return sf_original_schema_df, sf_query


def get_transformed_original_teradata_schema(schema_name, table_name, columns):
  schema_str = "SCHEMA_NAME string, TABLE_NAME string, COLUMN_NAME string, COLUMN_POSITION int, DATA_TYPE string, CHARACTER_MAX_LENGTH int, NUMERIC_PRECISION int, NUMERIC_SCALE int"
  
  # Get the list of columns and build filter to be used in SQL query
  if(columns == "*"):
    filter_cols = ""
  else:
    cols = ["'" + col.strip() + "'" for col in columns.split(',')]
    columns_list = ','.join(cols)
    filter_cols = f"and trim(col.ColumnName) IN ({columns_list})"
  
  print(filter_cols)
  # TODO: in case we want to also include Views complete information, we need to change from dbc.columnsV to dbc.columnsQV
  td_query = f"""SELECT upper(trim(tab.DatabaseName)) AS SCHEMA_NAME
        , upper(trim(tab.TableName)) AS TABLE_NAME
        , upper(trim(col.ColumnName)) AS COLUMN_NAME
        , row_number() OVER (PARTITION BY tab.TableName ORDER BY col.ColumnId) AS COLUMN_POSITION
        , CASE
            WHEN col.ColumnType IN ('I','N','D','I1','I8','I2') THEN 'NUMBER'
            WHEN col.ColumnType IN ('BO','BF','BV','CF','CV','CO') THEN 'TEXT'
            WHEN col.ColumnType IN ('DY','DH','DM','DS','HR','HM','HS','MI','MS','MO','SC','YR','YM') THEN 'TEXT'
            WHEN col.ColumnType IN ('PS') THEN 'TEXT'
            WHEN col.ColumnType = 'TS' THEN 'TIMESTAMP_NTZ'
            WHEN col.ColumnType = 'SZ' THEN 'TIMESTAMP_TZ'
            WHEN col.ColumnType = 'AT' THEN 'TIME'
            WHEN col.ColumnType = 'DA' THEN 'DATE'
            WHEN col.ColumnType = 'F' THEN 'FLOAT'
            ELSE 'UNKNOWN'
          END AS DATA_TYPE
        , CASE 
            WHEN col.ColumnType IN ('BO','BF','BV') THEN col.ColumnLength
            WHEN col.ColumnType IN ('CF','CV','CO') THEN CAST(SUBSTRING(col.ColumnFormat, 3, LENGTH(col.ColumnFormat)-3) AS SMALLINT)
            WHEN col.ColumnType IN ('DY','DH','DM','DS','HR','HM','HS','MI','MS','MO','SC','YR','YM') THEN 20
            WHEN col.ColumnType IN ('PS') THEN col.ColumnLength
            ELSE 0
          END AS CHARACTER_MAX_LENGTH
        , CASE
            WHEN col.ColumnType IN ('I','N','I1','I8','I2') THEN 38
            WHEN col.ColumnType = 'D' THEN col.DecimalTotalDigits
            WHEN col.ColumnType IN ('DA','F','AT','TS','BO','BF','BV','CF','CV','CO') THEN 0
            WHEN col.ColumnType IN ('DY','DH','DM','DS','HR','HM','HS','MI','MS','MO','SC','YR','YM') THEN 0
            WHEN col.ColumnType IN ('PS') THEN 0
          END AS NUMERIC_PRECISION
        , CASE
            WHEN col.ColumnType = 'D' THEN col.DecimalFractionalDigits
            WHEN col.ColumnType = 'N' THEN
              CASE
                WHEN col.DecimalFractionalDigits < 0 THEN 10
                ELSE col.DecimalFractionalDigits
              END
            ELSE 0
          END AS NUMERIC_SCALE
        --, col.ColumnType AS ORIGINAL_TYPE
		--, col.ColumnLength AS ORIGINAL_CHAR_LENGTH
        --, col.ColumnFormat AS ORIGINAL_FORMAT
        --, col.DecimalTotalDigits AS ORIGINAL_NUMERIC_PRECISION
		--, col.DecimalFractionalDigits AS ORIGINAL_NUMERIC_SCALE
    FROM dbc.tablesV tab
    JOIN dbc.columnsV col
        ON ( tab.DatabaseName = col.DatabaseName
            AND tab.TableName = col.TableName
            AND TableKind IN ('O', 'T', 'V'))
        and upper(trim(col.DatabaseName)) = '{schema_name}'
        and upper(trim(col.TableName)) = '{table_name}'
        {filter_cols}
    """
#   print(f"Teradata: {td_query}")
  td_transformed_schema_df = read_data_from_teradata(td_query, custom_schema=schema_str)
#   td_transformed_schema_df.printSchema()
#   td_transformed_schema_df.display()
  return td_transformed_schema_df, td_query



def validate_table_data_type(summary):
  print("======= Starting TABLE/VIEW DATA TYPE validation =======")
  
  td_transformed_schema_df, td_query = get_transformed_original_teradata_schema(source_schema_name, source_table_name, columns="*")
  sf_original_schema_df, sf_query = get_original_snowflake_schema(sf_database_name, target_schema_name, target_table_name, columns="*")
  
  join_columns = ["SCHEMA_NAME", "TABLE_NAME", "COLUMN_NAME"]
  
  comparison_result = validate_data_content(td_transformed_schema_df, sf_original_schema_df, join_columns)
  
  generate_table_data_type_report(comparison_result, td_query, sf_query)
  display_table_data_type_report(summary, comparison_result)
  
  print("======= TABLE/VIEW DATA TYPE validation complete =======")
  

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate aggregation on specific columns

# COMMAND ----------

def generate_aggregation_report(match_result, agg_result_list, count_result_dict):
  if(export_results):
    data_location, directory_location, file_name = get_report_file_name("AggSummary", sf_full_table_name, dbfs_output_folder, ".txt")
    
    match_str = MatchType(match_result).name
    
    dbutils.fs.mkdirs(directory_location)
    
    with open(data_location + file_name, 'w+', encoding='utf-8') as report_file:
      report_file.write("****** VALIDATION OF AGGREGATE FUNCTIONS ON SPECIFIC COLUMNS ******\n\n")
      report_file.write(f"Executed on: {execution_start_time_formatted}\n")
      report_file.write(f"Table: {sf_full_table_name}\n")
      report_file.write(f"Result: {match_str}\n\n")
    
      # Create a Dataframe from the list of aggregations calculated and save as a CSV
      if(agg_result_list):
        agg_result_columns = ['COLUMN_NAME', 'AGG_FUNCTION', 'TERADATA_VALUE', 'SNOWFLAKE_VALUE', 'MATCH_RESULT', 'AGG_TD_QUERY', 'AGG_SF_QUERY']
        aggregate_df = spark.createDataFrame(data=agg_result_list, schema=agg_result_columns)
        
        save_csv_from_dataframe(aggregate_df, "AggDetails", sf_full_table_name, dbfs_output_folder)
        
        report_file.write(f"Summary of MIN, MAX, SUM, AVG:\n\n")
        
        for row in agg_result_list:
          report_file.write(f"COLUMN_NAME={row[0]}, FUNCTION={row[1]}, TERADATA_VALUE={row[2]}, SNOWFLAKE_VALUE={row[3]}, {row[4]}\n")
          report_file.write("TERADATA SQL query: \n" + row[5] + "\n")
          report_file.write("SNOWFLAKE SQL query: \n" + row[6] + "\n\n")
        
        report_file.write("\n\n")
      if(count_result_dict):
        report_file.write(f"Summary of COUNT:\n\n")
        # Iterate through count result list and get result dataframes by column name
        for col in count_result_dict:
          report_file.write("Column name: " + col + "\n")
          report_file.write("TERADATA SQL query: \n" + count_result_dict[col][1] + "\n")
          report_file.write("SNOWFLAKE SQL query: \n" + count_result_dict[col][2] + "\n")
          
          result = count_result_dict[col][0]
          result.report(file=report_file)
          report_file.write("\n************\n\n")
          
          if(result.rows_both_mismatch.count() > 0):
            save_csv_from_dataframe(result.rows_both_mismatch, "AggCountMismatch_" + col, sf_full_table_name, dbfs_output_folder, CompressionType.GZIP)
          if(result.rows_only_base.count() > 0):
            save_csv_from_dataframe(result.rows_only_base, "AggCountOnlyTD_" + col, sf_full_table_name, dbfs_output_folder, CompressionType.GZIP)
          if(result.rows_only_compare.count() > 0):
            save_csv_from_dataframe(result.rows_only_compare, "AggCountOnlySF_" + col, sf_full_table_name, dbfs_output_folder, CompressionType.GZIP)
            


def display_aggregation_report(summary, match_result, agg_result_list, count_result_dict):
  match_str = MatchType(match_result).name
  
  html_report_list.append(f"""
  <div>
    </br>
    <h1>Validation of Aggregate Functions</h1>
    <p><b>Table: </b>{sf_full_table_name}</p>
    <p><b>Result: </b><mark>{match_str}</mark></p>
  </div>
  """)
  
  summary['AGG_STATUS'] = match_str
  
  if(agg_result_list):    
    agg_result_columns = ['COLUMN_NAME', 'AGG_FUNCTION', 'TERADATA_VALUE', 'SNOWFLAKE_VALUE', 'MATCH_RESULT', 'AGG_TD_QUERY', 'AGG_SF_QUERY']
    aggregate_df = spark.createDataFrame(data=agg_result_list, schema=agg_result_columns)
    aggregate_df = aggregate_df.drop(*('AGG_TD_QUERY', 'AGG_SF_QUERY'))
    html_report_list.append(f"""
    <div>
      <h2>Aggregate Validation Summary (MIN, MAX, SUM, AVG):</h2>
      {dataframe_to_html(aggregate_df, True)}
    </div>
    """)    
    
  if(count_result_dict):
    html_report_list.append("""
      <div>
        </br>
        <h2>Aggregate Validation Summary (COUNT):</h2>
      </div>
    """)
    for col in count_result_dict:
      result = count_result_dict[col][0]
      has_mismatch = result.rows_both_mismatch.count() > 0
      has_only_source = result.rows_only_base.count() > 0
      has_only_target = result.rows_only_compare.count() > 0
      matched_all = not (has_mismatch or has_only_source or has_only_target)
      
      html_report_list.append(f"""
      <div>
        <h3>Column: {col}</h3>
        <p><b>Teradata row count: </b>{result.base_row_count}</p>
        <p><b>Snowflake row count: </b>{result.compare_row_count}</p>
        <p><b>Common row count: </b>{result.common_row_count}</p>
      </div>
        """)      
      display_html_report_sample(result, matched_all, has_mismatch, has_only_source, has_only_target)
  
  

# COMMAND ----------

# DBTITLE 0,4. Validate aggregation on specific columns
def validate_aggregations(summary, source_df, target_df, columns_to_compare, agg_functions, skip_columns_list):
  print("======= Starting AGGREGATION validation =======")
  if((columns_to_compare is not None) and (agg_functions is not None)):
    if(len(columns_to_compare) == len(agg_functions)):
      agg_result_list = []
      count_result_dict = {}
      matched = True
      
      for col,func in zip(columns_to_compare, agg_functions):
        # In case a skipped columns is in the columns to compare, just skip the validation
        if(col in skip_columns_list):
          continue
          
        agg_column_name = f"{func}_VALUE_{col}"
        
        # Calculate the aggregation for source and target
        if(func in ['MIN','MAX','SUM','AVG']):
          td_query = f"SELECT {func}({col}) AS {agg_column_name} FROM {td_full_table_name} {source_filter}"
          sf_query = f"SELECT {func}({col}) AS {agg_column_name} FROM {sf_full_table_name} {target_filter}"          
          print(td_query)
          
#           teradata_df = source_df.agg({col: func}).withColumnRenamed(f"{func}({col})", agg_column_name)
#           snowflake_df = target_df.agg({col: func}).withColumnRenamed(f"{func}({col})", agg_column_name)

          # TODO: implement round on both TD and SF values to have same base for comparison
          td_agg_value = source_df.agg({col: func}).collect()[0][0]
          sf_agg_value = target_df.agg({col: func}).collect()[0][0]
    
          matched = matched and (td_agg_value == sf_agg_value)
          match_str = MatchType(td_agg_value == sf_agg_value).name
          agg_result_list.append((col, func, str(td_agg_value), str(sf_agg_value), match_str, td_query, sf_query))
          
        elif(func == 'COUNT'):
          td_query = f"SELECT {col}, CAST(COUNT({col}) AS BIGINT) AS {agg_column_name} FROM {td_full_table_name} {source_filter} GROUP BY {col} ORDER BY {col}"
          sf_query = f"SELECT {col}, CAST(COUNT({col}) AS BIGINT) AS {agg_column_name} FROM {sf_full_table_name} {target_filter} GROUP BY {col} ORDER BY {col}"
          
          print(td_query)
          
          teradata_df = source_df.groupBy(col).count().withColumnRenamed("COUNT", agg_column_name).orderBy(col)
          snowflake_df = target_df.groupBy(col).count().withColumnRenamed("COUNT", agg_column_name).orderBy(col)
          
          result = datacompy.SparkCompare(spark, base_df=teradata_df, compare_df=snowflake_df, join_columns=[col], cache_intermediates=True, show_all_columns=True, match_rates=True)
          count_result_dict[col] = (result, td_query, sf_query)
          matched = matched and ((result.rows_both_mismatch.count() == 0) and (result.rows_only_base.count() == 0) and (result.rows_only_compare.count() == 0))
        else:
          raise AssertionError(f"Aggregate function '{func}' is not supported by framework. Please review config file")
                
#         teradata_df.display()
#         snowflake_df.display()

        # Compare source and target tables using Pandas comparison, as no join_columns is informed
#         result = datacompy.Compare(teradata_df.toPandas(), snowflake_df.toPandas(), on_index=True, df1_name='TERADATA', df2_name='SNOWFLAKE', cast_column_names_lower=False)
# #         print(result.report())
#         mismatches = result.all_mismatch()
#         mismatches.display() if(mismatches.size > 0) else print("No mismatches found!")
      
      generate_aggregation_report(matched, agg_result_list, count_result_dict)
      display_aggregation_report(summary, matched, agg_result_list, count_result_dict)
    else:
      raise AssertionError("Each column to compare aggregation need to have an aggregate function associated!")
  else:
    print("No columns to validate aggregations")
    
  print("======= AGGREGATION validation complete! =======")   
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate Business Queries if any specific testing from Business
# MAGIC Until now, there's no specific query requested by Business team

# COMMAND ----------

# DBTITLE 0,5. Validate Business Queries if any specific testing from Business
# Validate any query that Business team requested for tables
def validate_business_queries(summary):
  print("======= Starting BUSINESS QUERIES validation =======")
  print("======= BUSINESS QUERIES validation complete =======")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate of Future Date Conversion between Source and Target

# COMMAND ----------

# Identify potential future dates that can have issues when converted to Snowflake
def validate_future_dates_conversion(summary):
  print("======= Starting FUTURE DATE CONVERSION validation =======")
  print("======= FUTURE DATE CONVERSION validation complete =======")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate table data content

# COMMAND ----------

def generate_data_content_report(result, source_query, target_query, unique_columns, skip_columns):
  
  if(export_results):
    has_mismatch = result.rows_both_mismatch.count() > 0
    has_only_source = result.rows_only_base.count() > 0
    has_only_target = result.rows_only_compare.count() > 0
    matched_all = not (has_mismatch or has_only_source or has_only_target)
    
    match_str = MatchType(matched_all).name

    if(has_mismatch):
      save_csv_from_dataframe(result.rows_both_mismatch, "DataContentMismatch", sf_full_table_name, dbfs_output_folder, CompressionType.GZIP)
    if(has_only_source):
      save_csv_from_dataframe(result.rows_only_base, "DataContentOnlyTD", td_full_table_name, dbfs_output_folder, CompressionType.GZIP)
    if(has_only_target):
      save_csv_from_dataframe(result.rows_only_compare, "DataContentOnlySF", sf_full_table_name, dbfs_output_folder, CompressionType.GZIP)
    
    data_location, directory_location, file_name = get_report_file_name("DataContentSummary", sf_full_table_name, dbfs_output_folder, ".txt")
    
    dbutils.fs.mkdirs(directory_location)
    
    with open(data_location + file_name, 'w+', encoding='utf-8') as report_file:
      report_file.write("****** VALIDATION OF TABLE DATA CONTENT ******\n")
      report_file.write(f"Executed on: {execution_start_time_formatted}\n")
      report_file.write(f"Table: {sf_full_table_name}\n\n")
      report_file.write(f"Unique keys: {unique_columns}\n")
      report_file.write(f"Skipped columns: {skip_columns}\n")
      report_file.write(f"Teradata row count: {result.base_row_count}\n")
      report_file.write(f"Snowflake row count: {result.compare_row_count}\n\n")
      report_file.write(f"Result = {match_str}\n\n")
      result.report(file=report_file)
      report_file.write("\n************\n\n")
      report_file.write(f"TERADATA SQL load query:\n{source_query}\n\n")
      report_file.write(f"SNOWFLAKE SQL load query:\n{target_query}\n")
  
  else:
    result.report()
    

def display_data_content_report(summary, result, unique_columns, skip_columns):
  has_mismatch = result.rows_both_mismatch.count() > 0
  has_only_source = result.rows_only_base.count() > 0
  has_only_target = result.rows_only_compare.count() > 0
  matched_all = not (has_mismatch or has_only_source or has_only_target)

  match_str = MatchType(matched_all).name
  
  summary['DATA_CONTENT_STATUS'] = match_str
  
  html_report_list.append(f"""
  <div>
    </br>
    <h1>Validation of Data Content</h1>
    <p><b>Table: </b>{sf_full_table_name}</p>    
    <p><b>Result: </b><mark>{match_str}</mark></p>
    <p><b>Unique keys: </b>{unique_columns}</p>
    <p><b>Skipped columns: </b>{skip_columns}</p>
    <p><b>Columns compared: </b>{result.columns_compared}</p>
    <p><b>Teradata row count: </b>{result.base_row_count}</p>
    <p><b>Snowflake row count: </b>{result.compare_row_count}</p>
    <p><b>Common row count: </b>{result.common_row_count}</p>
  </div>
  """)
  display_html_report_sample(result, matched_all, has_mismatch, has_only_source, has_only_target)
  

# COMMAND ----------

# DBTITLE 0,7. Validate table/view data content
# Use Datacompy to compare Snowflake X Teradata dataframes
def validate_data_content(source_df, target_df, join_columns_list, skip_columns_list=None):
  assert (source_df.count() > 0 and target_df.count() > 0), f"Content comparison failed. Some table is empty: Teradata = {source_df.count()}, Snowflake = {target_df.count()}"
  
  if(skip_columns_list is not None):
    print(f"Removed skipped columns from dataframes for comparison: {skip_columns_list}")
    source_df = source_df.drop(*skip_columns_list)
    target_df = target_df.drop(*skip_columns_list)
  
  return datacompy.SparkCompare(spark_session=spark, base_df=source_df, compare_df=target_df, join_columns=join_columns_list, cache_intermediates=True, show_all_columns=True, match_rates=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. File Comparison

# COMMAND ----------

# TODO: add code to compare file and generate reports (output and html)

def generate_file_comparison_report():
  pass


def display_file_comparison_report():
  pass


def validate_file():
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC # Execution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation pipeline

# COMMAND ----------

# DBTITLE 0,------------------------------------------------------Validation pipeline------------------------------------------------------
# Validation pipeline
def run_validations(config_row):
  try:
    # Load all global configuration variables from config file
    load_config_row(config_row)
    
    summary = {'STATUS': MatchType.UNKNOWN.name, 'SOURCE_TABLE': td_full_table_name, 'TARGET_TABLE': sf_full_table_name,
               'ROW_COUNT_STATUS': MatchType.UNKNOWN.name, 'DATA_TYPE_STATUS': MatchType.UNKNOWN.name, 'DATA_CONTENT_STATUS': MatchType.UNKNOWN.name, 'AGG_STATUS': MatchType.UNKNOWN.name,
              'SOURCE_ROW_COUNT': '', 'TARGET_ROW_COUNT': '', 'SOURCE_FILTER': source_filter, 'TARGET_FILTER': target_filter, 'VALIDATION_TIME': 0}
    
    if(("All" in validations) or ("Table Row Count" in validations)):
      # Validate Table Row Count
      validate_table_row_count(summary)
    
    if(("All" in validations) or ("Table Data Type" in validations)):
      # Validate Table Data Type
      validate_table_data_type(summary)
      
    if(("All" in validations) or ("Business Queries" in validations)):
      # Validate Business Queries
      validate_business_queries(summary)
      
    if(("All" in validations) or ("ETL execution (only Historical)" in validations)):
      # Validate ETL process execution
      validate_etl_execution_snowflake(summary)
      
    if(("All" in validations) or ("Future Dates Conversion" in validations)):
      # Validate Future Dates Conversion
      validate_future_dates_conversion(summary)
    
    if(("All" in validations) or ("Content Comparison" in validations) or ("Aggregation" in validations)):
      if(validation_object_type == 'TABLE'):
        # Convert Teradata data types to PySpark types and create a custom schema
        td_schema, td_query = get_transformed_original_teradata_schema(source_schema_name, source_table_name, columns=source_columns)
        td_spark_schema = parse_types_from_snowflake_to_spark(td_schema, to_ddl=True)

        # Convert Snowflake data types to PySpark types and create a custom schema
        sf_schema, sf_query = get_original_snowflake_schema(sf_database_name, target_schema_name, target_table_name, columns=target_columns)
        sf_spark_schema = parse_types_from_snowflake_to_spark(sf_schema, to_ddl=True)

        # Load tables from Teradata and Snowflake using custom schema
        teradata_df, td_load_query = load_table_from_teradata(custom_schema=td_spark_schema)
        snowflake_df, sf_load_query = load_table_from_snowflake(custom_schema=sf_spark_schema)
      elif(validation_object_type == 'VIEW'):
        # Inferring schema for Teradata
        teradata_df, td_load_query = load_table_from_teradata()
        sf_schema = teradata_df.schema
        # Using the same schema to load table from Snowflake
        snowflake_df, sf_load_query = load_table_from_snowflake(custom_schema=sf_schema)
      
      # Rename all column names from Teradata to be upper case
      teradata_df = rename_column_names_to_uppercase(teradata_df)
      
      # Trims all string columns to remove white spaces from begin and end (usually needed for Teradata)
      teradata_df = trim_all_string_columns(teradata_df)
      snowflake_df = trim_all_string_columns(snowflake_df)
      
      if(("All" in validations) or ("Aggregation" in validations)):
        # Validate Aggregate Functions
        validate_aggregations(summary, teradata_df, snowflake_df, aggregate_columns, aggregate_functions, skip_columns)
        
      if(("All" in validations) or ("Content Comparison" in validations)):
        # Generate a hashed column to use as unique key for content comparison in case unique_keys is not informed on config file
        if(not unique_keys):
          print("Calculating HASH from table columns")
          teradata_df = generate_hashed_unique_key(teradata_df, skip_columns)
          snowflake_df = generate_hashed_unique_key(snowflake_df, skip_columns)
          unique_columns = ['HASHED_UNIQUE_KEY']
        else:
          print("Using unique_keys as join columns for comparison")
          unique_columns = unique_keys
        
        # Validate data content
        print("======= Starting DATA CONTENT validation =======")
        comparison_result = validate_data_content(teradata_df, snowflake_df, join_columns_list=unique_columns, skip_columns_list=skip_columns)
        print("======= DATA CONTENT validation complete! =======")
        generate_data_content_report(comparison_result, td_load_query, sf_load_query, unique_columns, skip_columns)
        display_data_content_report(summary, comparison_result, unique_columns, skip_columns)
    
    # Calculate summary status based on each validation status
    all_status = {summary['ROW_COUNT_STATUS'], summary['DATA_TYPE_STATUS'], summary['DATA_CONTENT_STATUS'], summary['AGG_STATUS']}
    if(MatchType.MISMATCH.name in all_status):
      summary['STATUS'] = MatchType.MISMATCH.name
    elif(MatchType.MATCH.name in all_status):
      summary['STATUS'] = MatchType.MATCH.name
  
  finally:
    print("END OF COMPARISON")
    return summary
  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation loop

# COMMAND ----------

# DBTITLE 1,------------------------------------------------------Validation loop------------------------------------------------------
# Validation loop
if(verify_mandatory_parameters()):
  # TODO: implement logging to append the steps execution and more information
  print(f"====== Starting validations {validations} for {lob} with config {config_file_name} ======")
  summary_list = []
  try:
    # TODO: try to see if there's a better way to loop through rows of a PySpark Dataframe
    iter = active_config_df.toLocalIterator()
    for row in iter:
      start_time = datetime.now(timezone.utc)
      
      summary_result = run_validations(row)
      
      end_time = datetime.now(timezone.utc)
      # Calculates the elapsed time of all validations and add it to summary
      elapsed_time = str(end_time - start_time)
      summary_result['VALIDATION_TIME'] = elapsed_time
      
      summary_list.append(summary_result)
      
    # Remove all temporary files created during execution
    #clean_tmp_folder()
    
    print("====== Validation complete! ======")
    html_report_list.insert(0, generate_report_header())
    html_report_list.insert(1, generate_summary_report(summary_list))
    
    html_report = "".join(html_report_list)
    dbutils.notebook.exit(html_report)
    
  except AssertionError as e:
    print(e)
    dbutils.notebook.exit(f"""<font size="4" color="red" face="sans-serif">{e}</font>""")
    
else:
  dbutils.notebook.exit(f"""<font size="4" color="red" face="sans-serif">Missing mandatory parameter</font>""")

