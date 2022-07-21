# Databricks notebook source
# MAGIC %md
# MAGIC ![Air Canada](https://www.aircanada.com/content/dam/aircanada/generic/ac_logo.svg)
# MAGIC 
# MAGIC ***
# MAGIC * __*Version :*__ 0.01
# MAGIC 
# MAGIC # Teradata Migration Validation Report

# COMMAND ----------

dbutils.widgets.dropdown(name="lob", defaultValue="Flight Ops: FOP", choices=["Cargo: CGO","Maintenance: MNT","Flight Ops: FOP","Operations: OPS","Commercials: COM","Customer Relations: CRM","Rev Acct: RAC","Tax: TAX","Finance: FIN","Mkt Ops: MOP","Security: SCR","Loyalty: LTY"], label="LOB")
dbutils.widgets.text(name="configFileName", defaultValue="hist_config.csv", label="Config File Name")
dbutils.widgets.multiselect(name="validationsToRun", defaultValue="All", choices=["All", "ETL execution (only Historical)", "Table Row Count", "Table Data Type", "Aggregation", "Business Queries", "Future Dates Conversion", "Content Comparison"], label="Validations to Run")

dbutils.widgets.dropdown(name="environment", defaultValue="SIT", choices=["DEV", "SIT", "UAT"], label="Enviroment")
dbutils.widgets.dropdown(name="exportResults", defaultValue="No", choices=["Yes", "No"], label="Export results to Storage Account?")
dbutils.widgets.dropdown(name="showReportSamples", defaultValue="Yes", choices=["Yes", "No"], label="Show report samples?")

config_file_name = dbutils.widgets.get("configFileName")
validations = dbutils.widgets.get("validationsToRun")
environment = dbutils.widgets.get("environment")
export_results = dbutils.widgets.get("exportResults")
report_samples = dbutils.widgets.get("showReportSamples")

lob = dbutils.widgets.get("lob")


# COMMAND ----------

# Calls the validation notebook and get an HTML report to be displayed
report_to_display = dbutils.notebook.run("TeradataMigrationValidation", 0, arguments={'exportResults':export_results, 'environment':environment, 'lob':lob, 'configFileName':config_file_name, 'validationsToRun':validations, 'showReportSamples':report_samples})

# COMMAND ----------

displayHTML(report_to_display)