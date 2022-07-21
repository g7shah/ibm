# Databricks notebook source
# MAGIC %md
# MAGIC ## Dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import datacompy
import pandas as pd
import os
from zipfile import ZipFile
from datetime import datetime, timezone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.dropdown(name="environment", defaultValue="SIT", choices=["DEV", "SIT", "UAT"], label="Enviroment")
dbutils.widgets.dropdown(name="exportResults", defaultValue="Yes", choices=["Yes", "No"], label="Export results to Storage Account?")
dbutils.widgets.dropdown(name="showReportSamples", defaultValue="Yes", choices=["Yes", "No"], label="Show report samples?")
dbutils.widgets.text(name="numberOfSamples", defaultValue="10", label="Number of Samples")

environment = dbutils.widgets.get("environment").lower()

export_results = False if(dbutils.widgets.get("exportResults") == 'No') else True
show_samples = False if(dbutils.widgets.get("showReportSamples") == 'No') else True
number_of_samples = int(dbutils.widgets.get("numberOfSamples"))

# COMMAND ----------

# Define the timestamp that will be in the output files
execution_start_time = datetime.now(timezone.utc)
execution_start_time_formatted = execution_start_time.strftime("%a_%b_%d_%H_%M_%S_%Z_%Y")
execution_start_time_formatted

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storage account setup

# COMMAND ----------

secret_scope = ("testingkv-" + environment)
container = "ibm" + environment

# Configuration for Storage account
storage_container = "ibm" + environment
storage_account = "adlseus2tlsdp" + environment + "02"
storage_account_access_key = dbutils.secrets.get(secret_scope, f"sta-adls2-{storage_container}02-access-key")
storage_connection_str = "wasbs://" + storage_container + "@" + storage_account + ".blob.core.windows.net/" + "TestFramework"
storage_conf_key = "fs.azure.account.key." + storage_account + ".blob.core.windows.net"

# Mount configuration
dbfs_mount_point = "/mnt/test-framework/" + environment
dbfs_input_folder = dbfs_mount_point + "/webfocusReportTesting/input/"
dbfs_output_folder = dbfs_mount_point + "/webfocusReportTesting/output/"

# COMMAND ----------

# Mount the Storage account to use DBFS only if it's not already mounted
if([item.mountPoint for item in dbutils.fs.mounts()].count(dbfs_mount_point) == 0):
    dbutils.fs.mount(
        source = storage_connection_str,
        mount_point = dbfs_mount_point,
        extra_configs = {storage_conf_key:storage_account_access_key}
    )

# Check if input and output directories exists
# display(dbutils.fs.mounts())
# display(dbutils.fs.ls(dbfs_input_folder))
# display(dbutils.fs.ls(dbfs_output_folder))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data from CSV

# COMMAND ----------

def read_data_from_csv(file_location):
    # CSV options
    infer_schema = "true"
    first_row_is_header = "true"
    delimiter = ","
    
    # Read data from a CSV and return a Dataframe. Set empty string to null values
    df = spark.read.format("csv").option("delimiter", delimiter).option("header", first_row_is_header).load(file_location).na.fill(value="")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save CSV file

# COMMAND ----------

def get_report_file_path(filename, output_folder):
    # ex: bhpp_and_ptb_20220613_summary.txt
    filename = f"{filename}_summary.txt"
    filepath = f"{output_folder}{filename}"
    return filepath


def save_report_file(comparison_result, filename, result_str, output_folder):
    filepath = get_report_file_path(filename, output_folder)
    
    with open(filepath, 'w+', encoding='utf-8') as report_file:
        report_file.write("************ CSV CONTENT VALIDATION ************\n\n")
        report_file.write(f"Executed on: {execution_start_time_formatted}\n")
        report_file.write(f"Teradata filename: {filename}_td.csv\n")
        report_file.write(f"Snowflake filename: {filename}_sf.csv\n\n")
        report_file.write(f"Result = {result_str}\n\n")
        report_file.write(f"Teradata file unique columns: {list(comparison_result.df1_unq_columns())}\n")
        report_file.write(f"Snowflake file unique columns: {list(comparison_result.df2_unq_columns())}\n\n")
        report_file.write(comparison_result.report())
        report_file.write("\n************\n\n")    
    

# COMMAND ----------

def get_csv_file_path(content_type, filename, output_folder):
    # ex: bhpp_and_ptb_20220613_mismatch.csv
    # ex: bhpp_and_ptb_20220613_onlyTD.csv
    # ex: bhpp_and_ptb_20220613_onlySF.csv
    csv_filename = f"{filename}_{content_type}.csv"
    filepath = f"{output_folder}{csv_filename}"
    return filepath


def save_csv_from_dataframe(dataframe, content_type, filename, output_folder):
    filepath = get_csv_file_path(content_type, filename, output_folder)
    print("output csv filepath = " + filepath)

    dataframe.to_csv(filepath, sep=',', header=True, encoding='utf-8')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validations

# COMMAND ----------

def validate_file_content(df_base, df_compare):
    df_pandas_base = df_base.toPandas()
    df_pandas_compare = df_compare.toPandas()
    
    # Compare using Pandas dataframe to use on_index=True and not depend on unique keys to join dataframes
    comparison_result = datacompy.Compare(df1=df_pandas_base, df2=df_pandas_compare, on_index=True, df1_name="Teradata", df2_name="Snowflake", cast_column_names_lower=False)
    
    return comparison_result
    
    
def generate_report(comparison_result, csv_filename, export_results, show_samples, number_of_samples, input_folder):
    has_mismatch = not comparison_result.all_mismatch().empty
    has_td_unique_rows = not comparison_result.df1_unq_rows.empty
    has_sf_unique_rows = not comparison_result.df2_unq_rows.empty
    has_td_unique_cols = len(comparison_result.df1_unq_columns()) > 0
    has_sf_unique_cols = len(comparison_result.df2_unq_columns()) > 0
    
    matched_all = not(has_mismatch or has_td_unique_rows or has_sf_unique_rows or has_td_unique_cols or has_sf_unique_cols)
    result_str = "MATCH" if(matched_all) else "MISMATCH"
    
    if(export_results):
        # TODO: export all result files to storage account
        output_folder = "/" + input_folder.replace("/input/", "/output/").replace(":","")
        print(output_folder)
        
        # Export a CSV file for each mismatch type found
        if(has_mismatch):
            save_csv_from_dataframe(comparison_result.all_mismatch(), "mismatch", csv_filename, output_folder)
        if(has_td_unique_rows):
            save_csv_from_dataframe(comparison_result.df1_unq_rows, "onlyTD", csv_filename, output_folder)
        if(has_sf_unique_rows):
            save_csv_from_dataframe(comparison_result.df2_unq_rows, "onlySF", csv_filename, output_folder)
            
        
        # Export a summary report
        save_report_file(comparison_result, csv_filename, result_str, output_folder)
        
    else:
        # Display the comparison result on screen
        print("************ CSV CONTENT VALIDATION ************")
        print(f"Filename = {csv_filename}")
        print(f"Result = {result_str}\n")
        print(comparison_result.report())
        
        if(show_samples):
            print("MISMATCHES SAMPLES:")
            # Display all mismatched rows, if any
            if(has_mismatch):
                print(comparison_result.all_mismatch().head(number_of_samples))

            # Display all unique rows in Teradata, if any
            if(has_td_unique_rows):
                print(comparison_result.df1_unq_rows.head(number_of_samples))

            # Display all unique rows in Snowflake, if any
            if(has_sf_unique_rows):
                print(comparison_result.df2_unq_rows.head(number_of_samples))

            # Display all unique columns in Teradata, if any
            if(has_td_unique_cols):                
                print(f"Teradata file unique columns: {list(comparison_result.df1_unq_columns())}")

            # Display all unique columns in Snowflake, if any
            if(has_sf_unique_cols):
                print(f"Snowflake file unique columns: {list(comparison_result.df2_unq_columns())}")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution

# COMMAND ----------

# TODO:
#1- iterate through all child folders
#2- extract all zip files and move them to archive folder inside the input folder
#3- fetch the 2 files from Teradata and Snowflake
#4- validate content for the 2 files
input_path_mnt = dbutils.fs.ls(dbfs_input_folder)
input_folder_list = [x.path for x in input_path_mnt]

full_unmapped_csv_list = []
for folder in input_folder_list:
    print(folder)
    # Replace the ':' on dbfs path
    folder_write_path = "/" + folder.replace(":", "")
    file_list = dbutils.fs.ls(folder)
    # Gets all zip files inside the folder
    zip_file_list = [x.path for x in file_list if x.path.endswith(".zip")]
    #print(zip_file_list)
    
    # Iterate through the zip files, extract them and archive
    for zip_file_path in zip_file_list:
        print(zip_file_path)
        # Replace the ':' on dbfs path
        file_path = "/" + zip_file_path.replace(":", "")
        # Extract the zip file and acrhive it
        try:
            with ZipFile(file_path, 'r') as zip_file:
                #files = zip_file.namelist()
                zip_file.extractall(path=folder_write_path)
                zip_file_name = os.path.basename(zip_file.filename)
                #print("zip file name = " + zip_file_name)
            
            #Archive the zip file extracted            
            archive_path = folder + "archive/" + zip_file_name
            #print("archive path = " + archive_path)
            #dbutils.fs.mv(zip_file_path, archive_path)
        finally:
            pass
        
    # Update the list of files within the folder after extracting all zip files
    file_list = dbutils.fs.ls(folder)
    
    # Fetch the files to be compared
    teradata_csv_list = [x.path for x in file_list if x.path.endswith("_td.csv")]
    snowflake_csv_list = [x.path for x in file_list if x.path.endswith("_sf.csv")]
    
    # Get the name of file and try to read_data_from_csv using the _td and _sf. If it fails to read, it means the file doesn't have a pair. But you can continue to the next pair
    for td_filepath in teradata_csv_list[:]:
        common_filename = td_filepath.rsplit('_', 1)[0]
        for sf_filepath in snowflake_csv_list[:]:
            if sf_filepath.startswith(common_filename):
                teradata_csv_list.remove(td_filepath)
                snowflake_csv_list.remove(sf_filepath)
                # Read data from CSV files
                df_td = read_data_from_csv(td_filepath)
                df_sf = read_data_from_csv(sf_filepath)
                # Validate content
                comparison_result = validate_file_content(df_td, df_sf)
                # Generate comparison report
                csv_filename_no_extension = os.path.basename(common_filename).split('.')[0]
                generate_report(comparison_result, csv_filename_no_extension, export_results, show_samples, number_of_samples, folder)
                
                # archive the 2 csv files after comparing                
                dbutils.fs.mv(td_filepath, folder + "archive/" + os.path.basename(td_filepath))
                dbutils.fs.mv(sf_filepath, folder + "archive/" + os.path.basename(sf_filepath))
    # case for existing files without a pair fetched
    # collect list of unmapped csv
    full_unmapped_csv_list.extend(snowflake_csv_list + teradata_csv_list)
    for td_unmapped_csv in teradata_csv_list:
        print(f"No mapping Teradata csv file has been found for: {td_unmapped_csv}")
    for sf_unmapped_csv in snowflake_csv_list:
        print(f"No mapping Snowflake csv file has been found for: {sf_unmapped_csv}")

# COMMAND ----------

