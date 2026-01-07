# Databricks notebook source
from pyspark.sql.functions import col
import urllib
import zipfile
import os

# COMMAND ----------

files_all = {
    "business_associate_ab" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Business%20Associate/CSV",
    "well_infrastructure_ab": "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Well%20Infrastructure/CSV",
    "well_licence_ab" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Well%20Licence/CSV",
    "facility_infrastructure_ab" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Facility%20Infrastructure/CSV",
    "facility_operator_history_ab" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Facility%20Operator%20History/CSV",
    "well_to_facility_link_ab" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Well%20to%20Facility%20Link/CSV",
    "facility_licence_ab" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Facility%20Licence/CSV",
    "business_associate_sk" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Business%20Associate/CSV",
    "well_infrastructure_sk": "https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Well%20Infrastructure/CSV",
    "well_licence_sk" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Well%20Licence/CSV",
    "facility_infrastructure_sk" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Facility%20Infrastructure/CSV",
    "facility_operator_history_sk" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Facility%20Operator%20History/CSV",
    "well_to_facility_link_sk" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Well%20to%20Facility%20Link/CSV",
    "facility_licence_sk" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Facility%20Licence/CSV"
}

# COMMAND ----------

dbutils.widgets.text("env","development") #set default environment to dev

is_dev = dbutils.widgets.get("env") == "development"
catalog = "bronze"
schema = "petrinex"

if is_dev:
    catalog = f"{catalog}_dev"
    print(f"running in dev environment, saving data to {catalog}.{schema}")
else:
    print(f"running in prd environment, saving data to {catalog}.{schema}")

volume = "csv_files"
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------

# create any assets if not already created
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume};")

# COMMAND ----------

def extract_csv(input_selection):
    zip_path = f"{volume_path}/{input_selection}.zip"
    urllib.request.urlretrieve(files_all.get(input_selection), zip_path)
    print(f"Saving and moving the parent zip file to {zip_path}.zip")

    # Extracting first zip file 
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(f"{volume_path}/{input_selection}")
    print(f"Extracting the parent zip file into {volume_path}/{input_selection}")

    # Grabbing the name of the extracted files
    temp_name = None
    for files in os.listdir(f"{volume_path}/{input_selection}/"):
        temp_name = files
    print(f"Extracted file name: {temp_name}")

    # Extracting the second zip file
    with zipfile.ZipFile(f"{volume_path}/{input_selection}/{temp_name}", 'r') as zip_ref:
        zip_ref.extractall(f"{volume_path}/{input_selection}")
    print(f"Extracted nested zip file: {temp_name}")

    # Grabbing the name of the csv file after final extraction
    csv_name = None
    for files in os.listdir(f"{volume_path}/{input_selection}/"):
        if files.lower().endswith(".csv"):
            csv_name = files
    print(f"Final CSV file name: {csv_name}")

    # Move the csv file to the correct location
    src = os.path.join(f"{volume_path}/{input_selection}", f"{csv_name}")
    dst = os.path.join(f"{volume_path}", f"{csv_name}")
    os.rename(src, dst)
    print(f"Moved {csv_name} from {src} to {dst}")

    # Creating the dataframe from the loaded csv data
    df = spark.read.format("csv").option("header", True).load(
        f"{volume_path}/{csv_name}"
    )

    # Removing all left over zip files used to load the data
    os.remove(zip_path)
    os.remove(f"{volume_path}/{input_selection}/{temp_name}")
    os.rmdir(f"{volume_path}/{input_selection}")

    print(f"Cleaned up extracted files")

    return df

# COMMAND ----------

for f_name in list(files_all.keys()):
    df = extract_csv(f_name)
    print(f"Saving {f_name} to table {catalog}.{schema}.{f_name}")
    df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{f_name}")
