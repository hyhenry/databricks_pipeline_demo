from pyspark import pipelines as dp
from pyspark.sql.functions import col
import urllib
import zipfile
import os
import logging

logging.basicConfig(level=logging.INFO)
logging.info("Starting data ingestion step...")


files_all = {
    "well_licence_sk" : "https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Well%20Licence/CSV",
}

#is_dev = spark.conf.get("pipelines.env") == "development"
is_dev = ${var.env} == "development"
catalog = ${var.catalog}
schema = ${var.schema}
print(catalog)
print(schema)
volume = "csv_files"
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"


def extract_csv(input_selection):
    zip_path = f"{volume_path}/{input_selection}.zip"
    urllib.request.urlretrieve(files_all.get(input_selection), zip_path)
    print(f"Saving and moving the parent zip file to {zip_path}.zip")

    # Extracting first zip file 
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(f"{volume_path}/{input_selection}")
    logging.info(f"Extracting the parent zip file into {volume_path}/{input_selection}")

    # Grabbing the name of the extracted files
    temp_name = None
    for files in os.listdir(f"{volume_path}/{input_selection}/"):
        temp_name = files
    logging.info(f"Extracted file name: {temp_name}")

    # Extracting the second zip file
    with zipfile.ZipFile(f"{volume_path}/{input_selection}/{temp_name}", 'r') as zip_ref:
        zip_ref.extractall(f"{volume_path}/{input_selection}")
    logging.info(f"Extracted nested zip file: {temp_name}")

    # Grabbing the name of the csv file after final extraction
    csv_name = None
    for files in os.listdir(f"{volume_path}/{input_selection}/"):
        if files.lower().endswith(".csv"):
            csv_name = files
    logging.info(f"Final CSV file name: {csv_name}")

    # Move the csv file to the correct location
    src = os.path.join(f"{volume_path}/{input_selection}", f"{csv_name}")
    dst = os.path.join(f"{volume_path}", f"{csv_name}")
    os.rename(src, dst)
    logging.info(f"Moved {csv_name} from {src} to {dst}")

    # Creating the dataframe from the loaded csv data
    df = spark.read.format("csv").option("header", True).load(
        f"{volume_path}/{csv_name}"
    )

    # Removing all left over zip files used to load the data
    os.remove(zip_path)
    os.remove(f"{volume_path}/{input_selection}/{temp_name}")
    os.rmdir(f"{volume_path}/{input_selection}")

    logging.info(f"Cleaned up extracted files")

    return df

@dp.table(name="well_licence_sk")
def get_table():
    if is_dev:
        return extract_csv("well_licence_sk")
    else:
        raise RuntimeError(f"catalog = {catalog} and schema = {schema}, env = {is_dev}.")

      