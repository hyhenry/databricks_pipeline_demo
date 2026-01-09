# Databricks notebook source
# MAGIC %md
# MAGIC ## Write Data
# MAGIC Write data to a Delta table with options for merge and overwrite operations, and clustering.
# MAGIC
# MAGIC Writes data to a Delta table. Can either write a new table with or without clustering. Can
# MAGIC overwrite a table with or without clustering. Clustering is done if a cluster column(s) are specified.
# MAGIC Can merge data into a table using merge keys and the cluster keys are separate from those. 
# MAGIC Cluster keys cannot even be used during a merge, they only exist when a table is first created or if it is being overwritten.
# MAGIC
# MAGIC > The only required fields are the source dataframe and target table. The other arguments are considered optional
# MAGIC as they can be omitted and left to run as default. There is error handling and warnings to indicate if something
# MAGIC must be added to the arguments to perform the specific kind of action. 
# MAGIC
# MAGIC ### Inputs
# MAGIC - source_dataframe (DataFrame): Source DataFrame to write.
# MAGIC - target_table (str): Target table name (e.g., 'raw.jira.sr').
# MAGIC - merge_keys (list[str] | None): List of columns for merge operation (only used if `overwrite` is False).
# MAGIC - cluster_keys (list[str] | None): List of columns for clustering when creating or overwriting a table.
# MAGIC - overwrite (bool): If True, overwrites the table; if False, merges data if the table exists.
# MAGIC
# MAGIC > **Note:** \
# MAGIC `merge_keys` is only optional when writing a new table or overwriting, it is required for merging.\
# MAGIC If you do not want to use liquid clustering, do not set any `cluster_keys`
# MAGIC
# MAGIC ### Usage Examples
# MAGIC
# MAGIC #####Very basic usage:
# MAGIC ```python
# MAGIC write_data(source_dataframe=df, target_table="raw.jira.sr", merge_keys=['item_id'], cluster_keys=['item_id'], overwrite=True)
# MAGIC ```
# MAGIC
# MAGIC #####Other usage examples:
# MAGIC
# MAGIC ```python 
# MAGIC if load_type == 'Full':
# MAGIC     write_data(source_dataframe=df, target_table="raw.jira.sr", merge_keys=['item_id'], cluster_keys=['item_id'], overwrite=True)
# MAGIC else:
# MAGIC     write_data(source_dataframe=df, target_table="raw.jira.sr", merge_keys=['item_id'], cluster_keys=['item_id'], overwrite=False)
# MAGIC ```
# MAGIC > It is not required to directly specify each argument depending on the goal
# MAGIC ```python 
# MAGIC if load_type == 'Full':
# MAGIC     write_data(source_dataframe=df, target_table="raw.jira.sr", cluster_keys=['item_id'], overwrite=True)
# MAGIC else:
# MAGIC     write_data(source_dataframe=df, target_table="raw.jira.sr", merge_keys=['item_id'])
# MAGIC ```
# MAGIC > If you don't want to use liquid clustering, just omit the cluster_keys argument:
# MAGIC ```python 
# MAGIC write_data(source_dataframe=df, target_table="raw.jira.sr", overwrite=True)
# MAGIC ```

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame as ClassicDataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from delta.tables import DeltaTable

def write_data(
    source_dataframe,
    target_table: str,
    merge_keys: list[str] | None = None,
    cluster_keys: list[str] | None = None,
    overwrite: bool = False
) -> None:
    """
    Write data to a Delta table with support for both classic Spark and Spark Connect DataFrames.
    Supports:
      - Overwrite or merge mode
      - Optional clustering (bucketed sort order) on up to 4 columns
      - Schema compatibility checks
      - SQL fallback for Spark Connect
    """

    # Identify the DataFrame type (Classic or Connect)
    is_classic_df = isinstance(source_dataframe, ClassicDataFrame)
    is_connect_df = isinstance(source_dataframe, ConnectDataFrame)

    # Validate input types
    if not (is_classic_df or is_connect_df):
        raise TypeError("source_dataframe must be a classic Spark or Spark Connect DataFrame.")
    if not isinstance(target_table, str):
        raise TypeError("target_table must be a string.")

    # Validate and inspect clustering column types
    if cluster_keys:
        if len(cluster_keys) > 4:
            raise ValueError("You can specify up to 4 columns for clustering.")

        supported_types = {
            "date", "timestamp", "timestampntz", "string",
            "integer", "long", "short", "float",
            "double", "decimal", "byte"
        }

        for col in cluster_keys:
            col_type = source_dataframe.schema[col].dataType.typeName().lower().replace("type", "")
            if col_type not in supported_types:
                raise TypeError(f"Column '{col}' has unsupported data type '{col_type}' for clustering.")

    # Get the current Spark session
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    # Check if the target table already exists
    if spark.catalog.tableExists(target_table):
        if overwrite:
            # Handle full overwrite mode
            if merge_keys:
                print("Warning: 'merge_keys' are ignored during an overwrite operation.")
            print(f"Overwriting table: {target_table}")

            if is_classic_df:
                # Overwrite the Delta table using classic Spark DataFrame API
                writer = source_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

                if cluster_keys:
                    try:
                        # Check current clustering columns in the Delta table
                        existing_cluster_keys = (
                            spark.sql(f"SHOW TBLPROPERTIES {target_table}")
                            .filter("key = 'clusteringColumns'")
                            .collect()
                        )
                        current_keys = (
                            existing_cluster_keys[0]["value"]
                            .replace('[', '').replace(']', '').replace('"', '').split(",")
                            if existing_cluster_keys else []
                        )
                        current_keys = [key.strip() for key in current_keys]
                        
                        # Only alter clustering if there's a change
                        if set(current_keys) != set(cluster_keys):
                            print(f"Updating clustering keys on {target_table} to: {', '.join(cluster_keys)}")
                            spark.sql(f"ALTER TABLE {target_table} CLUSTER BY({','.join(cluster_keys)})")
                        else:
                            print("No change in clustering keys; skipping ALTER TABLE.")
                    except Exception as e:
                        print(f"Warning: Failed to check or update clustering keys: {e}")

                    writer = writer.clusterBy(*cluster_keys)

                writer.saveAsTable(target_table)
                print(f"Table {target_table} overwritten successfully.")
            else:
                # Fallback for Spark Connect (no clustering control)
                print(f"Overwriting {target_table} using SQL (Spark Connect fallback)...")
                source_dataframe.createOrReplaceTempView("temp_table")
                spark.sql(f"CREATE OR REPLACE TABLE {target_table} AS SELECT * FROM temp_table")
                print(f"Table {target_table} overwritten (SQL).")
        else:
            # Merge mode: Update matching rows and insert new ones
            if cluster_keys:
                print("Warning: 'cluster_keys' are ignored during a merge operation.")

            if not merge_keys:
                raise ValueError("merge_keys must be provided for merge operation.")

            print(f"Merging into {target_table} using keys: {', '.join(merge_keys)}")

            merge_condition = ' AND '.join([f"t.{key} = s.{key}" for key in merge_keys])
            delta_table = DeltaTable.forName(spark, target_table)

            # Perform MERGE INTO operation using Delta Lake APIs
            delta_table.alias("t").merge(
                source_dataframe.alias("s"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()

            print(f"Table {target_table} merged successfully.")
    else:
        # Table doesn't exist: Create it
        print(f"Creating new table: {target_table}")

        if is_classic_df:
            writer = source_dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

            if cluster_keys:
                writer = writer.clusterBy(*cluster_keys)
                print(f"Clustering on: {', '.join(cluster_keys)}")

            writer.saveAsTable(target_table)
            print(f"Table {target_table} created successfully.")
        else:
            # Fallback for Spark Connect table creation
            print(f"Creating table {target_table} using SQL (Spark Connect fallback)...")
            source_dataframe.createOrReplaceTempView("temp_table")
            spark.sql(f"CREATE OR REPLACE TABLE {target_table} AS SELECT * FROM temp_table")
            print(f"Table {target_table} created successfully (SQL fallback).")

