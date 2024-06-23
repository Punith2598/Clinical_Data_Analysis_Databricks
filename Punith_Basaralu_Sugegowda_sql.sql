-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC def read_csv_file(file_path, delimiter, header=True):
-- MAGIC     # Start a Spark Session
-- MAGIC     spark = SparkSession.builder.getOrCreate()
-- MAGIC
-- MAGIC     if(file_path=="/FileStore/tables/clinicaltrial_2023.csv"):
-- MAGIC         from pyspark.sql.functions import split,  regexp_extract
-- MAGIC
-- MAGIC         # Read the CSV file as a single column
-- MAGIC         clinicaltrial_2023 = spark.read.text("/FileStore/tables/clinicaltrial_2023.csv")
-- MAGIC
-- MAGIC         # Skip the header row
-- MAGIC         clinicaltrial_2023 = clinicaltrial_2023.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0]).toDF()
-- MAGIC
-- MAGIC         # Split the single column into multiple columns
-- MAGIC         clinicaltrial_2023 = clinicaltrial_2023.select(split(clinicaltrial_2023["value"], "\t").alias("split_values"))
-- MAGIC
-- MAGIC         # Assign each split value to its respective column
-- MAGIC         clinicaltrial_2023 = clinicaltrial_2023.select(
-- MAGIC             clinicaltrial_2023["split_values"].getItem(0).alias("Id"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(1).alias("Study Title"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(2).alias("Acronym"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(3).alias("Status"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(4).alias("Conditions"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(5).alias("Interventions"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(6).alias("Sponsor"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(7).alias("Collaborators"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(8).cast("float").alias("Enrollment"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(9).alias("Funder Type"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(10).alias("Type"),
-- MAGIC             clinicaltrial_2023["split_values"].getItem(11).alias("Study Design"),
-- MAGIC             regexp_extract(clinicaltrial_2023["split_values"].getItem(12), r"\d{4}-\d{2}(-\d{2})?", 0).cast("date").alias("Start"),
-- MAGIC             regexp_extract(clinicaltrial_2023["split_values"].getItem(13), r"\d{4}-\d{2}(-\d{2})?", 0).cast("date").alias("Completion")
-- MAGIC         )
-- MAGIC
-- MAGIC         # Show the DataFrame
-- MAGIC         return clinicaltrial_2023
-- MAGIC         
-- MAGIC     else:
-- MAGIC         # Read the CSV file into a DataFrame
-- MAGIC         df = spark.read.option("header", header).option("delimiter", delimiter).csv(file_path)
-- MAGIC         return df
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define the file paths for each file
-- MAGIC file_paths = {
-- MAGIC     "clinicaltrial_2020": "/FileStore/tables/clinicaltrial_2020.csv",
-- MAGIC     "clinicaltrial_2021": "/FileStore/tables/clinicaltrial_2021.csv",
-- MAGIC     "clinicaltrial_2023": "/FileStore/tables/clinicaltrial_2023.csv",
-- MAGIC     "pharma": "/FileStore/tables/pharma.csv"
-- MAGIC }
-- MAGIC
-- MAGIC # Define the delimiters for each file
-- MAGIC delimiters = {
-- MAGIC     "clinicaltrial_2020": "|",
-- MAGIC     "clinicaltrial_2021": "|",
-- MAGIC     "clinicaltrial_2023": "/t",
-- MAGIC     "pharma": ","
-- MAGIC }
-- MAGIC
-- MAGIC # Process each file
-- MAGIC for file_name, file_path in file_paths.items():
-- MAGIC     print(f"Processing {file_name}...")
-- MAGIC     globals()[file_name] = read_csv_file(file_path, delimiters[file_name])
-- MAGIC     # Show the DataFrame
-- MAGIC     display(globals()[file_name])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import when
-- MAGIC
-- MAGIC def handle_missing_values(df):
-- MAGIC     # Check for missing values in each column before handling
-- MAGIC     print("Before handling missing values:")
-- MAGIC     for col_name, col_dtype in df.dtypes:
-- MAGIC         missing_count = df.filter(df[col_name].isNull() | (df[col_name] == '')).count()
-- MAGIC         print(f"Column '{col_name}': Type '{col_dtype}', Missing Values: {missing_count}")
-- MAGIC
-- MAGIC     # List of non-numeric columns
-- MAGIC     non_numeric_cols = [col_name for col_name, col_dtype in df.dtypes if col_dtype not in ('int','float', 'date')]
-- MAGIC
-- MAGIC     # Replace empty strings or null values with 'None' for non-numeric columns
-- MAGIC     for col_name in non_numeric_cols:
-- MAGIC        df = df.withColumn(col_name, when((df[col_name] == '') | df[col_name].isNull(), 'None').otherwise(df[col_name]))
-- MAGIC
-- MAGIC     # Check for missing values in each column after handling
-- MAGIC     print("\nAfter handling missing values:")
-- MAGIC     for col_name, col_dtype in df.dtypes:
-- MAGIC         missing_count = df.filter(df[col_name].isNull() | (df[col_name] == '')).count()
-- MAGIC         print(f"Column '{col_name}': Type '{col_dtype}', Missing Values: {missing_count}")
-- MAGIC     
-- MAGIC     return df
-- MAGIC # Applying the function to a dataframe
-- MAGIC # df = handle_missing_values(df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Apply the function to each dataframe
-- MAGIC clinicaltrial_2020 = handle_missing_values(clinicaltrial_2020)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2021 = handle_missing_values(clinicaltrial_2021)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2023 = handle_missing_values(clinicaltrial_2023)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma = handle_missing_values(pharma)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2020.createOrReplaceTempView("clinicaltrial_2020")
-- MAGIC clinicaltrial_2021.createOrReplaceTempView("clinicaltrial_2021")
-- MAGIC clinicaltrial_2023.createOrReplaceTempView("clinicaltrial_2023")
-- MAGIC pharma.createOrReplaceTempView("pharma")

-- COMMAND ----------

-- Using SQL query
SELECT COUNT(DISTINCT Id) AS TotalStudies_2020 FROM clinicaltrial_2020;

SELECT COUNT(DISTINCT Id) AS TotalStudies_2021 FROM clinicaltrial_2021;

SELECT COUNT(DISTINCT Id) AS TotalStudies_2023 FROM clinicaltrial_2023;


-- COMMAND ----------

-- Using SQL query
SELECT Type, COUNT(*) AS count
FROM clinicaltrial_2020
GROUP BY Type
ORDER BY count DESC;

SELECT Type, COUNT(*) AS count
FROM clinicaltrial_2021
GROUP BY Type
ORDER BY count DESC;

SELECT Type, COUNT(*) AS count
FROM clinicaltrial_2023
GROUP BY Type
ORDER BY count DESC;


-- COMMAND ----------

-- Using SQL query for clinicaltrial_2020
CREATE OR REPLACE TEMP VIEW conditions_2020 AS
SELECT condition_split
FROM clinicaltrial_2020
LATERAL VIEW explode(SPLIT(conditions, ',')) AS condition_split;

-- SQL query for clinicaltrial_2020
SELECT condition_split, COUNT(condition_split) AS count
FROM conditions_2020
WHERE condition_split != 'None'
GROUP BY condition_split
ORDER BY count DESC
LIMIT 5;

-- Using SQL query for clinicaltrial_2021
CREATE OR REPLACE TEMP VIEW conditions_2021 AS
SELECT condition_split
FROM clinicaltrial_2021
LATERAL VIEW explode(SPLIT(conditions, ',')) AS condition_split;

-- SQL query for clinicaltrial_2021
SELECT condition_split, COUNT(condition_split) AS count
FROM conditions_2021
WHERE condition_split != 'None'
GROUP BY condition_split
ORDER BY count DESC
LIMIT 5;

-- Using SQL query for clinicaltrial_2023
CREATE OR REPLACE TEMP VIEW conditions_2023 AS
SELECT condition_split
FROM clinicaltrial_2023
LATERAL VIEW explode(SPLIT(conditions, ',')) AS condition_split;

-- SQL query for clinicaltrial_2023
SELECT condition_split, COUNT(condition_split) AS count
FROM conditions_2023
WHERE condition_split != 'None'
GROUP BY condition_split
ORDER BY count DESC
LIMIT 5;


-- COMMAND ----------

-- Using SQL query for clinicaltrial_2020
SELECT Sponsor, COUNT(*) AS Trials
FROM clinicaltrial_2020
WHERE Sponsor NOT IN (SELECT DISTINCT Parent_Company FROM pharma)
GROUP BY Sponsor
ORDER BY Trials DESC
LIMIT 10;

-- Using SQL query for clinicaltrial_2021
SELECT Sponsor, COUNT(*) AS Trials
FROM clinicaltrial_2021
WHERE Sponsor NOT IN (SELECT DISTINCT Parent_Company FROM pharma)
GROUP BY Sponsor
ORDER BY Trials DESC
LIMIT 10;

-- Using SQL query for clinicaltrial_2023
SELECT Sponsor, COUNT(*) AS Trials
FROM clinicaltrial_2023
WHERE Sponsor NOT IN (SELECT DISTINCT Parent_Company FROM pharma)
GROUP BY Sponsor
ORDER BY Trials DESC
LIMIT 10;


-- COMMAND ----------

-- Using SQL query
    SELECT CASE 
               WHEN MONTH(Completion) = 1 THEN 'Jan'
               WHEN MONTH(Completion) = 2 THEN 'Feb'
               WHEN MONTH(Completion) = 3 THEN 'Mar'
               WHEN MONTH(Completion) = 4 THEN 'Apr'
               WHEN MONTH(Completion) = 5 THEN 'May'
               WHEN MONTH(Completion) = 6 THEN 'Jun'
               WHEN MONTH(Completion) = 7 THEN 'Jul'
               WHEN MONTH(Completion) = 8 THEN 'Aug'
               WHEN MONTH(Completion) = 9 THEN 'Sep'
               WHEN MONTH(Completion) = 10 THEN 'Oct'
               WHEN MONTH(Completion) = 11 THEN 'Nov'
               WHEN MONTH(Completion) = 12 THEN 'Dec'
           END AS Month,
           COUNT(*) AS Completed_Studies
    FROM clinicaltrial_2023
    WHERE YEAR(Completion) = 2023
    GROUP BY MONTH(Completion)
    ORDER BY MONTH(Completion)


