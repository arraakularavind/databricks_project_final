# Databricks notebook source
from pyspark.sql.functions import lit,col,when,split,explode
from pyspark.sql.types import IntegerType,FloatType,StringType,StructType,StructField
from io import BytesIO
from re import sub
from datetime import datetime
from json import dumps

import pyspark
import requests
import zipfile
import os

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Select Environment")
dbutils.widgets.text("env", "dev")
mode = dbutils.widgets.get("env")

# COMMAND ----------

logs=[]

def log_verify(env,status,cell,message,return_to_parent=False):
    log_data = {
        "timestamp": datetime.now().isoformat(),
        "env": env,
        "status": status,
        "cell":cell,
        "message": message
    }

    logs.append(log_data)
    
    if return_to_parent:
        dbutils.notebook.exit(dumps(logs))

# COMMAND ----------

# MAGIC %md
# MAGIC #Extraction

# COMMAND ----------

if mode=='dev':
    print(f"stage:{mode}")
    output_directory = "/Volumes/workspace/default/stackoverflow_year"
    os.makedirs(output_directory, exist_ok=True)

    for year in range(2020, 2025):
        try:
            url = f"https://survey.stackoverflow.co/datasets/stack-overflow-developer-survey-{year}.zip"
            response = requests.get(url)
            
            with zipfile.ZipFile(BytesIO(response.content)) as z:
                print("Files in ZIP:", z.namelist())
                log_verify(mode,"SUCCESS",4,"Extracted Files..")

                csv_file = "survey_results_public.csv"
                
                with z.open(csv_file) as f:
                    file_path = os.path.join(output_directory, f"survey_{year}.csv")
                    with open(file_path, 'wb') as file:
                        file.write(f.read())
                    
                        print(f"Saved File: {file_path} -> dataset")
                        log_verify(mode,"SUCCESS",4,"Dataset is read..")

                csv_file = "survey_results_schema.csv"
                
                with z.open(csv_file) as f:
                    file_path = os.path.join(output_directory, f"survey_schema_{year}.csv")
                    with open(file_path, 'wb') as file:
                        file.write(f.read())
                    
                        print(f"Saved File: {file_path} -> Schema")
                        log_verify(mode,"SUCCESS",4,"Schema Stored..")

        except Exception as e:
            print(f"An unexpected error occurred for year {year}: {e}")
            log_verify(mode,"FAILED",4,"File operation Failed..")

elif mode=='prod':
    print(f"stage:{mode}")
    output_directory = "/Volumes/workspace/default/stackoverflow_year"
    os.makedirs(output_directory, exist_ok=True)

    for year in range(2020, 2025):
        try:
            url = f"https://survey.stackoverflow.co/datasets/stack-overflow-developer-survey-{year}.zip"
            response = requests.get(url)
            
            with zipfile.ZipFile(BytesIO(response.content)) as z:
                log_verify(mode,"SUCCESS",4,"Extracted Files..")

                csv_file = "survey_results_public.csv"
                
                with z.open(csv_file) as f:
                    file_path = os.path.join(output_directory, f"survey_{year}.csv")
                    with open(file_path, 'wb') as file:
                        file.write(f.read())
                    
                        log_verify(mode,"SUCCESS",4,"Dataset is read..")

                csv_file = "survey_results_schema.csv"
                
                with z.open(csv_file) as f:
                    file_path = os.path.join(output_directory, f"survey_schema_{year}.csv")
                    with open(file_path, 'wb') as file:
                        file.write(f.read())
                    
                        log_verify(mode,"SUCCESS",4,"Schema Stored..")

        except Exception as e:
            log_verify(mode,"FAILED",4,"File operation Failed..")


# COMMAND ----------

target_field=['ResponseId','Age','Employment','EdLevel','Country','YearsCode','YearsCodePro','DevType','LanguageHaveWorkedWith','DatabaseHaveWorkedWith','WebframeHaveWorkedWith','SurveyYear']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected Fields 
# MAGIC
# MAGIC The expected schema for the dataset consists of the following fields:
# MAGIC
# MAGIC - **ResponseId**: `LongInteger` (Not Null)  
# MAGIC
# MAGIC - **Age**: `String` (Nullable)  
# MAGIC
# MAGIC - **Employment**: `String` (Nullable)  
# MAGIC
# MAGIC - **EdLevel**: `String` (Nullable)  
# MAGIC
# MAGIC - **Country**: `String` (Nullable)  
# MAGIC
# MAGIC - **YearsCode**: `String` (Nullable)  
# MAGIC
# MAGIC - **YearsCodePro**: `String` (Nullable)  
# MAGIC
# MAGIC - **DevType**: `String` (Nullable)  
# MAGIC
# MAGIC - **LanguageHaveWorkedWith**: `String` (Nullable)  
# MAGIC
# MAGIC - **DatabaseHaveWorkedWith**: `String` (Nullable)  
# MAGIC
# MAGIC - **WebframeHaveWorkedWith**: `String` (Nullable)  
# MAGIC    
# MAGIC - **SurveyYear**: `String` (Nullable)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformed Schema
# MAGIC
# MAGIC The expected schema for the dataset consists of the following fields:
# MAGIC
# MAGIC - **SurveyYear**: `String` (Not Nullable)  
# MAGIC
# MAGIC - **Country**: `String` (Not Null)  
# MAGIC
# MAGIC - **Age**: `String` (Not Null)  
# MAGIC
# MAGIC - **Language**: `String` (Not Nullable)  
# MAGIC
# MAGIC - **Lang_count**: `Integer` (Not Nullable)  
# MAGIC
# MAGIC - **Webframe**: `String` (Not Nullable)  
# MAGIC
# MAGIC - **Webframe_count**: `Integer` (Not Nullable)  
# MAGIC
# MAGIC - **Database**: `String` (Not Nullable)  
# MAGIC
# MAGIC - **Database_count**: `Integer` (Not Nullable)  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Extract Data According to Expected Field.

# COMMAND ----------

if mode=='dev':
    schema_columns_by_year={}

    for year in range(2020, 2025):
        df_raw = spark.read.csv(output_directory+f"/survey_{year}.csv",header=True)
        schema_columns_by_year[year]=df_raw.columns

    all_columns=set().union(*schema_columns_by_year.values())

    print(f"{'Column Name':40}", end="")
    for year in range(2020,2025):
        print(f"{year}", end="\t")

    print()

    for column in sorted(all_columns):
        print(f"{column:40}", end="")
        for year in range(2020,2025):
            mark = "Y" if column in schema_columns_by_year[year] else "N/A"
            print(f"{mark}\t", end="")
        print()
    
    log_verify(mode,"SUCCESS",9,"Year wise Schema compare completed..")

elif mode=='prod':
    log_verify(mode,"SUCCESS",9,"Year wise Schema compare completed..")


# COMMAND ----------

#Expected Field

expected_schema = StructType([
    StructField("ResponseId",IntegerType(),False),
    StructField("Age", StringType(), True),
    StructField("Employment", StringType(), True),
    StructField("EdLevel", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("YearsCode", StringType(), True),
    StructField("YearsCodePro", StringType(), True),
    StructField("DevType", StringType(), True),
    StructField("LanguageHaveWorkedWith", StringType(), True),
    StructField("DatabaseHaveWorkedWith", StringType(), True),
    StructField("WebframeHaveWorkedWith", StringType(), True),
    StructField("SurveyYear",StringType(),False)
])


# COMMAND ----------

def convert_to_expected_schema(df,expected_schema):
    data=[tuple(row) for row in df.collect()]
    return spark.createDataFrame(data,schema=expected_schema)

# COMMAND ----------


if mode=='dev':

    print(f"stage:{mode}")
    df_2020 = spark.read.csv(output_directory+"/survey_2020.csv",header=True,inferSchema=True)

    print(f"Number of Rows {df_2020.count()}")
    print(f"Number of Columns {len(df_2020.columns)}")
    df_2020.printSchema()

    log_verify(mode,"SUCCESS",12,"Survey 2020 Raw data..")

    expected_field=[field.name for field in expected_schema]

    df_2020 = df_2020.withColumnRenamed("Respondent","ResponseId")\
                        .withColumnRenamed("LanguageWorkedWith","LanguageHaveWorkedWith")\
                        .withColumnRenamed("WebframeWorkedWith","WebframeHaveWorkedWith")\
                        .withColumnRenamed("DatabaseWorkedWith","DatabaseHaveWorkedWith")
                        
    df_2020 = df_2020.withColumn("SurveyYear",lit("2020")).select(*expected_field)
    extracted_df_2020 = convert_to_expected_schema(df_2020,expected_schema)

    display(extracted_df_2020)
    extracted_df_2020.printSchema()

    log_verify(mode,"SUCCESS",12,"Sruvey 2020 Extracted data Prepared..")

elif mode=='prod':

    print(f"stage:{mode}")
    df_2020 = spark.read.csv(output_directory+"/survey_2020.csv",header=True,inferSchema=True)
    log_verify(mode,"SUCCESS",12,"Survey 2020 Raw data..")

    expected_field=[field.name for field in expected_schema]

    df_2020 = df_2020.withColumnRenamed("Respondent","ResponseId")\
                        .withColumnRenamed("LanguageWorkedWith","LanguageHaveWorkedWith")\
                        .withColumnRenamed("WebframeWorkedWith","WebframeHaveWorkedWith")\
                        .withColumnRenamed("DatabaseWorkedWith","DatabaseHaveWorkedWith")
                        
    df_2020 = df_2020.withColumn("SurveyYear",lit("2020")).select(*expected_field)
    extracted_df_2020 = convert_to_expected_schema(df_2020,expected_schema)
    log_verify(mode,"SUCCESS",12,"Sruvey 2020 Extracted data Prepared..")
    


# COMMAND ----------

if mode=='dev':

    print(f"stage:{mode}")
    df_2021 = spark.read.csv(output_directory+"/survey_2021.csv",header=True,inferSchema=True)
    print(f"Number of Rows {df_2021.count()}")
    print(f"Number of Columns {len(df_2021.columns)}")
    df_2021.printSchema()

    log_verify(mode,"SUCCESS",13,"Sruvey 2021 Raw data Prepared..")

    expected_field=[column.name for column in expected_schema]

    df_2021 = df_2021.withColumn("SurveyYear",lit("2021")).select(*expected_field)
    extracted_df_2021 = convert_to_expected_schema(df_2021,expected_schema)

    display(extracted_df_2021)
    extracted_df_2021.printSchema()
        
    log_verify(mode,"SUCCESS",13,"Sruvey 2021 Extracted data Prepared..")

elif mode=='prod':

    print(f"stage:{mode}")
    df_2021 = spark.read.csv(output_directory+"/survey_2021.csv",header=True,inferSchema=True)
    log_verify(mode,"SUCCESS",13,"Sruvey 2021 Raw data Prepared..")

    expected_field=[column.name for column in expected_schema]

    df_2021 = df_2021.withColumn("SurveyYear",lit("2021")).select(*expected_field)
    extracted_df_2021 = convert_to_expected_schema(df_2021,expected_schema)    
    log_verify(mode,"SUCCESS",13,"Sruvey 2021 Extracted data Prepared..")



# COMMAND ----------


if mode=='dev':

    print(f"stage:{mode}")
    df_2022 = spark.read.csv(output_directory+"/survey_2022.csv",header=True,inferSchema=True)

    print(f"Number of Rows {df_2022.count()}")
    print(f"Number of Columns {len(df_2022.columns)}")
    df_2022.printSchema()

    log_verify(mode,"SUCCESS",14,"Sruvey 2022 Raw data Prepared..")

    expected_field=[column.name for column in expected_schema]

    df_2022 = df_2022.withColumn("SurveyYear",lit("2022")).select(*expected_field)
    extracted_df_2022 = convert_to_expected_schema(df_2022,expected_schema)

    display(extracted_df_2022)
    extracted_df_2022.printSchema()
        
    log_verify(mode,"SUCCESS",14,"Sruvey 2022 Extracted data Prepared..")

elif mode=='prod':

    print(f"stage:{mode}")
    df_2022 = spark.read.csv(output_directory+"/survey_2022.csv",header=True,inferSchema=True)
    log_verify(mode,"SUCCESS",14,"Sruvey 2022 Raw data Prepared..")

    expected_field=[column.name for column in expected_schema]

    df_2022 = df_2022.withColumn("SurveyYear",lit("2022")).select(*expected_field)
    extracted_df_2022 = convert_to_expected_schema(df_2022,expected_schema)
    
    log_verify(mode,"SUCCESS",14,"Sruvey 2022 Extracted data Prepared..")




# COMMAND ----------

if mode=='dev':
    
    print(f"stage:{mode}")
    df_2023 = spark.read.csv(output_directory+"/survey_2023.csv",header=True,inferSchema=True)
    print(f"Number of Rows {df_2023.count()}")
    print(f"Number of Columns {len(df_2023.columns)}")
    df_2023.printSchema()

    log_verify(mode,"SUCCESS",15,"Sruvey 2023 Raw data Prepared..")


    expected_field=[column.name for column in expected_schema]

    df_2023 = df_2023.withColumn("SurveyYear",lit("2023")).select(*expected_field)
    extracted_df_2023 = convert_to_expected_schema(df_2023,expected_schema)

    
    display(extracted_df_2023)
    extracted_df_2023.printSchema()

    log_verify(mode,"SUCCESS",15,"Sruvey 2023 Extracted data Prepared..")

elif mode=='prod':

    print(f"stage:{mode}")
    df_2023 = spark.read.csv(output_directory+"/survey_2023.csv",header=True,inferSchema=True)
    log_verify(mode,"SUCCESS",15,"Sruvey 2023 Raw data Prepared..")


    expected_field=[column.name for column in expected_schema]

    df_2023 = df_2023.withColumn("SurveyYear",lit("2023")).select(*expected_field)
    extracted_df_2023 = convert_to_expected_schema(df_2023,expected_schema)

    log_verify(mode,"SUCCESS",15,"Sruvey 2023 Extracted data Prepared..")




# COMMAND ----------

if mode=='dev':

    print(f"stage:{mode}")
    df_2024 = spark.read.csv(output_directory+"/survey_2024.csv",header=True,inferSchema=True)
    print(f"Number of Rows {df_2024.count()}")
    print(f"Number of Columns {len(df_2024.columns)}")
    df_2024.printSchema()
    
    log_verify(mode,"SUCCESS",16,"Sruvey 2024 Raw data Prepared..")


    expected_field=[column.name for column in expected_schema]

    df_2024 = df_2024.withColumn("SurveyYear",lit("2024")).select(*expected_field)
    extracted_df_2024 = convert_to_expected_schema(df_2024,expected_schema)

    
    display(extracted_df_2024)
    extracted_df_2024.printSchema()
        
    log_verify(mode,"SUCCESS",16,"Sruvey 2024 Extracted data Prepared..")

elif mode=='prod':

    print(f"stage:{mode}")
    df_2024 = spark.read.csv(output_directory+"/survey_2024.csv",header=True,inferSchema=True)
    
    log_verify(mode,"SUCCESS",16,"Sruvey 2024 Raw data Prepared..")
    
    expected_field=[column.name for column in expected_schema]

    df_2024 = df_2024.withColumn("SurveyYear",lit("2024")).select(*expected_field)
    extracted_df_2024 = convert_to_expected_schema(df_2024,expected_schema)

    log_verify(mode,"SUCCESS",16,"Sruvey 2024 Extracted data Prepared..")



# COMMAND ----------

# MAGIC %md
# MAGIC #Transformation Part

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Merge into Single dataFrame

# COMMAND ----------

if mode=='dev':

    print(f"stage:{mode}")
    merged_df=extracted_df_2020.union(extracted_df_2021)\
        .union(extracted_df_2022)\
        .union(extracted_df_2023)\
        .union(extracted_df_2024)

    print(merged_df.count())
    display(merged_df)
        
    log_verify(mode,"SUCCESS",19,"All Year survey dataset merged..")

elif mode=='prod':

    print(f"stage:{mode}")
    merged_df=extracted_df_2020.union(extracted_df_2021)\
        .union(extracted_df_2022)\
        .union(extracted_df_2023)\
        .union(extracted_df_2024)

    log_verify(mode,"SUCCESS",19,"All Year survey dataset merged..")



# COMMAND ----------

if mode=='dev':
    
    print(f"stage:{mode}")
    distinct_ages_df = merged_df.select(col("SurveyYear"), "YearsCode").distinct()
    display(distinct_ages_df)
    
    log_verify(mode,"SUCCESS",20,"Distinct Check..")

elif mode=='prod':
    log_verify(mode,"SUCCESS",20,"Distinct Check..")


# COMMAND ----------

#Both YearsCode and YearsCodePro can be binned converting them as Category.

#Convert YearsCode to Float Type and Bin them into category

if mode=='dev':

    print(f"stage:{mode}")
    merged_df = merged_df.withColumn(
        "YearsCodeNumeric",
        when(col("YearsCode").isin("NA",""),None)
        .when(col("YearsCode")=='Less than 1 years',0.5)
        .when(col("YearsCode")=='More than 50 years',51.0)
        .when(col("YearsCode").rlike("^[0-9.]+(\\.[0-9]+)?$"), col("YearsCode").cast(FloatType()))
        .otherwise(None)
    )

    merged_df = merged_df.withColumn(
        "YearsCode",
        when(col("YearsCodeNumeric").isNull(),None)
        .when(col("YearsCodeNumeric").between(0,1), "Less than 1 year")
        .when(col("YearsCodeNumeric").between(1,10), "1-10 years")
        .when(col("YearsCodeNumeric").between(11,20), "11-20 years")
        .when(col("YearsCodeNumeric").between(21,30), "21-30 years")
        .when(col("YearsCodeNumeric").between(31,40), "31-40 years")
        .when(col("YearsCodeNumeric").between(41,50), "41-50 years")
        .otherwise("More than 50 years")
        .cast(StringType())
    )

    #Convert YearsCodePro to Float Type and Bin them into category
    merged_df = merged_df.withColumn(
        "YearsCodeNumeric",
        when(col("YearsCodePro").isin("NA",""),None)
        .when(col("YearsCodePro")=='Less than 1 years',0.5)
        .when(col("YearsCodePro")=='More than 50 years',51.0)
        .when(col("YearsCodePro").rlike("^[0-9.]+(\\.[0-9]+)?$"), col("YearsCodePro").cast(FloatType()))
        .otherwise(None)
    )

    merged_df = merged_df.withColumn(
        "YearsCodePro",
        when(col("YearsCodeNumeric").isNull(),None)
        .when(col("YearsCodeNumeric").between(0,1), "Less than 1 year")
        .when(col("YearsCodeNumeric").between(1,10), "1-10 years")
        .when(col("YearsCodeNumeric").between(11,20), "11-20 years")
        .when(col("YearsCodeNumeric").between(21,30), "21-30 years")
        .when(col("YearsCodeNumeric").between(31,40), "31-40 years")
        .when(col("YearsCodeNumeric").between(41,50), "41-50 years")
        .otherwise("More than 50 years")
        .cast(StringType())
    )

    merged_df = merged_df.drop("YearsCodeNumeric")

    display(merged_df)
    log_verify(mode,"SUCCESS",21,"YearsCode and YearsCodePro Conversion Completed..")

elif mode=='prod':

    print(f"stage:{mode}")
    merged_df = merged_df.withColumn(
        "YearsCodeNumeric",
        when(col("YearsCode").isin("NA",""),None)
        .when(col("YearsCode")=='Less than 1 years',0.5)
        .when(col("YearsCode")=='More than 50 years',51.0)
        .when(col("YearsCode").rlike("^[0-9.]+(\\.[0-9]+)?$"), col("YearsCode").cast(FloatType()))
        .otherwise(None)
    )

    merged_df = merged_df.withColumn(
        "YearsCode",
        when(col("YearsCodeNumeric").isNull(),None)
        .when(col("YearsCodeNumeric").between(0,1), "Less than 1 year")
        .when(col("YearsCodeNumeric").between(1,10), "1-10 years")
        .when(col("YearsCodeNumeric").between(11,20), "11-20 years")
        .when(col("YearsCodeNumeric").between(21,30), "21-30 years")
        .when(col("YearsCodeNumeric").between(31,40), "31-40 years")
        .when(col("YearsCodeNumeric").between(41,50), "41-50 years")
        .otherwise("More than 50 years")
        .cast(StringType())
    )

    #Convert YearsCodePro to Float Type and Bin them into category
    merged_df = merged_df.withColumn(
        "YearsCodeNumeric",
        when(col("YearsCodePro").isin("NA",""),None)
        .when(col("YearsCodePro")=='Less than 1 years',0.5)
        .when(col("YearsCodePro")=='More than 50 years',51.0)
        .when(col("YearsCodePro").rlike("^[0-9.]+(\\.[0-9]+)?$"), col("YearsCodePro").cast(FloatType()))
        .otherwise(None)
    )

    merged_df = merged_df.withColumn(
        "YearsCodePro",
        when(col("YearsCodeNumeric").isNull(),None)
        .when(col("YearsCodeNumeric").between(0,1), "Less than 1 year")
        .when(col("YearsCodeNumeric").between(1,10), "1-10 years")
        .when(col("YearsCodeNumeric").between(11,20), "11-20 years")
        .when(col("YearsCodeNumeric").between(21,30), "21-30 years")
        .when(col("YearsCodeNumeric").between(31,40), "31-40 years")
        .when(col("YearsCodeNumeric").between(41,50), "41-50 years")
        .otherwise("More than 50 years")
        .cast(StringType())
    )

    merged_df = merged_df.drop("YearsCodeNumeric")
    log_verify(mode,"SUCCESS",21,"YearsCode and YearsCodePro Conversion Completed..")

    

# COMMAND ----------

#Convert the Age Column of SurveyYear 2020 to FloatType and bin them to Category.

if mode=='dev':

    print(f"stage:{mode}")
    merged_df = merged_df.withColumn(
        "Age_Numeric",
        when((col("Age").rlike("^[0-9]+(\\.[0-9]+)?$")),col("Age").cast(FloatType()))
        .when(col("Age").isin("NA",""),None)
        .otherwise(None)
    )

    merged_df = merged_df.withColumn(
        "Age",
        when((col("SurveyYear") == "2020") & (col("Age_Numeric").isNull()),None)
        .when((col("SurveyYear") == "2020") & (col("Age_Numeric") < 18), "Under 18 years old")
        .when((col("SurveyYear") == "2020") & col("Age_Numeric").between(18, 24.5), "18-24 years old")
        .when((col("SurveyYear") == "2020") & col("Age_Numeric").between(25, 34.5), "25-34 years old")
        .when((col("SurveyYear") == "2020") & col("Age_Numeric").between(35, 44), "35-44 years old")
        .when((col("SurveyYear") == "2020") & col("Age_Numeric").between(45, 54), "45-54 years old")
        .when((col("SurveyYear") == "2020") & col("Age_Numeric").between(55, 64), "55-64 years old")
        .when((col("SurveyYear") == "2020") & (col("Age_Numeric") >= 65), "65 years or older")
        .otherwise(col("Age"))

    )

    merged_df = merged_df.drop("Age_Numeric")

    display(merged_df)
    log_verify(mode,"SUCCESS",22,"Age transformation completed..")

elif mode=='prod':
    
    print(f"stage:{mode}")
    merged_df = merged_df.withColumn(
        "Age_Numeric",
        when((col("Age").rlike("^[0-9]+(\\.[0-9]+)?$")),col("Age").cast(FloatType()))
        .when(col("Age").isin("NA",""),None)
        .otherwise(None)
    )

    merged_df = merged_df.withColumn(
        "Age",
        when((col("SurveyYear") == "2020") & (col("Age_Numeric").isNull()),None)
        .when((col("SurveyYear") == "2020") & (col("Age_Numeric") < 18), "Under 18 years old")
        .when((col("SurveyYear") == "2020") & col("Age_Numeric").between(18, 24.5), "18-24 years old")
        .when((col("SurveyYear") == "2020") & col("Age_Numeric").between(25, 34.5), "25-34 years old")
        .when((col("SurveyYear") == "2020") & col("Age_Numeric").between(35, 44), "35-44 years old")
        .when((col("SurveyYear") == "2020") & col("Age_Numeric").between(45, 54), "45-54 years old")
        .when((col("SurveyYear") == "2020") & col("Age_Numeric").between(55, 64), "55-64 years old")
        .when((col("SurveyYear") == "2020") & (col("Age_Numeric") >= 65), "65 years or older")
        .otherwise(col("Age"))

    )

    merged_df = merged_df.drop("Age_Numeric")
    log_verify(mode,"SUCCESS",22,"Age transformation completed..")





# COMMAND ----------

if mode=='dev': 

        print(f"stage:{mode}")   
        for column in ["DevType",
                "LanguageHaveWorkedWith",
                "DatabaseHaveWorkedWith",
                "WebframeHaveWorkedWith",
                "EdLevel",
                "Employment"
                ]:
                merged_df = merged_df.withColumn(column,
                                        when(col(column)=="NA",None)
                                        .otherwise(col(column)))
                display(merged_df)
                
                log_verify(mode,"SUCCESS",23,"Null Conversion Completed..")

elif mode=='prod':
        
        print(f"stage:{mode}")
        for column in ["DevType",
               "LanguageHaveWorkedWith",
               "DatabaseHaveWorkedWith",
               "WebframeHaveWorkedWith",
               "EdLevel",
               "Employment"
            ]:
                merged_df = merged_df.withColumn(column,
                                    when(col(column)=="NA",None)
                                    .otherwise(col(column)))    
                log_verify(mode,"SUCCESS",23,"Null Conversion Completed..")

# COMMAND ----------

#Handling Multivalued Attributes in Merged_df

if mode=='dev':

    print(f"stage:{mode}")
    primary_lang = merged_df.withColumn("DevType",split(col("DevType"), ";").getItem(0))\
                        .withColumn("LanguageHaveWorkedWith",split(col("LanguageHaveWorkedWith"), ";").getItem(0))\
                        .withColumn("DatabaseHaveWorkedWith",split(col("DatabaseHaveWorkedWith"), ";").getItem(0))\
                        .withColumn("WebframeHaveWorkedWith",split(col("WebframeHaveWorkedWith"), ";").getItem(0))


    display(primary_lang)
        
    log_verify(mode,"SUCCESS",24,"Primary Language .getItem completed..")

elif mode=='prod':

    print(f"stage:{mode}")
    primary_lang = merged_df.withColumn("DevType",split(col("DevType"), ";").getItem(0))\
                        .withColumn("LanguageHaveWorkedWith",split(col("LanguageHaveWorkedWith"), ";").getItem(0))\
                        .withColumn("DatabaseHaveWorkedWith",split(col("DatabaseHaveWorkedWith"), ";").getItem(0))\
                        .withColumn("WebframeHaveWorkedWith",split(col("WebframeHaveWorkedWith"), ";").getItem(0))
  
    log_verify(mode,"SUCCESS",24,"Primary Language .getItem completed..")

                

# COMMAND ----------

if mode=='dev':
    print(f"stage:{mode}")
    explode_lang = merged_df.withColumn("DevType",explode(split(col("DevType"), ";")))\
                        .withColumn("LanguageHaveWorkedWith",explode(split(col("LanguageHaveWorkedWith"), ";")))\
                        .withColumn("DatabaseHaveWorkedWith",explode(split(col("DatabaseHaveWorkedWith"), ";")))\
                        .withColumn("WebframeHaveWorkedWith",explode(split(col("WebframeHaveWorkedWith"), ";")))

    display(explode_lang)
        
    log_verify(mode,"SUCCESS",25,"Explode operation completed...")

elif mode=='prod':
    print(f"stage:{mode}")
    explode_lang = merged_df.withColumn("DevType",explode(split(col("DevType"), ";")))\
                        .withColumn("LanguageHaveWorkedWith",explode(split(col("LanguageHaveWorkedWith"), ";")))\
                        .withColumn("DatabaseHaveWorkedWith",explode(split(col("DatabaseHaveWorkedWith"), ";")))\
                        .withColumn("WebframeHaveWorkedWith",explode(split(col("WebframeHaveWorkedWith"), ";")))

    log_verify(mode,"SUCCESS",25,"Explode operation completed...")



# COMMAND ----------

if mode=='dev':
    print(f"stage:{mode}")
    explode_lang.createOrReplaceTempView("Explode_data")
    explode_df = spark.sql("""
                    SELECT SurveyYear,Country,Age,LanguageHaveWorkedWith,
                    COUNT(DISTINCT CONCAT(SurveyYear, '_', ResponseId)) AS unique_language_users,
                    DatabaseHaveWorkedWith,
                    COUNT(DISTINCT CONCAT(SurveyYear, '_', ResponseId)) AS unique_database_users
                    from Explode_data
                    GROUP BY ALL
                    ORDER BY unique_language_users,unique_database_users DESC
    """)

    display(explode_df)
        
    log_verify(mode,"SUCCESS",26,"SQL operation on Explode Data completed..")

elif mode=='prod':
    print(f"stage:{mode}")
    explode_lang.createOrReplaceTempView("Explode_data")
    explode_df = spark.sql("""
                    SELECT SurveyYear,Country,Age,LanguageHaveWorkedWith,
                    COUNT(DISTINCT CONCAT(SurveyYear, '_', ResponseId)) AS unique_language_users,
                    DatabaseHaveWorkedWith,
                    COUNT(DISTINCT CONCAT(SurveyYear, '_', ResponseId)) AS unique_database_users
                    from Explode_data
                    GROUP BY ALL
                    ORDER BY unique_language_users,unique_database_users DESC
    """)
    
    log_verify(mode,"SUCCESS",26,"SQL operation on Explode Data completed..")



# COMMAND ----------

if mode=='dev':
    print(f"stage:{mode}")
    primary_lang.createOrReplaceTempView("Primary_lang_data")
    primary_df = spark.sql("""
        SELECT DISTINCT
            SurveyYear,
            Country,
            Age,
            LanguageHaveWorkedWith,
            count(*) OVER (PARTITION BY SurveyYear,Country,Age,LanguageHaveWorkedWith ) AS LANG_COUNT,
            WebframeHaveWorkedWith,
            count(*) OVER (PARTITION BY SurveyYear,Country,Age,webframeHaveWorkedWith ) AS WEB_COUNT,
            DatabaseHaveWorkedWith,
            count(*) OVER (PARTITION BY SurveyYear,Country,Age,DatabaseHaveWorkedWith ) AS DB_COUNT
        FROM 
            Primary_lang_data
        WHERE 
            Age IS NOT NULL 
            AND LanguageHaveWorkedWith IS NOT NULL 
            AND WebframeHaveWorkedWith IS NOT NULL
            AND DatabaseHaveWorkedWith IS NOT NULL
        GROUP BY ALL
        ORDER BY ALL
    """)
    display(primary_df)
        
    log_verify(mode,"SUCCESS",27,"SQL operation on Primary Lang Data completed..")

elif mode=='prod':
    print(f"stage:{mode}")
    primary_lang.createOrReplaceTempView("Primary_lang_data")
    primary_df = spark.sql("""
        SELECT DISTINCT
            SurveyYear,
            Country,
            Age,
            LanguageHaveWorkedWith,
            count(*) OVER (PARTITION BY SurveyYear,Country,Age,LanguageHaveWorkedWith ) AS LANG_COUNT,
            WebframeHaveWorkedWith,
            count(*) OVER (PARTITION BY SurveyYear,Country,Age,webframeHaveWorkedWith ) AS WEB_COUNT,
            DatabaseHaveWorkedWith,
            count(*) OVER (PARTITION BY SurveyYear,Country,Age,DatabaseHaveWorkedWith ) AS DB_COUNT
        FROM 
            Primary_lang_data
        WHERE 
            Age IS NOT NULL 
            AND LanguageHaveWorkedWith IS NOT NULL 
            AND WebframeHaveWorkedWith IS NOT NULL
            AND DatabaseHaveWorkedWith IS NOT NULL
        GROUP BY ALL
        ORDER BY ALL
    """)
   
    log_verify(mode,"SUCCESS",27,"SQL operation on Primary Lang Data completed..")

# COMMAND ----------

# MAGIC %md
# MAGIC #Load

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS Primary_language_data")
primary_df.write.mode("overwrite").saveAsTable("Primary_language_data")

log_verify(mode,"SUCCESS",29,"Primary_language_data into table..")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS explode_language_data")
explode_df.write.mode("overwrite").saveAsTable("explode_language_data")

log_verify(mode,"SUCCESS",30,"explode_language_data into table..")

# COMMAND ----------

log_verify(mode, "SUCCESS", 31, "ETL Process completed", return_to_parent=True)