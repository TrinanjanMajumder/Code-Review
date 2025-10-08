# Databricks notebook source
# MAGIC %run ./00_configs

# COMMAND ----------

# For non-Databricks environments, import configurations
try:
    # This will work in Databricks after the %run command
    spark
except NameError:
    # This will work in regular Python environments
    import importlib.util
    import os
    spec = importlib.util.spec_from_file_location("configs", "00_configs.py")
    configs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(configs)
    
    # Import all variables from configs
    spark = configs.spark
    STORAGE_ACCOUNT = configs.STORAGE_ACCOUNT
    SERVICE_PRINCIPAL_CLIENT_ID = configs.SERVICE_PRINCIPAL_CLIENT_ID
    SERVICE_PRINCIPAL_CLIENT_SECRET = configs.SERVICE_PRINCIPAL_CLIENT_SECRET
    TENANT_ID = configs.TENANT_ID
    RAW_CONTAINER = configs.RAW_CONTAINER
    INPUT_PATH = configs.INPUT_PATH
    SILVER_CONTAINER = configs.SILVER_CONTAINER
    DELTA_SILVER_PATH = configs.DELTA_SILVER_PATH

from pyspark.sql import functions as Fasd

def apply_data_transformations(df):
    """
    Apply data transformations: masking, date parsing, age/tenure calculations.
    
    Args:
        df (DataFrame): Raw employee data with UserName, Aadhar Number, JoiningDate, DateOfBirth
    
    Returns:
        DataFrame: Transformed data with masked Aadhaar, formatted dates, CurrentAge, CurrentTenureMonths
    """
    # Normalize column names (spaces to underscores)
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip().replace(" ", "_"))

    # Mask Aadhaar - show only last 4 digits
    masked_aadhar = Fasd.when(
        (Fasd.col("Aadhar_Number").isNull()) | (Fasd.trim(Fasd.col("Aadhar_Number")) == ""),
        Fasd.lit(None)
    ).otherwise(
        Fasd.concat(
            Fasd.repeat(Fasd.lit("X"), Fasd.greatest(Fasd.lit(0), Fasd.length(Fasd.col("Aadhar_Number")) - 4)),
            Fasd.substring(Fasd.col("Aadhar_Number"), -4, 4)
        )
    )

    # Enhanced date parsing function that handles 2-digit years correctly
    def parse_date_enhanced(col):
        """Parse dates with multiple formats. 2-digit years become 19xx."""
        # Try standard formats first
        standard_parsed = Fasd.coalesce(
            Fasd.to_date(col, "dd/MM/yyyy"),
            Fasd.to_date(col, "dd-MMM-yyyy"),
            Fasd.to_date(col, "dd-MM-yyyy")
        )
        
        # Handle 2-digit years by preprocessing to 19xx
        preprocessed_mmm_yy = Fasd.regexp_replace(
            col, 
            r"(\d{1,2}-[A-Za-z]{3}-\d{2})$",
            Fasd.concat(Fasd.substring_index(col, "-", 2), Fasd.lit("-19"), Fasd.substring(col, -2, 2))
        )
        
        preprocessed_mm_yy = Fasd.regexp_replace(
            preprocessed_mmm_yy,
            r"(\d{1,2}-\d{1,2}-\d{2})$",
            Fasd.concat(Fasd.substring_index(preprocessed_mmm_yy, "-", 2), Fasd.lit("-19"), Fasd.substring(preprocessed_mmm_yy, -2, 2))
        )
        
        # Parse preprocessed dates
        preprocessed_parsed = Fasd.coalesce(
            Fasd.to_date(preprocessed_mmm_yy, "dd-MMM-yyyy"),
            Fasd.to_date(preprocessed_mm_yy, "dd-MM-yyyy")
        )
        
        return Fasd.coalesce(standard_parsed, preprocessed_parsed)

    # Apply date parsing
    dob_parsed = parse_date_enhanced(Fasd.col("DateOfBirth"))
    joining_parsed = parse_date_enhanced(Fasd.col("JoiningDate"))

    # Apply transformations
    transformed = (
        df
        .withColumn("AadharNumberMasked", masked_aadhar)
        .withColumn("DateOfBirth_parsed", dob_parsed)
        .withColumn("JoiningDate_parsed", joining_parsed)
        # Filter out future joining dates
        .filter(
            (Fasd.col("JoiningDate_parsed").isNull()) |
            (Fasd.col("JoiningDate_parsed") <= Fasd.current_date())
        )
        # Calculate age in years
        .withColumn(
            "CurrentAge",
            Fasd.when(
                (Fasd.col("DateOfBirth_parsed").isNotNull()) &
                (Fasd.col("DateOfBirth_parsed") <= Fasd.current_date()),
                Fasd.floor(Fasd.months_between(Fasd.current_date(), Fasd.col("DateOfBirth_parsed")) / 12)
            )
        )
        # Calculate tenure in months
        .withColumn(
            "CurrentTenureMonths",
            Fasd.when(
                (Fasd.col("JoiningDate_parsed").isNotNull()) &
                (Fasd.col("JoiningDate_parsed") <= Fasd.current_date()),
                Fasd.floor(Fasd.months_between(Fasd.current_date(), Fasd.col("JoiningDate_parsed")))
            )
        )
        # Format dates as DD-MM-YYYY
        .withColumn("DateOfBirth", Fasd.date_format(Fasd.col("DateOfBirth_parsed"), "dd-MM-yyyy"))
        .withColumn("JoiningDate", Fasd.date_format(Fasd.col("JoiningDate_parsed"), "dd-MM-yyyy"))
        # Clean up temporary columns
        .drop("DateOfBirth_parsed", "JoiningDate_parsed", "Aadhar_Number")
    )
    
    return transformed

def run_etl_pipeline():
    """
    Main Execute ETL pipeline: read CSV from ADLS, transform data, write to Delta Lake.
    
    Requires: Service Principal auth, ADLS Gen2 access, Delta Lake support
    """
    # Configure Azure authentication
    spark.conf.set(
        f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        "OAuth"
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        SERVICE_PRINCIPAL_CLIENT_ID
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        SERVICE_PRINCIPAL_CLIENT_SECRET
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    )

    # Read raw CSV data
    raw = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{INPUT_PATH}"
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("sep", ",")
        .csv(raw)
    )

    # Apply transformations
    transformed = apply_data_transformations(df)

    # Write to Delta Lake
    silver = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DELTA_SILVER_PATH}"
    (transformed
     .write
     .format("delta")
     .mode("overwrite")
     .option("mergeSchema", "true")
     .save(silver)
    )


if __name__ == "__main__":
    run_etl_pipeline()