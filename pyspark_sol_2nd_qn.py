from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat, lit

def anonymize_data(df):
    """Anonymizes first_name, last_name, and address fields."""
    return df.withColumn("first_name", sha2(concat("first_name", lit("salt")), 256)) \
             .withColumn("last_name", sha2(concat("last_name", lit("salt")), 256)) \
             .withColumn("address", sha2(concat("address", lit("salt")), 256))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataAnonymization").getOrCreate()

    # Load the data (replace with your actual data source)
    data = spark.read.csv('C:\Venu\PERSONAL\MATERIALS\DEMYST\sample_names.csv', header=True)

    # Anonymize the data
    anonymized_data = anonymize_data(data)

    # Show the anonymized data
    anonymized_data.show()

    spark.stop()