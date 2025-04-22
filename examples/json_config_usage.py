from pyspark.sql import SparkSession
from src.transformer import PySparkTransformer

def main():
    spark = SparkSession.builder.appName("JsonConfigExample").getOrCreate()
    
    # Sample data
    data = [("James", "Sales", 3000),
            ("Michael", "Sales", 4600)]
    df = spark.createDataFrame(data, ["name", "dept", "salary"])
    
    transformer = PySparkTransformer(spark)
    transformer.process_from_json(df, "examples/configs/basic_transform.json")

if __name__ == "__main__":
    main()
