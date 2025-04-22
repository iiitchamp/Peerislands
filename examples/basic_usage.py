from pyspark.sql import SparkSession
from src.transformer import PySparkTransformer

def main():
    spark = SparkSession.builder.appName("BasicUsageExample").getOrCreate()
    
    # Sample data
    data = [("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 4100)]
    columns = ["name", "department", "salary"]
    df = spark.createDataFrame(data, columns)
    
    transformer = PySparkTransformer(spark)
    
    config = {
        "transformations": [
            {
                "op": "filter",
                "params": {"condition": "salary > 3000"}
            },
            {
                "op": "select",
                "params": {"columns": ["name", "department"]}
            }
        ],
        "action": {
            "op": "show",
            "params": {"numRows": 10, "truncate": False}
        }
    }
    
    transformer.process(df, config)

if __name__ == "__main__":
    main()
