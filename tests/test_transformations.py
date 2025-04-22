import unittest
from pyspark.sql import SparkSession
from src.transformer import PySparkTransformer
from src.exceptions import *

class TestTransformations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("tests").getOrCreate()
        
        data = [("A", 10), ("B", 20), ("C", 30)]
        cls.df = cls.spark.createDataFrame(data, ["id", "value"])
    
    def test_select_transformation(self):
        transformer = PySparkTransformer(self.spark)
        config = {
            "transformations": [{
                "op": "select",
                "params": {"columns": ["id"]}
            }]
        }
        result = transformer.process(self.df, config)
        self.assertEqual(len(result.columns), 1)
        self.assertIn("id", result.columns)
    
    def test_invalid_transformation(self):
        transformer = PySparkTransformer(self.spark)
        config = {
            "transformations": [{
                "op": "invalid_transform",
                "params": {}
            }]
        }
        with self.assertRaises(UnsupportedTransformationError):
            transformer.process(self.df, config)

if __name__ == '__main__':
    unittest.main()
