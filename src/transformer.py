from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from typing import Dict, List, Callable, Any, Optional
import json
import logging
from functools import reduce
from src.exceptions import *

class PySparkTransformer:
    """Configurable framework for dynamic DataFrame transformations in PySpark."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = self._setup_logging()
        self._initialize_default_transformations()
        self._initialize_default_actions()
    
    def _setup_logging(self) -> logging.Logger:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(self.__class__.__name__)
    
    def _initialize_default_transformations(self):
        """Initialize all default transformations."""
        self.TRANSFORMATIONS = {
            'select': self._select,
            'filter': self._filter,
            'with_column': self._with_column,
            # [Include all other transformation methods from previous implementation]
            # ... (copy all transformation methods from previous code)
        }
    
    def _initialize_default_actions(self):
        """Initialize all default actions."""
        self.ACTIONS = {
            'show': lambda df, params: df.show(**params) if params else df.show(),
            'collect': lambda df, _: df.collect(),
            # [Include all other action methods from previous implementation]
            # ... (copy all action methods from previous code)
        }
    
    # [Include all other methods from previous implementation]
    # ... (copy all remaining methods from previous code)
