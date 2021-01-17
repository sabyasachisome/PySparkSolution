import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import Row

import logging.config
from transactions import ingest, persist
from transformations import transform

class Pipeline:
    
    logging.config.fileConfig('utilities/configs/logging.conf')
    def createSparkSession(self):
        self.spark= SparkSession.\
        builder.\
        appName("my first spark session").\
        enableHiveSupport().\
        getOrCreate()
        
        logging.info('created spark session')
        
#         self.spark.sparkContext.setLogLevel('INFO')
    def testSparkSession(self):
        my_list=[1,2,3]
        df= self.spark.createDataFrame(my_list, IntegerType())
        df.show()
    
    def runPipeline(self):
        try:
            logging.info('Inside main pipelne')
        
			ingest_process= ingest.Ingest(self.spark)
			df= ingest_process.ingest_data()
	#         df.show()
			
			transform_process= transform.Transform(self.spark)
			summary_df= transform_process.transform_data(df)
	#         summary_df.show()
			
			persist_process= persist.Persist()
			persist_process.persist_data(summary_df)
        
            logging.info('ETL process complete')
        except Exception as exp:
            logging.error('error inside run pipeline -> {}'.format(exp))