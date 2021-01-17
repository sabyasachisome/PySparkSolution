import logging.config
import logging
class Ingest:
    
    logging.config.fileConfig('utilities/configs/logging.conf')
    ingest_logger= logging.getLogger('logger_ingest')
    
    def __init__(self, spark):
        self.spark= spark
        Ingest.ingest_logger.info(self.spark.version)
        
    def ingest_data(self):
#         listt=[1,2,3,4,5]
#         df_ingest= self.spark.createDataFrame(listt, IntegerType())
        Ingest.ingest_logger.info('Inside ingest')
        
		# ingesting the dataframe
        cust_df= self.spark.\
        read.\
        csv('PySparkProject/retailstore.csv', inferSchema=True, header=True)
        
        return cust_df