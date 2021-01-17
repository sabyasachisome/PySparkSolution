from pyspark.sql import functions as F
import logging.config
import logging
class Transform:
    
    logging.config.fileConfig('utilities/configs/logging.conf')
    transform_logger= logging.getLogger('logger_transform')
    def __init__(self, spark):
        self.spark= spark
        
    def transform_data(self, cust_df):
        Transform.transform_logger.info('Inside transform')
        
		# finding the mean salary per Gender
        country_avg= cust_df.groupby(['Country','Gender']).agg(F.mean('Salary').alias('AvgSalary')).sort('Country',F.desc('AvgSalary'))
        return country_avg