import logging.config
import logging
class Persist:
    
    logging.config.fileConfig('utilities/configs/logging.conf')
    persist_logger= logging.getLogger('logger_persist')
    
    def persist_data(self, summary_df):
        Persist.persist_logger.info('Inside persist')
        
		# writing the dataframe to HDFS output path
        summary_df.\
        coalesce(1).\
        write.\
        option('header','true').\
        csv('PySparkProject/output/summarized_income')