class Persist:
    def persist_data(self, summary_df):
        print('Inside persist')
        
		# writing the dataframe to HDFS output path
        summary_df.\
        coalesce(1).\
        write.\
        option('header','true').\
        csv('PySparkProject/output/summarized_income')