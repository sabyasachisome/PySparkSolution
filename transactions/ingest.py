class Ingest:
    
    def __init__(self, spark):
        self.spark= spark
        print(self.spark.version)
        
    def ingest_data(self):
        print('Inside ingest')
#         listt=[1,2,3,4,5]
#         df_ingest= self.spark.createDataFrame(listt, IntegerType())
        
		# ingesting the dataframe
        cust_df= self.spark.\
        read.\
        csv('PySparkProject/retailstore.csv', inferSchema=True, header=True)
        
        return cust_df