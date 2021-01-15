from pyspark.sql import functions as F
class Transform:
    
    def __init__(self, spark):
        self.spark= spark
        
    def transform_data(self, cust_df):
        print('Inside transform')
        
		# finding the mean salary per Gender
        country_avg= cust_df.groupby(['Country','Gender']).agg(F.mean('Salary').alias('AvgSalary')).sort('Country',F.desc('AvgSalary'))
        return country_avg