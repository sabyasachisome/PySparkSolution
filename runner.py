import pyspark
import pipeline

def main():
    pipeline_obj= pipeline.Pipeline()
    
    pipeline_obj.createSparkSession()
#     pipeline.testSparkSession()
    
    pipeline_obj.runPipeline()
	
if __name__ == '__main__':
	main()
