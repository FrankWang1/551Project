from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data_source_path = 's3://dsci551-bucket/data-source/Weather.csv'
data_output_path = 's3://dsci551-bucket/data-source/data-output'


def main():
    spark = SparkSession.builder.appName('Demo').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')
    all_data = spark.read.csv(data_source_path, header=True)
    print('Total number of records in the source data : %s' % all_data.count())
    selected_data = all_data.where((col('avg_temp') > 3) & (col('province') == 'Seoul'))
    print('the number of cases where the avg_temp is greater than 3 and is in Seoul : %s' % selected_data.count())
    selected_data.write.mode('overwrite').parquet(data_output_path)
if __name__ == '__main__':
    main()
