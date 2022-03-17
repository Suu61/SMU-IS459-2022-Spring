from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.functions import udf

def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
    col = split(sdf['value'], ',')

    #split attributes to nested array in one Column
    #now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf

def clean(x):
    punct = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    x = x.lower()
    for c in punct:
         x = x.replace(c, '')
    x.replace('\"topic\":', "")
    return x

cleanUDF = udf(lambda z: clean(z),StringType())

if __name__ == "__main__":

    spark = SparkSession.builder \
               .appName("KafkaWordCount") \
               .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
               .getOrCreate()

    #Read from Kafka's topic scrapy-output
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "scrapy-output") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

    #Parse the fields in the value column of the message
    lines = df.selectExpr("CAST(value AS STRING)", "timestamp")

    #Specify the schema of the fields
    hardwarezoneSchema = StructType([ \
        StructField("topic", StringType()), \
        StructField("author", StringType()), \
        StructField("content", StringType()) \
        ])

    #Use the function to parse the fields
    lines = parse_data_from_kafka_message(lines, hardwarezoneSchema) \
        .select("topic","author","content","timestamp")

    #set the window duration
    window_duration = "2 minutes"
    slide_duration = "1 minute"

#    lines2 = lines.select("topic").writeStream \
#        .queryName("selectline") \
#        .outputMode("Append") \
#        .format("console") \
#        .option("checkpointLocation", "/user/sueanne/spark-checkpoint") \
#        .start()

####################################################################################################################################################################################
#   1) top 10 users with most posts - start
#    contents = lines.groupBy(window(lines.timestamp, window_duration, slide_duration).alias("timewindow"), lines.author).count().orderBy(["timewindow", "count"], ascending=[0,0]).limit(10) \
#    .writeStream \
#    .queryName("WriteContent") \
#    .outputMode("complete") \
#    .format("console") \
#    .option("checkpointLocation", "/user/sueanne/spark-checkpoint") \
#    .start()
#    contents.awaitTermination()
#   top 10 users with most posts - end

#####################################################################################################################################################################################

#    2) top 10 words - start
    contents2 = lines.select(lines.timestamp, explode(split(cleanUDF(lines.topic), " +")).alias("words")).groupBy(window(lines.timestamp, window_duration, slide_duration).alias("timewindow"), "words").count().orderBy(["timewindow", "count"], ascending=[0,0]).limit(10) \
    .writeStream \
    .queryName("top10words") \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "/user/sueanne/spark-checkpoint") \
    .start()
    contents2.awaitTermination()
#    top 10 words - end
