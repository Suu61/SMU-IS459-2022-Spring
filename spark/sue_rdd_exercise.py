import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f 

spark = SparkSession.builder.appName('RDD Exercise').getOrCreate()

# Load CSV file into a data frame
score_sheet_df = spark.read.load('/user/sueanne/score-sheet.csv', \
    format='csv', sep=';', inferSchema='true', header='true')

score_sheet_df.show()

# Get RDD from the data frame
score_sheet_rdd = score_sheet_df.rdd
score_sheet_rdd.first()

#SUE: get minimum and maximum values
maximum = score_sheet_rdd.map(lambda x: x[1]).max()
minimum = score_sheet_rdd.map(lambda x: x[1]).min()
#print("Max: " +str(maximum)+ "\nMin:" +str(minimum))

#SUE: get rdd of max and rdd of min
max_rdd = score_sheet_rdd.filter(lambda x: x[1] == maximum).collect()[0][0]
min_rdd = score_sheet_rdd.filter(lambda x: x[1] == minimum).collect()[0][0]
#print(max_rdd)
#print(min_rdd)

#remove the two values from score_rdd
working_rdd = score_sheet_rdd.filter(lambda x: x[0] != max_rdd and x[0] != min_rdd)

# Project the second column of scores with an additional 1
final_rdd = working_rdd.map(lambda x: (x[1], 1))
final_rdd.first()

# Get the sum and count by reduce
(total, count) = final_rdd.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print('Average Score : ' + str(total/count))

# --- PART 2 --- #

# Load Parquet file into a data frame
posts_df = spark.read.load('/user/sueanne/hardwarezone.parquet')

#posts_df.createOrReplaceTempView("posts")

#append column of post length
intermediate_df = posts_df.withColumn("Word count", f.size(f.split(f.col('content'), " ")))
#print(intermediate_df.filter(intermediate_df['author'] == 'Pyre').count())
#intermediate_df.filter(intermediate_df['author'] == 'Pyre').agg({'Word count': 'sum'}).show()
final_df = intermediate_df.groupBy("author").agg(f.avg("Word count").alias("Average Post Length")).show()
#final_df.filter(final_df["author"] == 'Pyre').show()
#final_df.show()

#sqlDF = spark.sql("SELECT * FROM posts WHERE author='SG_Jimmy'")
#num_post = sqlDF.count()
#print('Jimmy has ' + str(num_post) + ' posts.')

#posts_rdd = posts_df.rdd

# Project the author and content columns
#author_content_rdd = posts_rdd.map(lambda x: (len(x[2]), 1))
#author_content_rdd.first()

# Get sume and count by reduce
#(sum, count) = author_content_rdd.reduce(lambda x,y: (x[0]+y[0], x[1]+y[1]))
#print('Average post length : ' + str(sum/count))
