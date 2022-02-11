import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import desc
from pyspark.sql import functions as f
from graphframes import *

spark = SparkSession.builder.appName('sg.edu.smu.is459.assignment2').getOrCreate()

# Load data
posts_df = spark.read.load('/user/sueanne/hardwarezone.parquet')
#posts_df.show()

# Clean the dataframe by removing rows with any null value
posts_df = posts_df.na.drop()

# Statistical information of the posts
author_count = posts_df.select('author','topic').distinct().groupBy('author').count()
author_count.sort(desc('count')).show()

print('# of topics: ' + str(posts_df.select('topic').distinct().count()))

# Find distinct users

author_df = posts_df.select('author').distinct()

#print('Author number :' + str(author_df.count()))

# Assign ID to the users
author_id = author_df.withColumn('id', monotonically_increasing_id())
#author_id.show()

# Construct connection between post and author
left_df = posts_df.select('topic', 'author') \
    .withColumnRenamed("topic","ltopic") \
    .withColumnRenamed("author","src_author")

right_df =  left_df.withColumnRenamed('ltopic', 'rtopic') \
    .withColumnRenamed('src_author', 'dst_author')

#  Self join on topic to build connection between authors
author_to_author = left_df. \
    join(right_df, left_df.ltopic == right_df.rtopic) \
    .select(left_df.src_author, right_df.dst_author)
edge_num = author_to_author.count()

#print('Number of edges with duplicate : ' + str(edge_num))

# Convert it into ids
id_to_author = author_to_author \
    .join(author_id, author_to_author.src_author == author_id.author) \
    .select(author_to_author.dst_author, author_id.id) \
    .withColumnRenamed('id','src')

id_to_id = id_to_author \
    .join(author_id, id_to_author.dst_author == author_id.author) \
    .select(id_to_author.src, author_id.id) \
    .withColumnRenamed('id', 'dst')

id_to_id = id_to_id.filter(id_to_id.src > id_to_id.dst) \
    .groupBy('src','dst') \
    .count() \
    .withColumnRenamed('count', 'n')


id_to_id = id_to_id.filter(id_to_id.n >= 5)

#id_to_id.cache()

print("Number of edges without duplciate :" + str(id_to_id.count()))

# Build graph with RDDs
graph = GraphFrame(author_id, id_to_id)

# For complex graph queries, e.g., connected components, you need to set
# the checkopoint directory on HDFS, so Spark can handle failures.
# Remember to change to a valid directory in your HDFS
#spark.sparkContext.setCheckpointDir('/user/sueanne/spark-checkpoint')

# The rest is your work, guys
# ......

#print("\nUsing label propagation to derive community links...")
#label_propagated_df = graph.labelPropagation(100)
#label_propagated_df.show()

#print("How large are the communities (connected components)?")
#distinct_communities_df = label_propagated_df.groupBy('label').count().withColumnRenamed("count", "Community Size").withColumnRenamed("label", "Community id")
#distinct_communities_df.orderBy(desc("count")).show()

#For checking that unlinked users are indeed unlinked
#posts_df.filter(posts_df.author == "plasmic").show()

print("\nWhat are the key words of the community (frequent words)?")


print("\nCalculating number of triangles passing through each vertex (author)...")

triangles_df = graph.triangleCount()
triangles_df.show()

print("\nHow cohesive are the communities (Average # of triangles over every user in a community)?")

total_triangles = triangles_df.agg({'count':'sum'}).first()[0]
author_count = author_df.count() 

cohesiveness = total_triangles / author_count
print("Average number of triangles over each user is", str(cohesiveness))
#Key words of the community
#words_df = posts_df.str_to_map()
