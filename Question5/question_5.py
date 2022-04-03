from pyspark import *
import pandas as pd
from pyspark.sql.session import SparkSession
from pyspark import SparkContext

# creating a sparkSession
context = SparkContext()
context.addPyFile('/home/hadoop/graphframes-0.8.2-spark2.4-s_2.11.jar')
spark = SparkSession(context)

from graphframes import GraphFrame
from pyspark.sql import *
spark = SparkSession.builder.appName('fun').getOrCreate()



# we will be loading sample data(.csv) of nodes (cities)
data = pd.read_csv("/home/hadoop/roadNet-CA.txt",header=None,names=["AB"])
datanew = data['AB'].str.split("\t", n=1,expand = True)
cleandata = datanew.rename(columns={0:"src", 1:"dst"})
cleandata.query('(src.str.isdigit()) & (dst.str.isdigit())', inplace=True)
cleandata = cleandata.apply(pd.to_numeric)
s1 = cleandata["src"]
s2 = cleandata["dst"]
s1 = s1.append(s2)

vertex = s1.drop_duplicates().reset_index(drop=True)
vertex = vertex.to_frame()
vertex = vertex.rename(columns={0:"id"})


vertex = spark.createDataFrame(vertex, ['id'])
edge = spark.createDataFrame(cleandata, ['src', 'dst'])
vertex.show()
edge.show()

g = GraphFrame(vertex, edge)

# results = g.shortestPaths(landmarks=[0])
results = g.triangleCount()
results.show()
results = results.toPandas()
results.to_csv("a.csv")
# print(type((vertices)))
# print(type(edges))













