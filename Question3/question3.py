from pyspark import *
import pandas as pd
from pyspark.sql.session import SparkSession
from pyspark import SparkContext
from random import randint
from datetime import datetime

# creating a sparkSession
context = SparkContext()
context.addPyFile('/home/hadoop/graphframes-0.8.2-spark2.4-s_2.11.jar')
spark = SparkSession(context)

from graphframes import GraphFrame
from pyspark.sql import *
spark = SparkSession.builder.appName('fun').getOrCreate()



# we will be loading sample data(.csv) of nodes (cities)
data = pd.read_csv("output.txt",header=None,names=["AB"])
datanew = data['AB'].str.split("\t", n=1,expand = True)
cleandata = datanew.rename(columns={0:"dst", 1:"src"})
cleandata.query('(src.str.isdigit()) & (dst.str.isdigit())', inplace=True)
cleandata = cleandata.apply(pd.to_numeric)
s1 = cleandata["src"]
s2 = cleandata["dst"]
s1 = s1.append(s2)

vertex = s1.drop_duplicates().reset_index(drop=True)
vertex = vertex.to_frame()
vertex = vertex.rename(columns={0:"id"})

nodes = vertex["id"].sample(n=20)
nodes = nodes.tolist()

vertex = spark.createDataFrame(vertex, ['id'])
edge = spark.createDataFrame(cleandata, ['dst', 'src'])
vertex.show()
edge.show()

# nodes = [randint(0,9) for i in range(2)]
# nodes = [0,1,2,3,4,5,6,7,8,9]

g = GraphFrame(vertex, edge)
with open("results.txt", "a") as f:
    for index,node in enumerate(nodes):
        dt = datetime.now()
        results = g.shortestPaths(landmarks=[node])
        results.show()
        results = results.toPandas()
        col = results["distances"]
        s = 0
        for i in range(len(col)):
            s += sum(col[i].values())
        s = s/len(col)
        dt2 = datetime.now()
        s = str(dt2-dt) + " " +str(s) +"\n"
        f.write(s)
        f.flush()
        print("Iteration done", index)
