from pyspark.sql.session import SparkSession
from pyspark import SparkContext
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

a =  open('roadNet-CA.txt','w')
b = open('/content/sample.txt','r')
#!/usr/bin/env python

from operator import itemgetter
import sys

cw = None
cc = ''
word = None

for line in b:
    if "#" in line:
         continue

    line = line.strip().split('\t')
    word = line[0]
    
    try:
         c=line[1]
    
    except:
          c = ' '
    
    if c == ' ':
        pass   
    
    else:
         c  =  c +','+str((int(word)+int(c))%20+1)+':'
   
    if cw == word:
        cc +=c
   
    else:
        
        if cw=='0':
            a.write('{}\t{}\t{}'.format(cw,0, cc)) 
            a.write('\n')
        
        elif cw:
            a.write('{}\t{}\t{}'.format(cw,999, cc))
            a.write('\n')

        cc = c
        cw = word

if cw == word:
    a.write('{}\t{}\t{}'.format(cw,999, cc))
      
a.close()

textFile = sc.textFile("ragzz.txt")
count = sc.accumulator(0)

def SplitNodes_txtFile(node):
    if len(node.split('\t')) < 3:
        nid, distance = node.split('\t')
        neighbours = None

    else:
        nid, distance, neighbours = node.split('\t')
        neighbours = neighbours.split(':')
        neighbours = neighbours[:len(neighbours) - 1]

    path = nid
    return (nid , (int(distance), neighbours, path))

def SplitNodes_Iterative(node):
    nid = node[0]
    distance = node[1][0]
    neighbours = node[1][1]
    path = node[1][2]
    elements = path.split('->')
 
    if elements[len(elements) - 1] != nid:
        path = path + '->' + nid;

    return (nid , (int(distance), neighbours, path))

def Split_Neighbour(parentPath, parentDistance, neighbour):
    if neighbour!=None:
        nid, distance = neighbour.split(',')
        distance = parentDistance + int(distance)
        path = parentPath + '->' + nid
        return (nid, (int(distance), 'None', path))

def minDist(nodeValue1, nodeValue2):
    neighbours = None
    distance = 0
    path = ''

    if nodeValue1[1] != 'None':
        neighbours = nodeValue1[1]

    else:
        neighbours = nodeValue2[1]
    dist1 = nodeValue1[0]
    dist2 = nodeValue2[0]

    if dist1 <= dist2:
        distance = dist1
        path = nodeValue1[2]

    else:
        count.add(1);
        distance = dist2
        path = nodeValue2[2]

    return (distance, neighbours, path)

def formatResult(node):
    nid = node[0]
    min = node[1][0]
    path = node[1][2]
    return nid, min, path

nodes = textFile.map(lambda node: SplitNodes_txtFile(node))

oldCount = 0
iterations = 0

while True:
    iterations += 1
    nodesValues = nodes.map(lambda x: x[1])
    neighbours = nodesValues.filter(lambda nodeDataFilter: nodeDataFilter[1]!=None).map(
        lambda nodeData: map(
            lambda neighbour: Split_Neighbour(
                nodeData[2], nodeData[0], neighbour
            ), nodeData[1]
        )
    ).flatMap(lambda x: x)
    mapper = nodes.union(neighbours)
    reducer = mapper.reduceByKey(lambda x, y: minDist(x, y))
    nodes = reducer.map(lambda node: SplitNodes_Iterative(node))
    nodes.count()
 
    if oldCount == count.value:
        break

    oldCount=count.value

print('Finished after: ' + str(iterations) + ' iterations')
result = reducer.map(lambda node: formatResult(node))

result.collect()