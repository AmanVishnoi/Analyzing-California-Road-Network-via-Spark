# Analyzing-California-Road-Network using Spark
The dataset can be found at this [link](https://snap.stanford.edu/data/roadNet-CA.html)
For the simplicity I have divided the Problem into 4 parts
1. Assuming distance between the pair of nodes to be one, calculating the shotest distance between the source nodes and all other nodes
2. In the actual scenario the there is always a weigted edge the graph the dataset is lacking that distace so I converted graph into a weighted graph and assigns the distance between node <i,j> = ((i+j)%20) + 1. Now recaculating the distance between the source node and all other nodes
3. I took randomly 1000 nodes to calculate all pair shortest path from these 1000 nodes.
4. Counting the number of triangles in Graph.

