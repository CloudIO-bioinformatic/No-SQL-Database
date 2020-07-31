#Write by: Claudio Quevedo G.
#Date: 25-07-2020
#Reason: Advance database
import pandas as pd
from py2neo import Graph, Node, Relationship
from sklearn.metrics import pairwise_distances_argmin
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
g = Graph()

def find_clusters(X, n_clusters, rseed=2):
    # 1. Randomly choose clusters
    rng = np.random.RandomState(rseed)
    i = rng.permutation(X.shape[0])[:n_clusters]
    centers = X[i]
    while True:
        # 2a. Assign labels based on closest center
        labels = pairwise_distances_argmin(X, centers)
        # 2b. Find new centers from means of points
        new_centers = np.array([X[labels == i].mean(0)
                                for i in range(n_clusters)])
        # 2c. Check for convergence
        if np.all(centers == new_centers):
            break
        centers = new_centers
    return centers, labels
X = g.run("MATCH (state:State) RETURN state.population,"+
"state.infected,state.death,state.recovered,state.migration,"+
"state.politic,state.density,state.temperature,"+
"state.employment,state.poverly,state.netspeed,"+
"state.netcoverage LIMIT 50").to_ndarray()
records = g.run("MATCH (state:State) RETURN state.name").data()

n_clusters = 4
centroids, labels = find_clusters(X, n_clusters)
#7. Generating clusters
cluster1 = []
cluster2 = []
cluster3 = []
cluster4 = []
for cluster,state in zip(labels,records):
    #print(cluster,state[0])
    if (cluster == 0):
        cluster1.append(state['state.name'])
    elif (cluster == 1):
        cluster2.append(state['state.name'])
    elif (cluster == 2):
        cluster3.append(state['state.name'])
    else:
        cluster4.append(state['state.name'])
print("Grupo 1\n")
result = ''
for row in cluster1:
    result= result+row+", "
print(result[:-2],".")
print("\nGrupo 2\n")
result = ''
for row in cluster2:
    result= result+row+", "
print(result[:-2],".")
print("\nGrupo 3\n")
result = ''
for row in cluster3:
    result= result+row+", "
print(result[:-2],".")
print("\nGrupo 4\n")
result = ''
for row in cluster4:
    result= result+row+", "
print(result[:-2],".")
