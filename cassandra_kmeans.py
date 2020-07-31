#Write by: Claudio Quevedo G.
#Date: 25-07-2020
#Reason: Advance database
import pandas as pd
from cassandra.cluster import Cluster
from sklearn.metrics import pairwise_distances_argmin
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans


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

cluster = Cluster()
session = cluster.connect('united_states')
session.execute('USE united_states')
rows = session.execute('select name,population,infected,death,recovered,migration,politic,density,temperature,employment,poverly,netspeed,netcoverage from state')
records = []
for data in rows:
    records.append([data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7],data[8],data[9],data[10],data[11],data[12]])
#print(records)
#3b. declare number of clusters
n_clusters = 4
#4. Transform to dataframe
df = DataFrame(records,columns=['state','population','infected','death','recovered','migration','politic','density','temperature','employment','poverly','netspeed','netcoverage'])
#5. Transform to numpy array
X = np.array(df[['population','infected','death','recovered','migration','politic','density','temperature','employment','poverly','netspeed','netcoverage']])
#6. Call find_clusters function
centroids, labels = find_clusters(X, n_clusters)
#7. Generating clusters
cluster1 = []
cluster2 = []
cluster3 = []
cluster4 = []
for cluster,state in zip(labels,records):
    #print(cluster,state[0])
    if (cluster == 0):
        cluster1.append(state[0])
    elif (cluster == 1):
        cluster2.append(state[0])
    elif (cluster == 2):
        cluster3.append(state[0])
    else:
        cluster4.append(state[0])
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
