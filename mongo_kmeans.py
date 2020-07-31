#Write by: Claudio Quevedo G.
#Date: 25-07-2020
#Reason: Advance database
import pandas as pd
import pymongo
from pymongo import MongoClient
import pprint
from sklearn.metrics import pairwise_distances_argmin
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.metrics import pairwise_distances_argmin_min

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

client = MongoClient('localhost',27017)
db = client['BDAproject']
states = db['states']

records = []
for states in states.find({'date':'26-05-2020'}):
        records.append([states['state'],states['population'],
        states['infected'],states['death'],
        states['recovered'],states['migration'],
        states['politic'],states['density'],
        states['temperature'],states['employment'],
        states['poverly'],states['netspeed'],
        states['netcoverage'],states['rurality']])

#print(records)
#3b. declare number of clusters
n_clusters = 4
#4. Transform to dataframe
df = DataFrame(records,columns=['state','population','infected','death','recovered','migration','politic','density','temperature','employment','poverly','netspeed','netcoverage','rurality'])
#5. Transform to numpy array
X = np.array(df[['population','infected','death','recovered','migration','politic','density','temperature','employment','poverly','netspeed','netcoverage','rurality']])
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
        cluster1.append([state[0],state[1],state[2],state[3],state[4],state[5],state[6],state[7],state[8],state[9],state[10],state[11],state[12],state[13]])
    elif (cluster == 1):
        cluster2.append([state[0],state[1],state[2],state[3],state[4],state[5],state[6],state[7],state[8],state[9],state[10],state[11],state[12],state[13]])
    elif (cluster == 2):
        cluster3.append([state[0],state[1],state[2],state[3],state[4],state[5],state[6],state[7],state[8],state[9],state[10],state[11],state[12],state[13]])
    else:
        cluster4.append([state[0],state[1],state[2],state[3],state[4],state[5],state[6],state[7],state[8],state[9],state[10],state[11],state[12],state[13]])
print("\nGrupo 1\n")
print("state",";","population",";","infected",";","death",";","recovered",";","migration",";","politic",";","density",";","temperature",";","employment",";","poverly",";","netspeed",";","netcoverage",";","rurality")
for state in cluster1:
    print(state[0],";",state[1],";",state[2],";",state[3],";",state[4],";",state[5],";",state[6],";",state[7],";",state[8],";",state[9],";",state[10],";",state[11],";",state[12],";",state[13])
print("\nGrupo 2\n")
print("state",";","population",";","infected",";","death",";","recovered",";","migration",";","politic",";","density",";","temperature",";","employment",";","poverly",";","netspeed",";","netcoverage"";","rurality")
for state in cluster2:
    print(state[0],";",state[1],";",state[2],";",state[3],";",state[4],";",state[5],";",state[6],";",state[7],";",state[8],";",state[9],";",state[10],";",state[11],";",state[12],";",state[13])
print("\nGrupo 3\n")
print("state",";","population",";","infected",";","death",";","recovered",";","migration",";","politic",";","density",";","temperature",";","employment",";","poverly",";","netspeed",";","netcoverage"";","rurality")
for state in cluster3:
    print(state[0],";",state[1],";",state[2],";",state[3],";",state[4],";",state[5],";",state[6],";",state[7],";",state[8],";",state[9],";",state[10],";",state[11],";",state[12],";",state[13])
print("\nGrupo 4\n")
print("state",";","population",";","infected",";","death",";","recovered",";","migration",";","politic",";","density",";","temperature",";","employment",";","poverly",";","netspeed",";","netcoverage"";","rurality")
for state in cluster4:
    print(state[0],";",state[1],";",state[2],";",state[3],";",state[4],";",state[5],";",state[6],";",state[7],";",state[8],";",state[9],";",state[10],";",state[11],";",state[12],";",state[13])

#vemos el representante del grupo, el usuario cercano a su centroid
closest, _ = pairwise_distances_argmin_min(centroids, X)
state=df['state'].values
#print("\n\nRepresentantes de cada grupo: ")
i = 1
represent = []
#for row in closest:
#    represent.append([state[row]])

#print(represent)
#print("           state",";","population",";","infected",";","death",";","recovered",";","migration",";","politic",";","density",";","temperature",";","employment",";","poverly",";","netspeed",";","netcoverage")
#for rep in represent:
#    for data in records:
#        if (rep[0] == data[0]):
#            print("Grupo",i,": ",data[0],";",data[1],";",data[2],";",data[3],";",data[4],";",data[5],";",data[6],";",data[7],";",data[8],";",data[9],";",data[10],";",data[11],";",data[12])
#            i = i +1
