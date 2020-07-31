#Write by: Claudio Quevedo G.
#Date: 25-07-2020
#Reason: Advance database
import pandas as pd
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('united_states')
session.execute('USE united_states')
session.execute('TRUNCATE state')

datasets = pd.read_csv('datasets',sep=';')
records = []
for i in range(0, 50):
    records.append([datasets.values[i,j] for j in range(0, 13)])
for data in records:
    session.execute("INSERT INTO state(name,population,infected,death,recovered,migration,politic,density,temperature,employment,poverly,netspeed,netcoverage)VALUES(%(name)s,%(population)s,%(infected)s,%(death)s,%(recovered)s,%(migration)s,%(politic)s,%(density)s,%(temperature)s,%(employment)s,%(poverly)s,%(netspeed)s,%(netcoverage)s)",
    {'name':data[0],'population':data[1],'infected':data[2],'death':data[3],'recovered':int(data[4]),'migration':data[5],'politic':data[6],'density':data[7],'temperature':data[8],'employment':data[9],'poverly':data[10],'netspeed':data[11],'netcoverage':data[12]})
print("\n>>  Datos guardados correctamente  <<")
