#Write by: Claudio Quevedo G.
#Date: 25-07-2020
#Reason: Advance database
import pandas as pd
from py2neo import Graph, Node, Relationship

datasets = pd.read_csv('datasets.csv',sep=';')
records = []
for i in range(0, 50):
    records.append([datasets.values[i,j] for j in range(0, 13)])


g = Graph()
g.run("MATCH (n) DETACH DELETE n")
tx = g.begin()
country = Node("Country", name="United States")
tx.create(country)
for data in records:
    state = Node("State", name=data[0], population=data[1],infected=data[2],
    death=data[3],recovered=data[4],migration=data[5],
    politic=data[6],density=data[7],temperature=data[8],
    employment=data[9],poverly=data[10],netspeed=data[11],netcoverage=data[12])
    country_state = Relationship(country, "HAS", state)
    tx.create(country_state)
tx.commit()
