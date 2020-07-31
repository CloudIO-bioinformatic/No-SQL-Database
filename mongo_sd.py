#Write by: Claudio Quevedo G.
#Date: 25-07-2020
#Reason: Advance database
import pandas as pd
import pymongo
from pymongo import MongoClient
import pprint

client = MongoClient('localhost',27017)
db = client['BDAproject']
states = db['states']
#se borra la bd para que no haya duplicamiento en el testeo de codigo
states.drop()
datasets = pd.read_csv('datasets.csv',sep=';')
records = []
for i in range(0, 50):
    records.append([datasets.values[i,j] for j in range(0, 14)])

for data in records:
    a = states.insert_one({'date':'26-05-2020','state':data[0],'population':data[1],
    'infected':data[2],'death':data[3],
    'recovered':data[4],'migration':data[5],
    'politic':data[6],'density':data[7],
    'temperature':data[8],'employment':data[9],
    'poverly':data[10],'netspeed':data[11],
    'netcoverage':data[12],'rurality':data[13]}).inserted_id
    #imprime key
    #print(a)
for states in states.find():
        pprint.pprint(states)
        #print(states['state'])
print("\n>>  Datos guardados correctamente  <<")
