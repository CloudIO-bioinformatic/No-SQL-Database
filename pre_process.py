import csv
data_covid = []
data_estados = []
data_migracion = []
data_nivel_empleo = []

with open('reporte_covid19.csv') as File:
    reader = csv.reader(File, delimiter=',', quotechar=',', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        data_covid.append(row)
with open('estados_parsed.csv') as File:
    reader = csv.reader(File, delimiter=',', quotechar=',', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        data_estados.append(row)
with open('migracion.csv') as File:
    reader = csv.reader(File, delimiter=';', quotechar=';', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        data_migracion.append(row)
data_hito1 = []
for row in data_estados:
        for row2 in data_covid:
                for row3 in data_migracion:
                        if (row[0] == row2[0] and row[0] == row3[0]):
                                tasa_migracion = int(row3[7]) / int(row[1])
                                data_hito1.append([row[0],row[1],row2[5],row2[6],row2[7],tasa_migracion])
data_hito2_1 = []
with open('pp_datasets.csv') as File:
    reader = csv.reader(File, delimiter='\t', quotechar='\t', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        for row2 in data_hito1:
            if (row[0] == row2[0]):
                if(row[1] == 'Republican'):
                    flag = 1
                    data_hito2_1.append([row2[0],row2[1],row2[2],row2[3],row2[4],row2[5],flag])
                if(row[1] == 'Democrat'):
                    flag = 0
                    data_hito2_1.append([row2[0],row2[1],row2[2],row2[3],row2[4],row2[5],flag])
data_hito2_2 = []
with open('pd_datasets.csv') as File:
    reader = csv.reader(File, delimiter='\t', quotechar='\t', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        for row2 in data_hito2_1:
            if (row[0] == row2[0]):
                if (row[1] != 'density'):
                    data_hito2_2.append([row2[0],row2[1],row2[2],row2[3],row2[4],row2[5],row2[6],row[1]])
data_hito2_3 = []
with open('tcelsius_datasets.csv') as File:
    reader = csv.reader(File, delimiter='\t', quotechar='\t', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        for row2 in data_hito2_2:
            if (row[0] == row2[0]):
                if (row[1] != 'temperature'):
                    data_hito2_3.append([row2[0],row2[1],row2[2],row2[3],row2[4],row2[5],row2[6],row2[7],row[1]])
data_hito2_4 = []
contador = 0
i = 0
with open('ne_datasets.csv') as File:
    reader = csv.reader(File, delimiter='\t', quotechar='\t', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        i = i + 1
        contador = contador + int(row[1])
        if (i == 9):
            data_nivel_empleo.append([row[0],contador])
            i = 0
            contador = 0
for row in data_nivel_empleo:
    for row2 in data_hito2_3:
        if (row[0] == row2[0]):
            data_hito2_4.append([row2[0],row2[1],row2[2],row2[3],row2[4],row2[5],row2[6],row2[7],row2[8],row[1]])
data_hito2_5 = []
with open('ppobreza_datasets.csv') as File:
    reader = csv.reader(File, delimiter='\t', quotechar='\t', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        for row2 in data_hito2_4:
            if (row[0] == row2[0]):
                data_hito2_5.append([row2[0],row2[1],row2[2],row2[3],row2[4],row2[5],row2[6],row2[7],row2[8],row2[9],row[1]])
data_hito2_6 = []
with open('cv_datasets.csv') as File:
    reader = csv.reader(File, delimiter='\t', quotechar='\t', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        for row2 in data_hito2_5:
            if (row[0] == row2[0]):
                if (row[1] != 'ASpeed_Mbps'):
                    data_hito2_6.append([row2[0],row2[1],row2[2],row2[3],row2[4],row2[5],row2[6],row2[7],row2[8],row2[9],row2[10],row[1],row[2]])
data_hito2_7 = []
with open('prural_datasets.csv') as File:
    reader = csv.reader(File, delimiter='\t', quotechar='\t', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        for row2 in data_hito2_6:
            if (row[0] == row2[0]):
                if (row[1] != 'rural_percentage'):
                    data_hito2_7.append([row2[0],row2[1],row2[2],row2[3],row2[4],row2[5],row2[6],row2[7],row2[8],row2[9],row2[10],row2[11],row2[12],row[1]])
print("state",";","population",";","infected",";","death",";","recovered",";","migration",";","politic",";","density",";","temperature",";","employment",";","poverly",";","netspeed",";","netcoverage",";","rurality")
for row in data_hito2_7:
        print(row[0],";",row[1],";",row[2],";",row[3],";",row[4],";",row[5],";",row[6],";",row[7],";",row[8],";",row[9],";",row[10],";",row[11],";",row[12],";",row[13])
