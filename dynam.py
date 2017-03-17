# -*- coding: utf-8 -*-
import glob
import pandas
import matplotlib.pyplot as plt
from os import system

# Récupération de la liste des noms de fichiers commençant par Source-fichier-
liste_chemins_fichiers = glob.glob("/home/cloud/Projet/Source/JcDecaux/ancien/Source-Stations-2017-03-15-*-*0-*.txt")
liste_chemins_fichiers.sort()


# Ouverture du premier fichier de la liste ci-dessus

premier_fichier = pandas.read_json(liste_chemins_fichiers[0],lines=True).values.tolist()


# Lecture de la totalité des lignes de ce fichier
liste_dict={}


for station in premier_fichier:
	liste_dict[station[1]]=[[station[3]]]

liste_timestamp = [str(premier_fichier[0][0])]


fichier_sortie=open("/home/cloud/Projet/Source/JcDecaux/Source-Stations-dynamique.txt","w")


for chemin_fichier_courant in liste_chemins_fichiers[1:]:
	fichier_courant = pandas.read_json(chemin_fichier_courant,lines=True).values.tolist()
	for station in fichier_courant:
		liste_dict[station[1]][0].append(station[3])
	liste_timestamp.append(str(fichier_courant[0][0]))

#print(liste_dict)

for station in liste_dict:
	item_avant=liste_dict[station][0][0]
	liste_dict[station].append([])
	for item in liste_dict[station][0][1:]:
		diff=item-item_avant
		item_avant=item
		liste_dict[station][1].append(diff)

#print(liste_dict)

#print(liste_dict)

#liste_filtre = list(map(lambda x:[["_id",str(x)],["diff_filtre",len(list(filter(lambda y:abs(y)>10,liste_dict[x][1])))]],liste_dict))

#for station in liste_dict:
#	fichier_sortie.writelines(str([station,liste_dict[station]]))


colors=['b', 'g', 'k','c', 'y', 'm', 'r','b','g','k','c']

for param in range(5,10):
	liste_x = list(map(lambda x:[x],liste_dict))
	liste_y = list(map(lambda x:[len(list(filter(lambda y:abs(y)>param,liste_dict[x][1])))],liste_dict))
	plt.scatter(list(range(len(liste_x))),liste_y,color=colors[param-5],marker=",",s=1)
	
	maximum = max(liste_y)
	print(maximum)	

	for indice in range(len(liste_x)):
		if liste_y[indice]==maximum:
			print(liste_x[indice])

plt.savefig("image.jpg")

system("gpicview image.jpg")

print(liste_dict[12151])
print(liste_dict[43005])
#print(liste_dict)

fichier_sortie.close()

#for fichier_courant in chaine_fichier[1:]:
#	stations = pandas.read_json(fichier_courant,lines=True).values.tolist()
#	for station in stations:
#		station[]



#liste_station = {}
#for station in chaine_fichier:
#	mon_dict={}
