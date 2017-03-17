import pandas

#Récupération des stations à partir du fichier source
stations = pandas.read_json("/home/cloud/Projet/Source/JcDecaux/Source-Stations-statique.txt",lines=True).drop("@timestamp",axis=1).values.tolist()

#Pour calculer la distance entre toutes les stations
#stations_int est un tableau à 3 colonnes et autant de lignes que de stations
#A chaque ligne, on ajoute une liste contenant la distance de la station aux autres stations
for x in stations:
	x.append(list(map(lambda y:pow(x[1]-y[1],2)+pow(x[2]-y[2],2),stations)))

#Pour calculer le nombre de stations dont la distance est inférieure à un seuil
#Pour chaque ligne (i.e.: station), on exécute une fonction qui renvoit le numéro, la latitude, la longitude et le nombre de distances inférieures à un seuil
stations_coeff = list(map(lambda x:[x[0],x[1],x[2],len(list(filter(lambda y:y<0.001,x[3])))],stations))

#On réinjecte les labels
ma_liste = list(map(lambda x:[["_id",str(int(x[0]))],["lat",str(x[1])],["lng",str(x[2])],["coeff",str(int(x[3]))]],stations_coeff))

#Ouverture du fchier de sortie
fichier_sortie=open("/home/cloud/Projet/Source/JcDecaux/Source-Stations-statique-coeff.txt","w")

#Pour calculer le minimum et le maximum de voisins, enlever les commentaires suivants
#minimum=int(ma_liste[0][3][1])
#maximum=int(ma_liste[0][3][1])

#Pour chaque liste dans la liste, on créé un dictionnaire que l'on écrit dans le fichier de sortie
for item in ma_liste:
	mon_dict={}
#	print(item[3][1])
	for i in range(4):
		mon_dict[item[i][0]]=item[i][1]
#	if int(item[3][1]) < minimum:
#		minimum = int(item[3][1])
#	if int(item[3][1]) > maximum:
#		maximum = int(item[3][1])
	fichier_sortie.writelines([str(mon_dict),"\n"])

#print(minimum,maximum)

#Fermeture du fichier de sortie
fichier_sortie.close()
