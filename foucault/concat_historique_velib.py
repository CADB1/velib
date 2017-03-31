import glob

liste_nom_fichier = glob.glob("/home/cloud/Projet/Source/JcDecaux/historique/challenge/part_2/paris/stations/*.csv")

fichier_sortie = open("/home/cloud/Projet/Source/JcDecaux/historique/stations.csv","w")

for fichier in liste_nom_fichier:
	number = str(int(fichier[78:].split("-")[0]))
	print(number)
	fichier_courant = open(fichier,"r")
	donnees_fichier_courant = fichier_courant.readlines()
	for ligne in donnees_fichier_courant[1:]:
		fichier_sortie.write(number+","+ligne)	
	fichier_courant.close()
fichier_sortie.close()
