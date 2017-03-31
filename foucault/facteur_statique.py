import urllib.request
import pandas
import json

#Récupération des stations à partir de l'API JcDecaux

liste_entree = json.loads(urllib.request.urlopen("https://api.jcdecaux.com/vls/v1/stations?contract=Paris&apiKey=520780b7094c71787fba8f71bc0c2d8db8ec4aaa").read().decode("utf-8").replace("\'"," "))

coord_float=[]

for station in liste_entree:
	coord_float.append([float(station["position"]["lat"]),float(station["position"]["lng"])])

fichier_sortie = open("/home/cloud/Projet/Source/JcDecaux/Source-Stations-statique.csv","w")

for station in liste_entree:
	lat_x = float(station["position"]["lat"])
	lng_x = float(station["position"]["lng"])

	station["latitude"] = lat_x
	station["longitude"] = lng_x
	
	del station["position"]
	
	#Calcul des distances entre les stations
	ma_liste = list(map(lambda y:pow(lat_x-y[0],2)+pow(lng_x-y[1],2),coord_float))
	station["isolation"] = len(list(filter(lambda y:y<0.001,ma_liste)))
	
	#Récupération de l'altitude avec l'API elevation de Google
	
	station["altitude"]=int(json.loads(urllib.request.urlopen("https://maps.googleapis.com/maps/api/elevation/json?locations={},{}&key=AIzaSyBSiepiGCcpmuf7AS3nbAUDXTIIcWWTpkg".format(lat_x,lng_x)).read().decode("utf-8"))["results"][0]["elevation"])
#AIzaSyAUbN2uRiQBmAMbIvVJRlQoWdqZg6bx7Ws".format(lat_x,lng_x)).read().decode("utf-8"))["results"][0]["elevation"])

data_sortie = pandas.DataFrame(liste_entree)
fichier_sortie.write(data_sortie.to_csv(header=False,index=False))

fichier_sortie.close()
