import pandas
import urllib.request

#Récupération des stations à partir du fichier source
stations = pandas.read_json("/home/cloud/Projet/Source/JcDecaux/Source-Stations-statique.txt",lines=True).drop("@timestamp",axis=1).values.tolist()

fichier_sortie=open("/home/cloud/Projet/Source/JcDecaux/Source-Stations-statique-altitude.txt","w")

ma_liste = list(map(lambda x:[["_id",str(int(x[0]))],["lat",str(x[1])],["lng",str(x[2])]],stations))

for x in ma_liste:
	mon_dict={}
	x.append(["elevation",str(int(pandas.read_json(urllib.request.urlopen("https://maps.googleapis.com/maps/api/elevation/json?locations={},{}&key=AIzaSyAUbN2uRiQBmAMbIvVJRlQoWdqZg6bx7Ws".format(x[1][1],x[2][1])).read().decode("utf-8"))["results"][0]["elevation"]))])
	for i in range(4):
                mon_dict[x[i][0]]=x[i][1]
	
	fichier_sortie.writelines([str(mon_dict),"\n"])

fichier_sortie.close()
