
# coding: utf-8

# In[1]:

import requests
import pandas as pd
import pyspark
from pprint import pprint

#if not sc:
sc = pyspark.SparkContext()

type(sc)


# # Objectifs :
# 
#    - construire un dataframe pandas qui regroupe l'ensemble des données que sur lesquelles on veut lancer notre apprentissage
#      - données statiques additionnelles à ajouter:
#         1 coefficient d'isolation (nombre de stations accessible depuis la station dans un rayon donné raisonnable 5km ?)
#         associer la station météo la plus proche de la station vélib
#         
# $e^{i\pi} + 1 = 0$
# 
# $$e^x=\sum_{i=0}^\infty \frac{1}{i!}x^i$$
#        
# 
# 

# # recupération des stations velibs de paris

# In[2]:

urlParisStations = "https://api.jcdecaux.com/vls/v1/stations"
jcdKey = "c7c379f39e07e619be9663cfce805b8b266dce1a"
# https://api.jcdecaux.com/vls/v1/stations?contract={contract_name} 
stations_json = requests.get(url = urlParisStations, params={"contract":"Paris","apiKey":jcdKey}).text


# In[3]:

df_stations = pd.read_json(stations_json,precise_float=True)                .drop("contract_name",axis=1)                .set_index("number")
df_stations.head()


# # Récupération des stations météo de paris et alentours
# 
# source openWeather api:
# 
# http://api.openweathermap.org/data/2.5/weather?q={city name}
# 
# http://api.openweathermap.org/data/2.5/weather?q={city name},{country code}
# 
# http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}
# 
# APPID : 679875fddff53d1517c9f4acda29535e

# In[4]:

first_station = df_stations.head(1).position.values[0]
first_station


# eclater la colonne postion qui contient la latitude et la longitude en 2 colonnes lat et lng
# 
# df_stations contient les coordonnées à plat

# In[5]:


dfs = [df_stations.drop('position',axis=1),df_stations["position"].apply(pd.Series)]
df_stations = pd.concat(dfs,axis=1)



# In[6]:

df_stations.head()


# associer la station météo de chez openweathermap qui correspond à la stations vélib par geolocalisation (cityId)
# 
# http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}
# 
# 
# APPID  : 679875fddff53d1517c9f4acda29535e
# 
# 

# In[7]:

owmUrl = "http://api.openweathermap.org/data/2.5/weather"
appid="679875fddff53d1517c9f4acda29535e" 
#resp = requests.get(url=owmUrl,params={"APPID":appid,"lat":48.864528,"lon":2.416171})


def cityIdSerie(velib):
    return requests.get(url=owmUrl,params={"APPID":appid,"lat":velib.lat,"lon":velib.lng}).json()["id"]
    
city_ids = df_stations.apply(cityIdSerie,axis=1)



# In[8]:


df_cities = pd.DataFrame(data=city_ids,columns=["owm_city_id"])

df_stations = pd.merge(left=df_stations,right=df_cities,how="left",left_index=True,right_index=True)



# In[9]:

df_stations.head()


# In[11]:

len(df_stations["owm_city_id"].unique())


# In[13]:

df_stations.to_csv("/home/jovyan/work/df_stations.csv")


#    # calcul du coefficient d'isolation de chaque station
#    
# 
#     import pandas as pd
#     def isolationFactor(geoPoint,radius):
#        ''' retourne le nombre de stations vélib dans un rayon de radius autour du centre geoPoint '''
# 

# In[88]:

from geopy.distance import vincenty
from geopy.point import Point
 

station = {"lat":48.864528,"lng":2.416171}    

distances = df_stations[["lat","lng"]].apply(lambda row: vincenty((station['lat'],station['lng']),(row.lat,row.lng)).km,axis=1) <4

print(len(df_stations[distances])-1)



            


# In[ ]:

def isolationFactor(station,radius):
    distances = df_stations[["lat","lng"]].apply(lambda row: vincenty((station.lat,station.lng),(row.lat,row.lng)).km ,axis=1)
    return len( df_stations[distances < radius] ) - 1

#res = isolationFactor(df_stations.iloc[1],3)
#res

df_stations["isolation_factor"] =df_stations.apply(isolationFactor,args=[3],axis=1)      
#res = df_stations.head().apply(isolationFactor,args=[3],axis=1)      
#res


# In[ ]:

df_stations.head()
df_stations.to_csv("/home/jovyan/work/df_stations.csv")

