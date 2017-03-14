
# coding: utf-8

# In[168]:

import requests
import pandas as pd
import pyspark
from pprint import pprint

if not sc:
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

# In[21]:

urlParisStations = "https://api.jcdecaux.com/vls/v1/stations"
jcdKey = "c7c379f39e07e619be9663cfce805b8b266dce1a"
# https://api.jcdecaux.com/vls/v1/stations?contract={contract_name}
stations_json = requests.get(url = urlParisStations, params={"contract":"Paris","apiKey":jcdKey}).text


# In[184]:

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

# In[54]:

first_station = df_stations.head(1).position.values[0]
first_station


# eclater la colonne postion qui contient la latitude et la longitude en 2 colonnes lat et lng
#
# df_stations contient les coordonnées à plat

# In[185]:


dfs = [df_stations.drop('position',axis=1),df_stations["position"].apply(pd.Series)]
df_stations = pd.concat(dfs,axis=1)



# In[151]:

df_stations.head()


# associer la station météo de chez openweathermap qui correspond à la stations vélib par geolocalisation (cityId)
#
# http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}
#
#
# APPID  : 679875fddff53d1517c9f4acda29535e
#
#

# In[171]:

owmUrl = "http://api.openweathermap.org/data/2.5/weather"
appid="679875fddff53d1517c9f4acda29535e"
#resp = requests.get(url=owmUrl,params={"APPID":appid,"lat":48.864528,"lon":2.416171})


def cityIdSerie(velib):
    return requests.get(url=owmUrl,params={"APPID":appid,"lat":velib.lat,"lon":velib.lng}).json()["id"]

city_ids = df_stations.apply(cityIdSerie,axis=1)



# In[189]:


df_cities = pd.DataFrame(data=city_ids,columns=["owm_city_id"])

df_stations = pd.merge(left=df_stations,right=df_cities,how="left",left_index=True,right_index=True)



# In[191]:

df_stations.head()


# In[193]:

len(df_stations["owm_city_id"].unique())
