import pyspark
sc = pyspark.SparkContext()
print(dir(sc))

from pyspark.sql import SQLContext

sqlCont = SQLContext(sc)

print("J en suis ici")

rdd_station_statique = sqlCont.read.format("org.apache.spark.sql.cassandra").options(table="station_statique", keyspace="velib_db").load().rdd
rdd_station_statique_cle_valeur = rdd_station_statique.map(lambda x:(x["number"],(x["altitude"],x["isolation"])))
print(rdd_station_statique_cle_valeur.sortByKey().collect())








print(sqlCont.read.format("org.apache.spark.sql.cassandra").options(table="meteo", keyspace="velib_db"))
print(dir(sqlCont.read.format("org.apache.spark.sql.cassandra").options(table="meteo", keyspace="velib_db")))
rdd_source = sqlCont.read.format("org.apache.spark.sql.cassandra").options(table="meteo", keyspace="velib_db").load().rdd
#rdd_trie = rdd_source.sortByKey()

print("Et maintenant ici")
rdd_source_cle_valeur = rdd_source.map(lambda x:(x["moment"],(x["clouds"],x["description"],x["humidity"],x["pressure"],["temperature"],["wind"])))

print("Toujours pas fini ?")
print(rdd_source_cle_valeur.first())

rdd_source_trie = rdd_source_cle_valeur.sample(False,0.10).sortByKey()
print(rdd_source_trie.first())

print(type(rdd_source))
print(type(SQLContext))


