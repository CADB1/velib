from cassandra.query import SimpleStatement
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.cluster import Cluster

cluster = Cluster(["84.39.48.220", "84.39.45.149", "84.39.45.143"],port=9042)
session_number = cluster.connect("velib_db")
session_select = cluster.connect("velib_db")
session_update = cluster.connect("velib_db")

requete_number = "SELECT DISTINCT number FROM station_dynamique"
declaration_number = SimpleStatement(requete_number, fetch_size = 1)

for ligne_number in session_number.execute(declaration_number):
	requete_select = "SELECT * FROM station_dynamique WHERE number ={}".format(ligne_number[0])
	declaration_select = SimpleStatement(requete_select, fetch_size = 1)
	for ligne_select in session_select.execute(declaration_select):
		if ligne_select[13] == None:
  			session_update.execute("UPDATE station_dynamique SET bikes_availability = {} WHERE number ={} AND moment =\'{}\'".format(int(100*ligne_select[12]/ligne_select[4]),ligne_select[0],str(ligne_select[1])))
		if ligne_select[15] == None:
			session_update.execute("UPDATE station_dynamique SET spaces_availability = {} WHERE number ={} AND moment =\'{}\'".format(int(100*ligne_select[14]/ligne_select[4]),ligne_select[0],str(ligne_select[1])))
	print(ligne_select[0])
