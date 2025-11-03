import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object SimpleApp {
	def main(args: Array[String]) {

		// Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()

		// Chemins des fichiers
		val tipFile = "../dataset/yelp_academic_dataset_tip.csv"
		val businessFile = "../../dataset/yelp_academic_dataset_business.json"


		// Lire le fichier CSV
		/*val df = spark.read
		.option("header", "true")
		.option("inferSchema", "true")
		.csv(inputPath)*/
		//compte les valeurs de tip
		// Filtrer les lignes où le premier champ a exactement 22 caractères et ne récupérer que cette colonne
		//val filteredDF = df.filter(length(col("Colonne1")) === 22).select("Colonne1")


		//connection postgres
		println("Postgres connector")

		// Création de la session Spark
		val sparkSn = SparkSession.builder()
			.appName("PostgreSQLSparkConnection")
			.master("local[*]") // Mets à jour si tu exécutes sur un cluster
			.getOrCreate()

		// Informations de connexion PostgreSQL
		val jdbcUrl = "jdbc:postgresql://kafka.iem:5432/vt718096"
		val jdbcProperties = new java.util.Properties()
		jdbcProperties.setProperty("user", "vt718096")
		jdbcProperties.setProperty("password", "vt718096")
		jdbcProperties.setProperty("driver", "org.postgresql.Driver")

		// Lire une table depuis PostgreSQL
		val tableName = "fact" // Remplace avec ta table réelle
		val df = sparkSn.read.jdbc(jdbcUrl, tableName, jdbcProperties)

		// Afficher quelques lignes pour valider la connexion
		df.show()


		/*
		// Chargement du fichier JSON
		//var users = spark.read.json(usersFile).cache()
		
		// Changement du type d'une colonne
		users = users.withColumn("yelping_since", col("yelping_since").cast(DateType))
		
		// Extraction des amis, qui formeront une table "user_id - friend_id"
		var friends = users.select("user_id", "friends")
		friends = friends.withColumn("friends", explode(org.apache.spark.sql.functions.split(col("friends"), ",")))
		friends = friends.withColumnRenamed("friends", "friend_id")
		// Pour supprimer les lignes sans friend_id
		friends = friends.filter(col("friend_id").notEqual("None"))
		
		// Suppression de la colonne "friends" dans le DataFrame users
		users = users.drop(col("friends"))
		
		// Extraction des années en temps qu'"élite", qui formeront une table "user_id - year"
		var elite = users.select("user_id", "elite")
		elite = elite.withColumn("elite", explode(org.apache.spark.sql.functions.split(col("elite"), ",")))
		elite = elite.withColumnRenamed("elite", "year")
		// Pour supprimer les lignes sans year
		elite = elite.filter(col("year").notEqual(""))
		elite = elite.withColumn("year", col("year").cast(IntegerType))
		
		// Suppression de la colonne "elite" dans le DataFrame users
		users = users.drop(col("elite"))
		
		// Affichage du schéma des DataFrame
		users.printSchema()
		friends.printSchema()
		elite.printSchema()
		
		val reviewsFile = "/chemin_dossier/yelp_academic_dataset_review.json"
		// Chargement du fichier JSON
		var reviews = spark.read.json(reviewsFile).cache()
		// Changement du type d'une colonne
		reviews = reviews.withColumn("date", col("date").cast(DateType))
		
		// Affichage du schéma du DataFrame
		reviews.printSchema()
		
		
		// Enregistrement du DataFrame users dans la table "user"
		users.write
			.mode(SaveMode.Overwrite).jdbc(url, "yelp.\"user\"", connectionProperties)
		
		// Enregistrement du DataFrame friends dans la table "friend"
		friends.write
			.mode(SaveMode.Overwrite).jdbc(url, "yelp.friend", connectionProperties)
		// Enregsitrement du DataFrame elite dans la table "elite"
		elite.write
			.mode(SaveMode.Overwrite).jdbc(url, "yelp.elite", connectionProperties)
		
		// Enregistrement du DataFrame reviews dans la table "review"
		reviews.write
			.mode(SaveMode.Overwrite)
			.jdbc(url, "yelp.review", connectionProperties)
		*/
		spark.stop()
	}
}
