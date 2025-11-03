import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.DataFrame

object SimpleApp {
	def main(args: Array[String]) {
		// Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()
		
		val usersFile = "../../dataset/yelp_academic_dataset_business.json"
		//PARTIE LECTURE ET TRANSFO JSON//////////////////////////////////////////////////////////////
		val reader = new read_transform_json(spark)

		//Filtre sur les restaurants A EXECUTER POUR GENERER LE JSON RESTAURANT
		val df_restaurant = reader.filterRestaurant()
		//UPDATE LE CHEMIN
		val df_restaurant_FIXE = reader.readJSON("C:/Users/julie/Documents/Cours/BDIA/projetYelp/dataset/intermediate_json/restaurants_filtered")

		
		
		// Chargement du fichier JSON
		var users = spark.read.json(usersFile).cache()
		/*
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
		
		// Paramètres de la connexion BD
		Class.forName("org.postgresql.Driver")
		val url = "jdbc:postgresql://stendhal:5432/tpid2020"
		import java.util.Properties
		val connectionProperties = new Properties()
		connectionProperties.setProperty("driver", "org.postgresql.Driver")
		connectionProperties.setProperty("user", "user")
		connectionProperties.setProperty("password","password")
		
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
