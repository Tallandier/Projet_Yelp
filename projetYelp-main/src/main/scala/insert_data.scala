import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.Properties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import java.sql.DriverManager

class insert_data(spark: SparkSession) {
        private val URL = "jdbc:postgresql://kafka.iem:5432/vt718096"
        private val USER = "vt718096"
        private val PASSWORD = "vt718096"

        private lazy val JDBC_PROPERTIES = {
            val props = new Properties()
            props.setProperty("user", USER)
            props.setProperty("password", PASSWORD)
            props.setProperty("driver", "org.postgresql.Driver")
            props
        }

        private val Reader = new read_transform_json(spark)

    def insert_data_geography(){
        //Récupération du DF à partir de notre json et de la table fact
        val factDF = spark.read.jdbc(URL, "fact", JDBC_PROPERTIES)
        val dataToInsert = Reader.getFinalDF()

        //Jointure sur business_id, résultat stocké dans un df
        val joinedDF = factDF
        .join(dataToInsert, Seq("business_id"), "inner") 
        .select("geography_id", "etat", "city") 
        .distinct() 

        //Insertion de notre df issu de la jointure dans la table correspondante
        joinedDF.write
        .mode("append")
        .jdbc(URL, "dim_geographie", JDBC_PROPERTIES)

        //Affichage de la table geography 
        val geography_table = spark.read.jdbc(URL, "dim_geographie", JDBC_PROPERTIES)
        geography_table.show()

    }

    def insert_data_ambience(){
        //Récupération du DF à partir de notre json et de la table fact
        val factDF = spark.read.jdbc(URL, "fact", JDBC_PROPERTIES)
        val dataToInsert = Reader.getFinalDF()
        
        //Jointure sur business_id, résultat stocké dans un df
        val joinedDF = factDF
        .join(dataToInsert, Seq("business_id"), "inner") 
        .select("ambience_id", "casual", "divey" , "classy",  "hipster", "intimate" , "romantic", "touristy" , "trendy", "upscale") 
        .distinct()  

        //Insertion de notre df issu de la jointure dans la table correspondante
        joinedDF.write
        .mode("append")
        .jdbc(URL, "dim_ambience", JDBC_PROPERTIES)

        //Affichage de la table geography 
        val ambience_table = spark.read.jdbc(URL, "dim_ambience", JDBC_PROPERTIES)
        ambience_table.show()

    }

    def insert_data_attributes(){
        //Récupération du DF à partir de notre json et de la table fact
        val factDF = spark.read.jdbc(URL, "fact", JDBC_PROPERTIES)
        val dataToInsert = Reader.getFinalDF()

        //Jointure sur business_id, résultat stocké dans un df
        val joinedDF = factDF
        .join(dataToInsert, Seq("business_id"), "inner") 
        .select("attributes_id", "OutdoorSeating", "RestaurantsDelivery" , "RestaurantsTableService",  
        "RestaurantsTakeOut", "RestaurantsGoodForGroups" , "RestaurantsReservations", "GoodForKids" 
        ,"WiFi") 
        .distinct()  

        //Insertion de notre df issu de la jointure dans la table correspondante
        joinedDF.write
        .mode("append")
        .jdbc(URL, "dim_attributes", JDBC_PROPERTIES)

        //Affichage de la table geography 
        val attributes_table = spark.read.jdbc(URL, "dim_attributes", JDBC_PROPERTIES)
        attributes_table.show()

    }
    def calcul_nbChekin_nbTip(){
        // Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()

		
		val tipFile = "/home1/vt718096/Documents/M2/yelp/projetYelp-main/dataset/yelp_academic_dataset_tip.csv"
		val businessFile = "/home1/vt718096/Documents/M2/yelp/projetYelp-main/dataset/yelp_academic_dataset_business.json"
		val chekinFile = "/home1/vt718096/Documents/M2/yelp/projetYelp-main/dataset/yelp_academic_dataset_checkin.json"

		//connection postgres
		println("Postgres connector")

		// connexion
		val jdbcUrl = "jdbc:postgresql://kafka.iem:5432/vt718096"
		val jdbcProperties = new java.util.Properties()
		jdbcProperties.setProperty("user", "vt718096")
		jdbcProperties.setProperty("password", "vt718096")
		jdbcProperties.setProperty("driver", "org.postgresql.Driver")

		val factDF = spark.read
		.jdbc(jdbcUrl, "fact", jdbcProperties)


		val chekinDF = spark.read
		.json(chekinFile)
		.select("business_id", "date") 
		.withColumn("date", explode(split(col("date"), ",\\s*"))) // séparation dates
		.groupBy("business_id") 
		.agg(count("date").alias("nbchekin_temp")) 

		chekinDF.printSchema()
		chekinDF.show(10, truncate=false)

		// fact <-> nbchekin
		val updatedFactDF = factDF
		.join(chekinDF, Seq("business_id"), "left") // Garder tous les `business_id`
		.withColumn("nbchekin", coalesce(col("nbchekin_temp"), lit(0))) // Remplacer `NULL` par `0`
		.drop("nbchekin_temp") // Supprimer la colonne temporaire

		updatedFactDF.printSchema()
		updatedFactDF.show(10, truncate=false)

		// table temp
		val tempTable = "temp_nbchekin"
		updatedFactDF.write
		.mode("overwrite") // ⚠ Temporaire, juste pour l'update
		.jdbc(jdbcUrl, tempTable, jdbcProperties)

		// update fact
		val updateQuery = """
		UPDATE fact f
		SET nbchekin = t.nbchekin
		FROM temp_nbchekin t
		WHERE f.business_id = t.business_id
		"""
		val connection = DriverManager.getConnection(jdbcUrl, "vt718096", "vt718096")
		val statement = connection.createStatement()
		statement.executeUpdate(updateQuery)
		statement.close()
		connection.close()

		val res = spark.read.jdbc(jdbcUrl, "fact", jdbcProperties)
		res.show()



		/*
		// extraction 22 premiers caractères
		val tipDF = spark.read
		.option("header", "true")
		.option("inferSchema", "true")
		.csv(tipFile)
		.withColumn("business_id", substring(col("business_id"), 1, 22)) // Adapter le nom de la colonne si besoin
		.groupBy("business_id")
		.agg(count("*").alias("nbTip_temp")) // Compter le nombre d'occurrences et renommer
		.na.drop() // Supprimer les valeurs nulles si besoin

		//connection postgres
		println("Insertion données")
		// Fusionner les données en mettant à jour `nbTip`
		val updatedFactDF = factDF
		.join(tipDF, Seq("business_id"), "left") // Joindre les deux tables sur business_id
		.withColumn("nbTip", coalesce(col("nbTip_temp"), lit(0))) // Remplace NULL par 0
		.drop("nbTip_temp")

		updatedFactDF.printSchema()
		updatedFactDF.show(10)

		// table temp
		val tempTable = "temp_nbtip"
		updatedFactDF.write
		.mode("overwrite") // ⚠ Écrase uniquement la table temporaire, pas de risque ici
		.jdbc(jdbcUrl, tempTable, jdbcProperties)

		// update
		val updateQuery = """
		UPDATE fact f
		SET "nbtip" = t."nbTip"
		FROM temp_nbtip t
		WHERE f.business_id = t.business_id
		"""
		try {
		val connection = java.sql.DriverManager.getConnection(jdbcUrl, "vt718096", "vt718096")
		val statement = connection.createStatement()
		val rowsUpdated = statement.executeUpdate(updateQuery)
		println(s"Nombre de lignes mises à jour : $rowsUpdated")
		statement.close()
		connection.close()
		} catch {
		case e: Exception =>
			println(s"Erreur lors de la mise à jour : ${e.getMessage}")
		}
        */





    }
    
}