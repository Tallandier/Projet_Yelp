import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.io.File

class read_transform_json(spark: SparkSession) {
    val DATASET_FILE_PATH:String = "C:/Users/julie/Documents/Cours/BDIA/projetYelp/dataset"

    val FILEPATH: String = DATASET_FILE_PATH + "/yelp_academic_dataset_business.json"
    val INTERMEDIATE_JSON_RESTAURANT_FILTERED:String = DATASET_FILE_PATH + "/intermediate_json/restaurants_filtered"
    val INTERMEDIATE_JSON_ATTRIBUTES:String = DATASET_FILE_PATH + "/intermediate_json/attributes_added"
    val INTERMEDIATE_JSON_AMBIENCE:String = DATASET_FILE_PATH + "/intermediate_json/ambience_added"
    val FINAL_JSON :String = DATASET_FILE_PATH + "/intermediate_json/final"

    def getFinalDF() : DataFrame = {
        return readJSON(FINAL_JSON)
    }

    def readJSON(filepath : String) : DataFrame  = spark.read.json(filepath)   
    
    def filterRestaurant() : DataFrame = {
        val df_complet = readJSON(FILEPATH)
        //Colonne categorie en lower case pour que le filtre soit correctement appliqué
        val lowerCaseCategories = df_complet.withColumn("categories", lower(col("categories")))
        val df_restaurant = lowerCaseCategories.filter(col("categories").contains("restaurant"))
        //enregistrer le json du DataFrame obtenu 
        createIntermediateJson(df_restaurant, INTERMEDIATE_JSON_RESTAURANT_FILTERED)
        return df_restaurant
    }

    //----------------PARTIE AJOUT DES COLONNES ATTRIBUTS ET AMBIENCE ---------------

    //Ajoute les colonnes correspondantes aux attributs 
    def addAttributesColumn(): Option[DataFrame] = {
        val df = readIntermediateJson(INTERMEDIATE_JSON_RESTAURANT_FILTERED).getOrElse(spark.emptyDataFrame)
        
        val attributes:List[String] = List("OutdoorSeating", "RestaurantsDelivery", "RestaurantsTableService", "RestaurantsTakeOut", 
            "RestaurantsGoodForGroups", "GoodForKids", "RestaurantsReservations","WiFi")

        var jsonSchema: StructType = new StructType()

        if(df.isEmpty){
            System.err.println("Erreur: addAttributesColumn le dataframe récupéré est vide")
            return None
        } else {
            var dfFinal = df.withColumn("attributesAsJSON", to_json(col("attributes")))

            for(attribute <- attributes){
                //Definition du schema JSON attendu
                jsonSchema = jsonSchema.add(attribute, StringType, true)
            }
            // Parser la colonne JSON et créer une colonne Struct
            dfFinal = dfFinal.withColumn("attributesParsed", from_json(col("attributesAsJSON"), jsonSchema))
        
            for(attribute <- attributes){
                //Stockage des data json dans une colonne 
                //dfFinal =  dfFinal.withColumn(attribute, col(s"attributesParsed.$attribute"))
                
            dfFinal = dfFinal.withColumn(attribute,
        coalesce(
            when(col(s"attributesParsed.$attribute").isNotNull, lit("true"))
            .otherwise(lit("false")),
            lit("false")
        )
    )

                dfFinal = cleanColumnAttributes(dfFinal, attribute)
            }
            //Affichage du début du df obtenu 
            dfFinal.select("OutdoorSeating", "RestaurantsDelivery", "RestaurantsTableService", "RestaurantsTakeOut",  
            "RestaurantsGoodForGroups", "GoodForKids", "RestaurantsReservations", "WiFi").show()

            createIntermediateJson(dfFinal, INTERMEDIATE_JSON_ATTRIBUTES)
            return Some(dfFinal)
        }
    }

    //Ajoute les colonnes correspondantes aux attributs 
    def addAmbienceColumn() : Option[DataFrame] = {
        val df = readIntermediateJson(INTERMEDIATE_JSON_ATTRIBUTES).getOrElse(spark.emptyDataFrame)
        if(df.isEmpty){
            System.err.println("Erreur: addAmbienceColumn le dataframe récupéré est vide")
            return None
        } else {
            val ambiences:List[String] = List("touristy","hipster","romantic","intimate","trendy","upscale","classy","casual","divey")

            
            var dfFinal: DataFrame = df.withColumn("AmbienceASStr", col("attributes.Ambience"))
            //Remplacer les guillemets simples par des doubles 
            dfFinal = dfFinal.withColumn("AmbienceASJson", regexp_replace(col("AmbienceASStr"), "'", "\""))

            dfFinal = dfFinal.withColumn("AmbienceASJson", regexp_replace(col("AmbienceASJson"), "\\bFalse\\b", "false"))
                             .withColumn("AmbienceASJson",regexp_replace(col("AmbienceASJson"), "\\bTrue\\b", "true")
                    )
            dfFinal = dfFinal.withColumn("AmbienceMap", from_json(col("AmbienceASJson"), MapType(StringType, BooleanType)))

            for(ambience <- ambiences){
                //dfFinal = dfFinal.withColumn(ambience, col("AmbienceMap")(ambience))
                dfFinal = dfFinal.withColumn(ambience, 
                coalesce(col("AmbienceMap").getItem(ambience), lit(false))
                )
                //Clean column of null value 
                dfFinal = cleanColumn(dfFinal, ambience)

            }
            dfFinal.show()
            createIntermediateJson(dfFinal, INTERMEDIATE_JSON_AMBIENCE)

            return Some(dfFinal)
        }
        
    }
    
    def cleanColumn(df:DataFrame, colName:String) : DataFrame = {
        var df_temp = df.na.fill(false, Seq(colName)) 
        return df_temp   
    }

    def cleanColumnAttributes(df:DataFrame, colName:String) : DataFrame = {
        var df_temp = df.na.fill(false, Seq(colName)) 
        if(colName == "WiFi") {
            df_temp = df.withColumn(colName,
                when(col(colName).isin("free", "true"), true)
                .otherwise(false)
            )}
        df_temp = df.withColumn(colName,
            when(col(colName).isin("true", "True"), true)
            .otherwise(false)
        )
        return df_temp;
    }
 
    def createDFFinal() : Option[DataFrame] = {
        val df = readIntermediateJson(INTERMEDIATE_JSON_AMBIENCE).getOrElse(spark.emptyDataFrame)
        if(df.isEmpty){
            System.err.println("Erreur: createDFFinal le dataframe récupéré est vide")
            return None
        } else {
            var newDF = df.drop("attributes", "attributesAsJSON", "attributesParsed", "categories", "hours", "is_open", "latitude", "longitude" , "name", "AmbienceASStr", "AmbienceASJson", "AmbienceMap", "review_count", "stars")     
            newDF = newDF.withColumnRenamed("state", "etat")
            createIntermediateJson(newDF, FINAL_JSON)
            return Some(newDF)
        }
    }

    //----------------PARTIE LECTURE ET CREATION FICHIER INTERMEDIARES JSON ---------------

    //Créer un fichier json au lieu indiqué contenant le dataframe donné, retourne le chemin du 
    //fichier avec son nom
    private def createIntermediateJson(jsonData: DataFrame, filePath: String) : Boolean = {
        //Vider le dossier  
        val dir = new File(filePath)
        if(dir.exists() && dir.isDirectory){
            dir.listFiles().foreach(_.delete())

        } else {
                System.err.println("Erreur: chemin du fichier intermediaire json n'est pas trouvé :" + filePath)
                return false
        }
        //Ajout de notre dataframe sous forme de json au dossier 
        jsonData.coalesce(1) //On ne souhaite qu'un fichier json et non pas 1 par partition spark
            .write
            .mode("overwrite") //Ecrase le fichier existant du même nom
            .json(filePath)

        return true
    }

    //Recherche du fichier json intermediare dans le dossier indiqué
    private def getFileNameIntermediateJson(directoryPath:String) : Option[String] = {
        val dir = new File(directoryPath)

        dir.listFiles()
        .find(f => f.getName.endsWith(".json") && f.getName.startsWith("part-"))
        .map(_.getAbsolutePath)
    }

    private def readIntermediateJson(directoryPath:String) : Option[DataFrame] = {
        val jsonFilePath:String = getFileNameIntermediateJson(directoryPath).getOrElse("")
       
        if(jsonFilePath != ""){
            return Some(readJSON(jsonFilePath)) 
        } else {
            None
        }
        
    }
}