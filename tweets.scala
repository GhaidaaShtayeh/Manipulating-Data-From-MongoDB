
import com.mongodb.MongoClient
import com.mongodb.client.model
import com.mongodb.client.model.Filters.{and, geoWithinCenter, gte, lte}
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession
import org.mongodb.scala.model.{Filters, Indexes}
import com.mongodb.casbah.Imports._

import java.text.SimpleDateFormat
import scala.Console.println
import com.mongodb.casbah.query.dsl
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

import scala.collection.convert.ImplicitConversions.{`iterable AsScalaIterable`, `seq AsJavaList`}
import org.apache.spark.sql.functions.{col, to_timestamp, udf}
object RevenueRetrieval {

  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("MongoSparkDataFrame")
      .config(
        "spark.mongodb.input.uri",
        "mongodb://localhost:27017/acme.Tweets"
      )
      .config(
        "spark.mongodb.output.uri",
        "mongodb://localhost:27017/acme.Tweets"
      )
      .getOrCreate()

    val sc = spark.sparkContext

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // read the json-formatted tweets.tweets in the attached file and use MongoSpark
    // library to insert them into mongoDB database in a collection called 'tweets.tweets'

    val data_df = spark.read
      .format("json")
      .load("src/main/boulder_flood_geolocated_tweets.json")
    val conf = WriteConfig(
      Map("collection" -> "Tweets",
        "writeConcern.w" -> "majority"),
      Some(WriteConfig(sc))
    )
    //////////////////////////////////////////////////////////////////////////////////////////////

    // The timestamp associated with each tweet is to be stored as a Date object
    // where the timestamp field is to be indexed.
    // AT FIRST : create a user define function to convert the date string to another format

    val Format = udf((created_at: String) => {
      val simpleDateF = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy")
      val new_date = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss")
      val DataAfterFormatted = new_date.format((simpleDateF.parse(created_at)))
        DataAfterFormatted
    })
    //////////////////////////////////////////////////////////////////////////////////////////////

    //add column with new format
    val col_formatted = Format(col("created_at"))
    var dataDf = data_df.withColumn("created_at", col_formatted)

    // convert it to timestamp field
    val _timestamp = to_timestamp(col("created_at"),"MM-dd-yyyy HH:mm:ss")
    dataDf = dataDf.withColumn("created_at",_timestamp)
    MongoSpark.save(dataDf, conf)
    //////////////////////////////////////////////////////////////////////////////////////////////
    //Indexing the geo-coordinates of tweets.tweets to ensure a fast spatial-based retrieval
    val mongoClient = new MongoClient("localhost",27017)
    val database = mongoClient.getDatabase("acme")
    val collection = database.getCollection("Tweets")
    collection.createIndex(Indexes.geo2dsphere("coordinates.coordinates"))
    collection.createIndex(Indexes.ascending("created_at"))

    println("**********************************************************")
    //////////////////////////////////////////////////////////////////////////////////////////////

    //calling all values and variables needed

    /*println("Enter radius : ")
    val r=scala.io.StdIn.readInt()
    println("Enter central point of (lon, lat) : ")
    val lon=scala.io.StdIn.readDouble()
    val lat=scala.io.StdIn.readDouble()
    println("Enter time interval (start, end) : ")
    val start=scala.io.StdIn.readLine()
    val End=scala.io.StdIn.readLine()
    println("Enter word u wanna search : ")
    val w=scala.io.StdIn.readLine()*/

    val r = args(0).toInt
    val lon = args(1).toDouble
    val lat = args(2).toDouble
    val start = args(3)
    val End = args(4)
    val w = args(5)


    val formatter = new SimpleDateFormat("MM-dd-yyyy")
    val start_date_ = formatter.parse(start)
    val end_date_ = formatter.parse(End)
    println(start_date_,end_date_)



    val timeSpaceFilter1 = collection.find (and(gte("created_at", start_date_),lte("created_at", end_date_),
        model.Filters.regex("text",w),
        geoWithinCenter ("coordinates.coordinates",lon,lat, r)))

    val timeSpaceFilter = timeSpaceFilter1.toList


   var counter = 0
   val word_count = timeSpaceFilter.map(x => x.get("text").toString)
    val word_count2 = word_count.flatMap(x => x.split(" "))

    word_count2.foreach( word =>{
       if(word==w)
         counter = counter + 1
     })
    println("number of occurrences of word is :  " + counter)
    println("**********************************************************")
//    val q: DBObject= ("created_at" $lte end_date_ $gt start_date_) ++ ("text" $regex w)
    /////////////////////////////////////////////////////////////////////////


  }
}