package guru.learningjournal.spark.examples

import swaydb._
import swaydb.serializers.Default._
import scala.concurrent.duration._ //import default serializers
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}


object SparkSchemaDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Schema Demo")
      .master("local[3]")
      .getOrCreate()

    val flightSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))


    val flightSchemaDDL = "FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, " +
      "ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, " +
      "WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"


    val flightTimeCsvDF = spark.read
      .format("csv")
      .option("header", "true")
      //.option("inferSchema", "true")
      .option("mode", "FAILFAST")
      .option("path", "data/flight*.csv")
      .option("dateFormat", "M/d/y")
      .schema(flightSchemaStruct)
      .load()


    //flightTimeCsvDF.show(5)
    //logger.info("CSV Schema:" + flightTimeCsvDF.schema.simpleString)

    val flightTimeJsonDF = spark.read
      .format("json")
      .option("path", "data/flight*.json")
      .option("dateFormat", "M/d/y")
      .schema(flightSchemaDDL)
      .load()
    val selectflightTimeJsonDF =  flightTimeJsonDF.select("OP_CARRIER_FL_NUM","ORIGIN_CITY_NAME")
    //selectflightTimeJsonDF.printSchema()
    var itr = 0
    val resultRow= 30000//selectflightTimeJsonDF.count()
    val resultSet = selectflightTimeJsonDF.collectAsList

    val map = memory.Map[Int, String, Nothing, Glass]()
    while ( itr < resultRow ){

      var col1 = resultSet.get(itr).getInt(0)
      var col2 = resultSet.get(itr).getString(1)
      map.put(col1,col2)
      itr = itr + 1
    }

    selectflightTimeJsonDF.cache()
    val Output =selectflightTimeJsonDF.collect()
    logger.info("JSON Schema:" + flightTimeJsonDF.schema.simpleString)

/*
    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "data/flight*.parquet")
      .load()
*/

    //flightTimeParquetDF.show(5)
    //logger.info("Parquet Schema:" + flightTimeParquetDF.schema.simpleString)

//    val map = memory.Map[Int, String, Nothing, Glass]()

    //some basic put, get, expire and remove operations
  //  map.put(key = 1, value = "one")
  //  map.get(key = 1) //returns Some("one")
  //  map.expire(key = 1, after = 2.seconds)
  //  map.remove(key = 1)

    //range based put, expire and remove
  //  map.put((1, "one"), (2, "two"), (3, "three")) //put multiple as batch transaction
  //  map.expire(from = 1, to = 3, after = 2.seconds) //expire range 1 - 3 after 2 seconds
  //  map.remove(from = 1, to = 3) //remove all keys ranging from 1 to 3

  //  val keyValues = (10 to 100).map(key => (key, s"$key's value"))
  //  map.put(keyValues) //write 100 key-values atomically

    //Create a stream that updates all values within range 10 to 90.
    //val updatedValuesStream =
    //  map
    /*    .fromOrAfter(0) //0 does not exist so it finds the first which is 10.
        .takeWhile(_._1 <= 90) //take only few based on some condition
        .map {
          case (key, value) =>
            (key, value + " updated via Stream")
        }

    //submit the stream to update the key-values as a single transaction.
    map.put(updatedValuesStream)
*/
    //print all key-values to see the updates.
    map.foreach(println)

    scala.io.StdIn.readLine()

  }

}
