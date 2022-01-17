package guru.learningjournal.spark.examples

import com.google.common.base._
import com.google.common.cache._
import com.google.common.cache.CacheBuilder

import scala.concurrent.ExecutionContext.Implicits._
import java.util.concurrent.{Callable, TimeUnit}
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

import scala.concurrent.Future

object Simple_Guava_Caches_in_Scala {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Schema Demo")
      .master("local[3]")
      .getOrCreate()

    val flightSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", StringType),
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
    var itr = 0
    val resultRow= 30000//selectflightTimeJsonDF.count()
    val resultSet = selectflightTimeJsonDF.collectAsList


    // Simple scala guava cache
    val simpleCache1 =
      CacheBuilder
        .newBuilder()
        .build(new CacheLoader[String, String] {
          def load(key: String): String = {
            println(s"Simple scala guava cache, heavy work calculating $key")
            s"It's me $key"
          }
        })
    /*println(simpleCache1.get("5"))
    println(simpleCache1.get("1"))
    println(simpleCache1.get("2"))
    println(simpleCache1.get("2"))
    println(simpleCache1.get("2"))
    simpleCache1.put("10","Senthil")*/

    while ( itr < resultRow ){

      var col1 = resultSet.get(itr).getString(0)
      var col2 = resultSet.get(itr).getString(1)
      simpleCache1.put(col1,col2)
      itr = itr + 1
    }




    // Simple scala guava supplier cache / factory
    println()
    val supplier_cache: Supplier[String] = Suppliers.memoize(
      () => {
        println("Simple scala guava supplier cache / factory, heavy work creating singleton:")
        "It's me"
      }
    )
    println(supplier_cache.get)
    println(supplier_cache.get)
    println(supplier_cache.get)
  }
}