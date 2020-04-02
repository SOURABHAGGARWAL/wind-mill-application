package com.renew_power.wind_mill_application

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ DateType, TimestampType, StructType, StructField, StringType }
import org.apache.spark.sql.functions.{col,date_format}
import com.renew_power.wind_mill_application.context.SparkContext
import org.apache.spark.sql.expressions.Window


/**
 * Class used to find the fault factor for the windmill data set.
 * @created By Sourabh Aggarwal on 31-MARCH-2020.
 */
object FaultFactor extends SparkContext {

  //schema for the file containing the data related to the windmill.
  val newSchema = StructType(
    List(StructField("TimeStamp (India Standard Time UTC+05:30)", TimestampType, nullable = false),
      StructField("LAHT461-CommunicationState", StringType, nullable = true),
      StructField("LAHT461-WindSpeed", StringType, nullable = true)))

  def main(args: Array[String]) {

    //Check whether sufficient params are supplied
    if (args.length < 1) {
      println("Please provide the correct path for the input file from wind mills.")
      System.exit(1)
    }

    // reading the xlx file int the code
    val windMillRawData = sparkSession.read.schema(newSchema).format("com.crealytics.spark.excel")
      .option("useHeader", "true").option("treatEmptyValuesAsNulls", "false")
      .option("inferSchema", "false").option("addColorColumns", "false")
      .load(args(0)).withColumnRenamed("TimeStamp (India Standard Time UTC+05:30)", "TimeStamp")
      .withColumnRenamed("LAHT461-CommunicationState", "CommunicationState")
      .withColumnRenamed("LAHT461-WindSpeed", "WindSpeed")

    var statusCodeFour = windMillRawData.filter(windMillRawData("CommunicationState") === "4")
    // getting count of the 4 status code in the data
    var statusFourCount =  statusCodeFour.count().toDouble
    
    // getting total number of records in the data.
    var totalCount = windMillRawData.count().toDouble
    
    // calculate the percentage of the 400.
    var factor : Double = ((statusFourCount*100)/(totalCount))
    
    println("TOTAL NOT CONNECTED PERCENTAGE ::::::: " + factor + "%")
    sparkSession.stop()
  }
}