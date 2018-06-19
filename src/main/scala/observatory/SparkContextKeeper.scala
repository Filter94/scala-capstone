package observatory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkContextKeeper {
//  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Capstone spark")
    .config("spark.master", "local")
    .getOrCreate()
}
