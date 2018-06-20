package observatory.helpers.spark

import observatory.{Location, TempByLocation, Temperature}
import observatory.helpers.SparkContextKeeper.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.avg

object Utilities {
  import spark.implicits._
  def toCaseClassDs(data: Iterable[(Location, Temperature)]): Dataset[TempByLocation] = spark.createDataset(data.map {
    case (location, temp) => TempByLocation(location, temp)
  }.toSeq)

  def toTupleDs(data: RDD[(Location, Temperature)]): Dataset[(Location, Temperature)] =
    data.map {
      case (location, temp) => TempByLocation(location, temp)
    }.toDS()
      .select($"location".as[Location], $"temp".as[Temperature])

  def toTupleDs(data: Iterable[(Location, Temperature)]): Dataset[(Location, Temperature)] =
    spark.createDataset(data.toSeq)

  def average(locationTemps: Dataset[TempByLocation]): Dataset[(Location, Temperature)] = {
    locationTemps
    .groupBy($"location")
    .agg(avg($"temperature").as("temp"))
    .select($"location".as[Location], $"temp".as[Temperature])
  }
}
