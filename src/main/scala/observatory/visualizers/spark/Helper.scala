package observatory.visualizers.spark

import observatory.SparkContextKeeper.spark
import observatory.SparkContextKeeper.spark.implicits._
import observatory.{Location, TempByLocation, Temperature}
import org.apache.spark.sql.Dataset

object Helper {
  def toDs(data: Iterable[(Location, Temperature)]): Dataset[TempByLocation] = spark.createDataset(
    data.map {
      case (location, temp) => TempByLocation(location, temp)
    }.toSeq)
}
