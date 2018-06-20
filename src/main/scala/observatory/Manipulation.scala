package observatory

import observatory.helpers.Grid
import observatory.helpers.SparkContextKeeper.spark
import observatory.helpers.spark.Utilities

/**
  * 4th milestone: value-added information
  */
object Manipulation {
  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    Grid(temperatures).getTemperature
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    import spark.implicits._
    val records = for {
      temperatures <- spark.sparkContext.parallelize(temperaturess.toSeq)
      tempByLocation <- temperatures
    } yield {
      tempByLocation
    }
    val locationTempDs = records.map { case (location, temp) =>
      TempByLocation(location, temp)
    }.toDF()
    val averages = Utilities.average(locationTempDs).collect()
    makeGrid(averages)
  }

  /**
    * @param temperatures Known temperatures
    * @param normals      A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)],
                normals: GridLocation => Temperature): GridLocation => Temperature = {
    val grid = Grid(temperatures)
    location: GridLocation => {
      grid.combine(location, normals(location), _ - _)
    }
  }
}

