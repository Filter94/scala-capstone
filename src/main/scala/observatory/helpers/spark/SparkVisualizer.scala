package observatory.helpers.spark

import com.sksamuel.scrimage.{Image, Pixel}
import observatory._
import observatory.helpers.SparkContextKeeper.spark
import observatory.helpers.SparkContextKeeper.spark.implicits._
import observatory.helpers.VisualizationMath.{sphereDistance, w}
import observatory.helpers.VisualizationMath
import observatory.helpers.spark.aggregators.{InterpolatedTempDs, InterpolatedTempSql}
import org.apache.spark.sql.Dataset

import scala.math._

object SparkVisualizer {
  private val DEFAULT_P = 3.0
  private val COLOR_MAX = 255
  private val epsilon: Double = 1E-5

  def computePixels(temps: Dataset[TempByLocation], locations: Dataset[Location],
                    colors: Seq[(Temperature, Color)], transparency: Int): Array[Pixel] = {
    val colorsSorted = VisualizationMath.sortPoints(colors)
    val tempsInterpolated = predictTemperatures(temps, locations)
      .select($"temperature".as[Temperature])
      .collect()
    for {
      temp <- tempsInterpolated
    } yield {
      val color = VisualizationMath.interpolateColor(colorsSorted, temp)
      Pixel(color.red, color.green, color.blue, transparency)
    }
  }

  def locationsGenerator(WIDTH: Int, HEIGHT: Int)(i: Long): Location = {
    val latStart: Double = 90
    val latLength: Double = 180
    val lonStart: Double = -180
    val lonLength: Double = 360
    val latIdx = i / WIDTH
    val lonIdx = i % WIDTH
    val latStep = latLength / HEIGHT
    val lonStep = lonLength / WIDTH
    Location(latStart - latIdx * latStep, lonStart + lonIdx * lonStep)
  }

// fastest implementation so far. Will fail if the temperatures dataset won't fit into memory
  def predictTemperatures(temperatures: Dataset[TempByLocation],
                          locations: Dataset[Location], P: Double = DEFAULT_P): Dataset[Temperature] = {
    val temps = spark.sparkContext.broadcast(temperatures.collect())
    val res = for {
      location <- locations
    } yield {
      val comp = temps.value.map {
        tempByLocation: TempByLocation =>
          val d = sphereDistance(location, tempByLocation.location) max epsilon
          val wi = w(d, P)
          (wi * tempByLocation.temperature, wi)
      }
      val (nominator: Temperature, denominator: Temperature) = comp.reduce {
        (a: (Temperature, Temperature), b: (Temperature, Temperature)) =>
          (a._1 + b._1, a._2 + b._2)
      }
      nominator / denominator
    }
    res.toDF("temperature").as[Temperature]
  }

//  Fast, memory safe implementation
  private val it = new InterpolatedTempSql(DEFAULT_P, epsilon)
  def predictTemperaturesSql(temperatures: Dataset[TempByLocation],
                          locations: Dataset[Location], P: Double = DEFAULT_P): Dataset[Temperature] = {
    temperatures
      .crossJoin(locations)
      .map { row =>
        (Location(row.getAs("lat"),
          row.getAs("lon")),
          Location.fromRow(row, "location"),
          row.getAs[Temperature]("temperature"))
      }
      .groupBy($"_1".as("location"))
      .agg(it($"_1", $"_2", $"_3").as("temperature"))
      .orderBy($"location.lat".desc, $"location.lon")
      .select($"temperature".as[Temperature])
  }


//  Seems a little bit slower, type safe
  private val interpolatedTemp = new InterpolatedTempDs(DEFAULT_P, epsilon).toColumn
  def predictTemperaturesDs(temperatures: Dataset[TempByLocation],
                          locations: Dataset[Location], P: Double = DEFAULT_P): Dataset[Temperature] = {
    temperatures
      .crossJoin(locations)
      .map { row =>
        (Location(row.getAs("lat"),
          row.getAs("lon")),
          Location.fromRow(row, "location"),
          row.getAs[Temperature]("temperature"))
      }
      .groupByKey(_._1)
      .agg(interpolatedTemp.name("temperature"))
      .toDF("location", "temperature")
      .orderBy($"location.lat".desc, $"location.lon")
      .select($"temperature".as[Temperature])
  }

  /**
    * Generates an heatmap image of size WIDTH * HEIGHT by given parameters
    *
    * @param temperatures temperatures by location
    * @param colors       palette of colors for some temperatures
    * @param width        width of the image in pixels
    * @param height       height of the image in pixels
    * @return
    */
  def visualize(width: Int, height: Int, transparency: Int = COLOR_MAX)
               (temperatures: Dataset[TempByLocation], colors: Iterable[(Temperature, Color)]): Image = {
    val locations: Dataset[Location] = spark.range(width * height)
      .map(i => locationsGenerator(width, height)(i))
    val pixels = computePixels(temperatures, locations, colors.toSeq, transparency)
    Image(width, height, pixels)
  }

  def visualizeTile(width: Int, height: Int, transparency: Int = COLOR_MAX, tile: Tile)
                   (temperatures: Dataset[TempByLocation], colors: Iterable[(Temperature, Color)]): Image = {
    val targetZoom = (log(width) / log(2)).toInt
    val zoomedTiles = pow(2, targetZoom).toInt
    val xStart = zoomedTiles * tile.x
    val yStart = zoomedTiles * tile.y

    def tileLocationsGenerator(tile: Tile)(i: Long): Location = {
      val latIdx = (i / width).toInt
      val lonIdx = (i % width).toInt
      val zoomedTile = Tile(xStart + lonIdx, yStart + latIdx, targetZoom + tile.zoom)
      zoomedTile.location
    }

    val locations: Dataset[Location] = spark.range(width * height)
      .map(i => tileLocationsGenerator(tile)(i))
    val pixels = computePixels(temperatures, locations, colors.toSeq, transparency)
    Image(width, height, pixels)
  }

  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature =
    predictTemperatures(toDs(temperatures), spark.createDataset(List(location))).first()

  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    visualize(360, 180, COLOR_MAX)(toDs(temperatures), colors)
  }

  def toDs(data: Iterable[(Location, Temperature)]): Dataset[TempByLocation] = spark.createDataset(data.map {
    case (location, temp) => TempByLocation(location, temp)
  }.toSeq)
}
