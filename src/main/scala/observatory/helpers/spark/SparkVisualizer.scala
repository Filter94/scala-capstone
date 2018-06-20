package observatory.helpers.spark

import com.sksamuel.scrimage.{Image, Pixel}
import observatory._
import observatory.helpers.SparkContextKeeper.spark
import observatory.helpers.SparkContextKeeper.spark.implicits._
import observatory.helpers.VisualizationMath._
import observatory.helpers.VisualizationMath.Implicits.interpolateComponent
import observatory.helpers.{VisualizationMath, Visualizer}
import org.apache.spark.sql.Dataset

import scala.collection.GenIterable
import scala.math._

object SparkVisualizer {
  def computePixels(temps: Dataset[TempByLocation], locations: Dataset[Location],
                    colors: Iterable[(Temperature, Color)], transparency: Int): Array[Pixel] = {
    val tempsInterpolated = predictTemperatures(temps, locations)
      .orderBy($"location.lat".desc, $"location.lon")
      .select($"temp".as[Temperature]).collect()
    for {
      temp <- tempsInterpolated
    } yield {
      val color = interpolateColor(colors, temp)
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

  def tileLocationsGenerator(WIDTH: Int, HEIGHT: Int, tile: Tile)(i: Long): Location = {
    val latIdx = (i / WIDTH).toInt
    val lonIdx = (i % WIDTH).toInt
    val precision = (log(WIDTH) / log(2)).toInt
    val targetZoom = precision
    val xStart = (pow(2, precision) * tile.x).toInt
    val yStart = (pow(2, precision) * tile.y).toInt
    val zoomedTile = Tile(xStart + lonIdx, yStart + latIdx, targetZoom + tile.zoom)
    zoomedTile.location
  }

  val epsilon = 1E-5


  /*  This agggregator is inefficient with datasets
      private def interpolatedTemp(P: Int = 3) =
        new Aggregator[(Location, Location, Temperature), (Temperature, Temperature), Temperature] {
          // Pure ds / df aggregator. Haven't figured out how to use it with DS efficiently
          override def zero: (Temperature, Temperature) = (0.0, 0.0)

          override def merge(b: (Temperature, Temperature), a: (Distance, Temperature)): (Distance, Temperature) =
            (a._1 + b._1, a._2 + b._2)

          override def reduce(b1: (Temperature, Temperature), b2: (Location, Location, Temperature)): (Temperature, Temperature) =
            (b1, b2) match {
              case ((nomAcc, denomAcc), (xi, location, ui)) =>
                val d = max(sphereDistance(location, xi), epsilon)
                val wi = w(location, d, P)
                (nomAcc + wi * ui, denomAcc + wi)
            }

          override def finish(reduction: (Temperature, Temperature)): Temperature = reduction._1 / reduction._2

          def bufferEncoder: Encoder[(Distance, Temperature)] =
            Encoders.tuple(Encoders.scalaDouble, Encoders.scalaDouble)

          def outputEncoder: Encoder[Temperature] = Encoders.scalaDouble
        }.toColumn  */
  def predictTemperatures(temperatures: Dataset[TempByLocation],
                          locations: Dataset[Location]): Dataset[(Location, Temperature)] = {
    val P = 3
    temperatures // TODO: optimize by broadcasting the temps
      .crossJoin(locations)
      .map { row =>
        (Location(row.getAs("lat"),
          row.getAs("lon")),
          Location.fromRow(row, "location"),
          row.getAs[Temperature]("temperature"))
      }.rdd
      .keyBy(row => row._1)
      .aggregateByKey((0.0, 0.0))(
        { (b1: (Temperature, Temperature), b2: (Location, Location, Temperature)) =>
          (b1, b2) match {
            case ((nomAcc, denomAcc), (xi, location, ui)) =>
              val d = max(sphereDistance(location, xi), epsilon)
              val wi = w(location, d, P)
              (nomAcc + wi * ui, denomAcc + wi)
          }
        }, {
          (b: (Temperature, Temperature), a: (Distance, Temperature)) =>
            (a._1 + b._1, a._2 + b._2)
        })
      .mapValues {
        case (nominator, denominator) =>
          nominator / denominator
      }.toDF("location", "temp").as[(Location, Temperature)]
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
    val pixels = computePixels(temperatures, locations, colors, transparency)
    Image(width, height, pixels)
  }

  def visualizeTile(width: Int, height: Int, transparency: Int = COLOR_MAX, tile: Tile)
                   (temperatures: Dataset[TempByLocation], colors: Iterable[(Temperature, Color)]): Image = {
    val locations: Dataset[Location] = spark.range(width * height)
      .map(i => tileLocationsGenerator(width, height, tile)(i))
    val pixels = computePixels(temperatures, locations, colors, transparency)
    Image(width, height, pixels)
  }

  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature =
    predictTemperatures(Utilities.toCaseClassDs(temperatures), spark.createDataset(List(location))).first()._2

  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    visualize(360, 180, COLOR_MAX)(Utilities.toCaseClassDs(temperatures), colors)
  }
}

trait SparkVisualizer extends Visualizer {
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature =
    SparkVisualizer.predictTemperature(temperatures, location)

  override def interpolateColor(points: GenIterable[(Temperature, Color)], value: Temperature): Color = {
    VisualizationMath.interpolateColor(points, value)
  }

  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image =
    SparkVisualizer.visualize(temperatures, colors)

  def visualizeTile(width: Int, height: Int, transparency: Int, tile: Tile)
                   (temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    SparkVisualizer.visualizeTile(width, height, transparency, tile)(Utilities.toCaseClassDs(temperatures), colors)
  }
}
