package observatory.helpers.spark

import com.sksamuel.scrimage.{Image, Pixel}
import observatory._
import observatory.helpers.SparkContextKeeper.spark
import observatory.helpers.SparkContextKeeper.spark.implicits._
import observatory.helpers.VisualizationMath._
import observatory.helpers.VisualizationMath.Implicits.interpolateComponent
import observatory.helpers.{VisualizationMath, Visualizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import scala.collection.GenIterable
import scala.math._

object SparkVisualizer {

  object Implicits {
    implicit def locationsGeneratorLong(WIDTH: Int, HEIGHT: Int)(i: Long): Location = {
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

    implicit def computePixels(temps: Dataset[TempByLocation], locations: Dataset[Location],
                               colors: Iterable[(Temperature, Color)], transparency: Int): Array[Pixel] = {
      val tempsInterpolated = predictTemperaturesSpark(temps, locations)
        .orderBy($"location.lat".desc, $"location.lon")
        .select($"temp".as[Temperature]).collect()
      for {
        temp <- tempsInterpolated
      } yield {
        val color = interpolateColor(colors, temp)
        Pixel(color.red, color.green, color.blue, transparency)
      }
    }
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
  def predictTemperaturesSpark(temperatures: Dataset[TempByLocation],
                               locations: Dataset[Location]): Dataset[(Location, Temperature)] = {
    val P = 3
    temperatures
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
    * @param WIDTH        width of the image in pixels
    * @param HEIGHT       height of the image in pixels
    * @return
    */
  def visualize(WIDTH: Int, HEIGHT: Int, transparency: Int = COLOR_MAX)
               (temperatures: Dataset[TempByLocation], colors: Iterable[(Temperature, Color)])
               (implicit locationsGenerator: (Int, Int) => Long => Location,
                computePixels: (Dataset[TempByLocation], Dataset[Location],
                  Iterable[(Temperature, Color)], Int) => Array[Pixel]): Image = {
    val locations: Dataset[Location] = spark.range(WIDTH * HEIGHT)
      .map(i => {
        val latIdx = i / WIDTH
        val lonIdx = i % WIDTH
        val latStep = 180 / HEIGHT
        val lonStep = 360 / WIDTH
        Location(90 - latIdx * latStep, -180 + lonIdx * lonStep)
      })
    val pixels = computePixels(temperatures, locations, colors, transparency)
    Image(WIDTH, HEIGHT, pixels)
  }

  def toDs(data: Iterable[(Location, Temperature)]): Dataset[TempByLocation] = spark.createDataset(data.map {
    case (location, temp) => TempByLocation(location, temp)
  }.toSeq)

  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature =
    predictTemperature(temperatures, location)

  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    import Implicits._
    visualize(360, 180)(toDs(temperatures), colors)
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

  def visualize(width: Int, height: Int, transparency: Int)
               (temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    import SparkVisualizer.Implicits._
    SparkVisualizer.visualize(width, height, transparency)(SparkVisualizer.toDs(temperatures), colors)
  }
}
