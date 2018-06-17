package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.sql._

import math._
import scala.collection.parallel.ParIterable
import SparkContextKeeper.spark

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  import spark.implicits._

  private val epsilon = 1E-5
  private val R = 6372.8
  private val COLOR_MAX = 255
  private type Distance = Double

  def visualize(temperatures: Iterable[(Location, Temperature)],
                colors: Iterable[(Temperature, Color)]): Image = visualizeSparkWrapper(temperatures, colors)

  def equals(a: Location, b: Location): Boolean =
    a.lon - b.lon < epsilon && a.lat - b.lat < epsilon


  def sphereDistance(a: Location, b: Location): Distance =
    (a, b) match {
      case _ if Location.equals(a, b) => 0
      case _ =>
        R * acos(sin(toRadians(a.lat)) * sin(toRadians(b.lat)) +
          cos(toRadians(a.lat)) * cos(toRadians(b.lat)) * cos(toRadians(b.lon - a.lon)))
    }

  def w(x: Location, d: Distance, p: Double): Temperature = 1 / math.pow(d, p)

  def findTwoClosest(points: Iterable[(Temperature, Color)], temp: Temperature):
  ((Temperature, Color), (Temperature, Color)) = {
    def tempDistance(a: Temperature, b: Temperature): Distance = abs(a - b)

    case class PointDistance(point: (Temperature, Color), distance: Distance)

    val twoClosest: List[PointDistance] = points.foldLeft(List[PointDistance]()) {
      (agg: List[PointDistance], point: (Temperature, Color)) => {
        val newDistance = tempDistance(point._1, temp)
        agg match {
          case x :: y :: Nil =>
            if (newDistance < x.distance)
              PointDistance(point, newDistance) :: y :: Nil
            else if (newDistance < y.distance)
              x :: PointDistance(point, newDistance) :: Nil
            else
              agg
          case _ => PointDistance(point, newDistance) :: agg
        }
      }
    }
    (twoClosest.head.point, twoClosest(1).point)
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    def boundValue(min: Int, max: Int)(value: Int): Int = math.min(math.max(min, value), max)

    def interpolateComponent(x1: Temperature, x2: Temperature, value: Temperature)(y1: Int, y2: Int): Int =
      math.round(y1 + ((y2 - y1) / (x2 - x1) * (value - x1))).toInt

    val ((x1: Temperature, y1: Color), (x2: Temperature, y2: Color)) = findTwoClosest(points, value)

    def interpolator = interpolateComponent(x1, x2, value)(_, _)

    def bounder = boundValue(0, COLOR_MAX)(_)

    val newRed = bounder(interpolator(y1.red, y2.red))
    val newGreen = bounder(interpolator(y1.green, y2.green))
    val newBlue = bounder(interpolator(y1.blue, y2.blue))
    Color(newRed, newGreen, newBlue)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualizeSparkWrapper(temperatures: Iterable[(Location, Temperature)],
                            colors: Iterable[(Temperature, Color)], WIDTH: Int = 360, HEIGHT: Int = 180): Image = {
    val tempByLocationDs = spark.createDataset(temperatures.map {
      case (location, temp) => TempByLocation(location, temp)
    }.toSeq)
    visualizeSpark(tempByLocationDs, colors)
  }

  //  Spark implementation.

  case class TempByLocation(location: Location, temperature: Temperature)

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

  def visualizeSpark(temperatures: Dataset[TempByLocation],
                     colors: Iterable[(Temperature, Color)], WIDTH: Int = 360, HEIGHT: Int = 180): Image = {
    val latOffset: Double = 180 / 2
    val lonOffset: Double = -360 / 2

    def computePixels(temps: Dataset[TempByLocation], locations: Dataset[Location]): Dataset[Pixel] = {
      val tempsInterpolated = predictTemperaturesSpark(temps, locations)
        .orderBy($"location.lat".desc, $"location.lon")
        .select($"temp".as[Temperature])
      for {
        temp <- tempsInterpolated
      } yield {
        val color = interpolateColor(colors, temp)
        Pixel(color.red, color.green, color.blue, COLOR_MAX)
      }
    }

    val locations: Dataset[Location] = spark.range(WIDTH * HEIGHT)
      .map(i => Location(latOffset - i / WIDTH, i % WIDTH + lonOffset))
    val pixels = computePixels(temperatures, locations).collect()
    Image(WIDTH, HEIGHT, pixels)
  }

  //  This agggregator with datasets is inefficient
//    private def interpolatedTemp(P: Int = 3) =
//      new Aggregator[(Location, Location, Temperature), (Temperature, Temperature), Temperature] {
//        // Pure ds / df aggregator. Haven't figured out how to use it with DS efficiently
//        override def zero: (Temperature, Temperature) = (0.0, 0.0)
//
//        override def merge(b: (Temperature, Temperature), a: (Distance, Temperature)): (Distance, Temperature) =
//          (a._1 + b._1, a._2 + b._2)
//
//        override def reduce(b1: (Temperature, Temperature), b2: (Location, Location, Temperature)): (Temperature, Temperature) =
//          (b1, b2) match {
//            case ((nomAcc, denomAcc), (xi, location, ui)) =>
//              val d = max(sphereDistance(location, xi), epsilon)
//              val wi = w(location, d, P)
//              (nomAcc + wi * ui, denomAcc + wi)
//          }
//
//        override def finish(reduction: (Temperature, Temperature)): Temperature = reduction._1 / reduction._2
//
//        def bufferEncoder: Encoder[(Distance, Temperature)] =
//          Encoders.tuple(Encoders.scalaDouble, Encoders.scalaDouble)
//
//        def outputEncoder: Encoder[Temperature] = Encoders.scalaDouble
//      }.toColumn

  //  Parallel implementation
  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location,
                         P: Double = 3.0): Temperature = {
    val (nominator, denominator) = temperatures.aggregate((0.0, 0.0))({
      case ((nomAcc: Distance, denomAcc: Temperature), (xi: Location, ui: Temperature)) =>
        val d = max(sphereDistance(location, xi), epsilon)
        val wi = w(location, d, P)
        (nomAcc + wi * ui, denomAcc + wi)
    }, {
      (a: (Temperature, Temperature), b: (Temperature, Temperature)) =>
        (a._1 + b._1, a._2 + b._2)
    })
    nominator / denominator
  }

  def predictTemperatures(temperatures: Iterable[(Location, Temperature)],
                          locations: Iterable[Location]): ParIterable[Temperature] = {
    for {
      location <- locations.par
    } yield predictTemperature(temperatures, location)
  }

  def visualizePar(temperatures: Iterable[(Location, Temperature)],
                   colors: Iterable[(Temperature, Color)], WIDTH: Int = 360, HEIGHT: Int = 180): Image = {
    val latOffset: Double = 180 / 2
    val lonOffset: Double = -360 / 2

    def computePixels(temps: Iterable[(Location, Temperature)], locations: Iterable[Location]): Array[Pixel] = {
      val tempsInterpolated: ParIterable[Temperature] = predictTemperatures(temps, locations)
      val pixels = new Array[Pixel](locations.size)
      for {
        (temp, i) <- tempsInterpolated.zipWithIndex.par
      } {
        val color = interpolateColor(colors, temp)
        pixels(i) = Pixel(color.red, color.green, color.blue, COLOR_MAX)
      }
      pixels
    }

    val locations: Array[Location] = new Array[Location](WIDTH * HEIGHT)
    locations.indices.par
      .foreach(i =>
        locations(i) = Location(latOffset - i / WIDTH, i % WIDTH + lonOffset))
    val pixels = computePixels(temperatures, locations)
    Image(WIDTH, HEIGHT, pixels)
  }
}

