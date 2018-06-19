package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.sql._

import math._
import scala.collection.parallel.ParIterable
import SparkContextKeeper.spark
import org.apache.spark.rdd.RDD

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  import spark.implicits._

  object implicits {
    implicit def locationsGenerator(WIDTH: Int, HEIGHT: Int)(i: Long): Location = {
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


    implicit def computePixelsPar(temps: Iterable[(Location, Temperature)], locations: ParIterable[Location],
                                  colors: Iterable[(Temperature, Color)], transparency: Int): Array[Pixel] = {
      val tempsInterpolated: ParIterable[Temperature] = predictTemperatures(temps, locations)
      val pixels = new Array[Pixel](locations.size)
      for {
        (temp, i) <- tempsInterpolated.zipWithIndex.par
      } {
        val color = interpolateColor(colors, temp)
        pixels(i) = Pixel(color.red, color.green, color.blue, transparency)
      }
      pixels
    }

    implicit def computePixelsSpark(temps: Dataset[TempByLocation], locations: Dataset[Location],
                                    colors: Iterable[(Temperature, Color)], transparency: Int): RDD[Pixel] = {
      val tempsInterpolated = predictTemperaturesSpark(temps, locations)
        .orderBy($"location.lat".desc, $"location.lon")
        .select($"temp".as[Temperature])
      (for {
        temp <- tempsInterpolated
      } yield {
        val color = interpolateColor(colors, temp)
        Pixel(color.red, color.green, color.blue, transparency)
      }).rdd
    }

    implicit def interpolateComponent(x1: Temperature, x2: Temperature, value: Temperature)(y1: Int, y2: Int): Int =
      math.round(y1 + ((y2 - y1) / (x2 - x1) * (value - x1))).toInt

    implicit def tempDistance(a: Temperature, b: Temperature): Distance = abs(a - b)
  }

  import implicits._

  private val epsilon = 1E-5
  private val R = 6372.8
  private val COLOR_MAX = 255
  private type Distance = Double

  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image =
    visualizeSparkWrapper()(temperatures, colors)

  def sphereDistance(a: Location, b: Location): Distance =
    (a, b) match {
      case _ if Location.equals(a, b) => 0
      case _ =>
        val delta = (a.lon max b.lon) - (a.lon min b.lon)
        R * acos(sin(toRadians(a.lat)) * sin(toRadians(b.lat)) +
          cos(toRadians(a.lat)) * cos(toRadians(b.lat)) * cos(toRadians(delta)))
    }

  def w(x: Location, d: Distance, p: Double): Temperature = 1 / math.pow(d, p)

  def findTwoClosest(points: Iterable[(Temperature, Color)], temp: Temperature)
                    (implicit tempDistance: (Temperature, Temperature) => Distance):
  ((Temperature, Color), (Temperature, Color)) = {
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
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature)
                      (implicit interpolateComponent: (Temperature, Temperature, Temperature) => (Int, Int) => Int): Color = {
    def boundValue(min: Int, max: Int)(value: Int): Int = math.min(math.max(min, value), max)

    def bounder = boundValue(0, COLOR_MAX)(_)

    val ((x1: Temperature, y1: Color), (x2: Temperature, y2: Color)) = findTwoClosest(points, value)

    def interpolator = interpolateComponent(x1, x2, value)(_, _)

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
  def visualizeSparkWrapper(WIDTH: Int = 360, HEIGHT: Int = 180)(temperatures: Iterable[(Location, Temperature)],
                                                                 colors: Iterable[(Temperature, Color)]): Image = {
    val tempByLocationDs = spark.createDataset(temperatures.map {
      case (location, temp) => TempByLocation(location, temp)
    }.toSeq)
    visualizeSpark(WIDTH, HEIGHT)(tempByLocationDs, colors)
  }

  //  Spark implementation.

  def predictTemperaturesSpark(temperatures: Dataset[TempByLocation],
                               locations: Dataset[Location], P: Double = 3): Dataset[(Location, Temperature)] = {
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
  def visualizeSpark(WIDTH: Int, HEIGHT: Int, transparency: Int = COLOR_MAX)
                    (temperatures: Dataset[TempByLocation], colors: Iterable[(Temperature, Color)])
                    (implicit locationsGenerator: (Int, Int) => Long => Location,
                     computePixelsSpark: (Dataset[TempByLocation], Dataset[Location],
                       Iterable[(Temperature, Color)], Int) => RDD[Pixel]): Image = {
    val locations = spark.range(WIDTH * HEIGHT)
      .map(i => locationsGenerator(WIDTH, HEIGHT)(i))
    val pixels = computePixelsSpark(temperatures, locations, colors, transparency).collect()
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
                          locations: ParIterable[Location]): ParIterable[Temperature] = {
    for {
      location <- locations.par
    } yield predictTemperature(temperatures, location)
  }

  def visualizePar(WIDTH: Int, HEIGHT: Int, transparency: Int = COLOR_MAX)
                  (temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)])
                  (implicit locationsGenerator: (Int, Int) => Int => Location,
                   computePixelsPar: (Iterable[(Location, Temperature)], ParIterable[Location],
                     Iterable[(Temperature, Color)], Int) => Array[Pixel]): Image = {
    val locations = Range(0, WIDTH * HEIGHT).par
      .map(i => locationsGenerator(WIDTH, HEIGHT)(i))
    val pixels = computePixelsPar(temperatures, locations, colors, transparency)
    Image(WIDTH, HEIGHT, pixels)
  }
}

