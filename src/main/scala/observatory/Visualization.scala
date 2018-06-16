package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.sql.Dataset
import SparkContextKeeper.spark

import math._

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  import spark.implicits._

  val R = 1
  val epsilon = 1E-5
  val PI = 180

  def equals(a: Location, b: Location): Boolean =
    a.lon - b.lon < epsilon && a.lat - b.lat < epsilon

  private type Distance = Double

  def sphereDistance(a: Location, b: Location): Distance =
    (a, b) match {
      case _ if Location.equals(a, b) => 0
      case _ if Location.equals(a, Location(-b.lat, -b.lon)) => PI
      case _ => R * acos(sin(a.lat) * sin(b.lat) + cos(a.lat) * cos(b.lat) * cos(a.lon - b.lon))
    }

  def w(x: Location, d: Distance, xi: Location, p: Double): Temperature =
    1 / math.pow(d, p)

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val temps = spark.createDataset(temperatures.toSeq)
    val res = predictTemperatureSpark(temps, spark.createDataset(List(location)))
    res.collect().head
  }

  def predictTemperatureSpark(temperatures: Dataset[(Location, Temperature)],
                              locations: Dataset[Location]): Dataset[Temperature] = {
    val P = 2.0
    val temps = temperatures.collect()
    for {
      location <- locations
    } yield {
      val comp = temps.map {
        case (xi, ui) =>
          val d = sphereDistance(location, xi)
          (d, xi, ui)
      }
      val point: Option[(Distance, Location, Temperature)] = comp.find { case (distance, _, _) => distance == 0 }
      if (point.nonEmpty)
        point.get._3
      else {
        val sumsCompDf = comp.map {
          case (d: Distance, xi: Location, ui: Temperature) =>
            val wi = w(location, d, xi, P)
            (wi * ui, wi)
        }
        val (nominator: Temperature, denominator: Temperature) = sumsCompDf.reduce {
          (a: (Temperature, Temperature), b: (Temperature, Temperature)) =>
            (a._1 + b._1, a._2 + b._2)
        }
        nominator / denominator
      }
    }
  }

  def tempDistance(a: Temperature, b: Temperature): Distance = {
    abs(a - b)
  }

  def findTwoClosest(points: Iterable[(Temperature, Color)], temp: Temperature):
  ((Temperature, Color), (Temperature, Color)) = {
    case class ClosePoint(point: (Temperature, Color), distance: Distance)
    val twoClosest: List[ClosePoint] = points.foldLeft(List[ClosePoint]()) {
      (agg: List[ClosePoint], point: (Temperature, Color)) => {
        val newDistance = tempDistance(point._1, temp)
        agg match {
          case x :: y :: Nil =>
            if (newDistance < x.distance)
              ClosePoint(point, newDistance) :: y :: Nil
            else if (newDistance < x.distance)
              x :: ClosePoint(point, newDistance) :: Nil
            else
              agg
          case _ => ClosePoint(point, newDistance) :: agg
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
    val ((x1: Temperature, y1: Color), (x2: Temperature, y2: Color)) = findTwoClosest(points, value)
    (y1 * (x2 - value) + y2 * (value - x1)) / (x2 - x1)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    visualizeSpark(temperatures, colors)
  }

  def visualizeSpark(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val WIDTH = 360
    val HEIGHT = 180
    val latOffset: Double = 90
    val lonOffset: Double = -180
    val tempsDs = spark.createDataset(temperatures.toSeq)

    def computePixels(temps: Dataset[(Location, Temperature)], location: Dataset[Location]): Dataset[Pixel] = {
      val tempsInterpolated = predictTemperatureSpark(temps, location)
      for {
        temp <- tempsInterpolated
      } yield {
        val color = interpolateColor(colors, temp)
        Pixel(color.red, color.green, color.blue, 255)
      }
    }

    val locations: Dataset[Location] = spark.range(WIDTH * HEIGHT)
      .map(i => Location(i / HEIGHT + latOffset, i % HEIGHT + lonOffset))
    val pixelsDs = computePixels(tempsDs, locations)

    val pixels = pixelsDs.collect()
    Image(WIDTH, HEIGHT, pixels)
  }
}

