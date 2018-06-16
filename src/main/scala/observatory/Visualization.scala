package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import math._

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  val epsilon = 1E-5
  val PI = 180
  val R = 6372.8

  def equals(a: Location, b: Location): Boolean =
    a.lon - b.lon < epsilon && a.lat - b.lat < epsilon

  private type Distance = Double

  def sphereDistance(a: Location, b: Location): Distance =
    (a, b) match {
      case _ if Location.equals(a, b) => 0
      case _ =>
        R * acos(sin(toRadians(a.lat)) * sin(toRadians(b.lat)) +
          cos(toRadians(a.lat)) * cos(toRadians(b.lat)) * cos(toRadians(b.lon - a.lon)))
    }

  def w(x: Location, d: Distance, xi: Location, p: Double): Temperature =
    1 / math.pow(d, p)

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    predictTemperatures(temperatures, List(location)).head
  }

  def predictTemperatures(temperatures: Iterable[(Location, Temperature)],
                          locations: Iterable[Location]): Iterable[Temperature] = {
    val P = 3.0
    (for {
      location <- locations.par
    } yield {
      val dists = temperatures.par.map {
        case (xi, ui) =>
          val d = sphereDistance(location, xi)
          (d, xi, ui)
      }
      val point: Option[(Distance, Location, Temperature)] = dists.find { case (distance, _, _) => distance == 0 }
      if (point.nonEmpty)
        point.get._3
      else {
        val sumsCompDf = dists.map {
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
    }).seq
  }

  def tempDistance(a: Temperature, b: Temperature): Distance = {
    abs(a - b)
  }

  def findTwoClosest(points: Seq[(Temperature, Color)], temp: Temperature):
  ((Temperature, Color), (Temperature, Color)) = {
    case class ClosePoint(point: (Temperature, Color), distance: Distance)
    val twoClosest: List[ClosePoint] = points
      .foldLeft(List[ClosePoint]()) {
      (agg: List[ClosePoint], point: (Temperature, Color)) => {
        val newDistance = tempDistance(point._1, temp)
        agg match {
          case x :: y :: Nil =>
            if (newDistance < x.distance)
              ClosePoint(point, newDistance) :: y :: Nil
            else if (newDistance < y.distance)
              x :: ClosePoint(point, newDistance) :: Nil
            else
              agg
          case _ => ClosePoint(point, newDistance) :: agg
        }
      }
    }
    val sortedPoints = twoClosest.sortBy(cp => cp.point._1)
    (sortedPoints.head.point, sortedPoints(1).point)
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Seq[(Temperature, Color)], value: Temperature): Color = {
    def boundValue(min: Int, max: Int)(value: Int): Int = math.min(math.max(min, value), max)

    def interpolateComponent(x1: Temperature, x2: Temperature, value: Temperature)(y1: Int, y2: Int): Int =
      math.round(y1 + ((y2 - y1) / (x2 - x1) * (value - x1))).toInt

    val ((x1: Temperature, y1: Color), (x2: Temperature, y2: Color)) = findTwoClosest(points, value)

    def interpolator = interpolateComponent(x1, x2, value)(_, _)

    def bounder = boundValue(0, 255)(_)

    value match {
      case _ if value < x1 => y1
      case _ if value > x2 => y2
      case _ =>  // in between
        val newRed = bounder(interpolator(y1.red, y2.red))
        val newGreen = bounder(interpolator(y1.green, y2.green))
        val newBlue = bounder(interpolator(y1.blue, y2.blue))
        Color(newRed, newGreen, newBlue)
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Seq[(Temperature, Color)]): Image = {
    val WIDTH = 360
    val HEIGHT = 180
    val latOffset: Double = 90
    val lonOffset: Double = -180

    def computePixels(temps: Iterable[(Location, Temperature)], locations: Iterable[Location]): Array[Pixel] = {
      val tempsInterpolated = predictTemperatures(temps, locations)
      val pixels = new Array[Pixel](locations.size)
      for {
        (temp, i) <- tempsInterpolated.zipWithIndex.par
      } {
        val color = interpolateColor(colors, temp)
        pixels(i) = Pixel(color.red, color.green, color.blue, 255)
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

//  def predictTemperatureSpark(temperatures: Dataset[(Location, Temperature)],
//                              locations: Dataset[Location]): Dataset[Temperature] = {
//    val P = 2.0
//    val temps = temperatures.collect()
//    for {
//      location <- locations
//    } yield {
//      val comp = temps.map {
//        case (xi, ui) =>
//          val d = sphereDistance(location, xi)
//          (d, xi, ui)
//      }
//      val point: Option[(Distance, Location, Temperature)] = comp.find { case (distance, _, _) => distance == 0 }
//      if (point.nonEmpty)
//        point.get._3
//      else {
//        val sumsCompDf = comp.map {
//          case (d: Distance, xi: Location, ui: Temperature) =>
//            val wi = w(location, d, xi, P)
//            (wi * ui, wi)
//        }
//        val (nominator: Temperature, denominator: Temperature) = sumsCompDf.reduce {
//          (a: (Temperature, Temperature), b: (Temperature, Temperature)) =>
//            (a._1 + b._1, a._2 + b._2)
//        }
//        nominator / denominator
//      }
//    }
//  }

//  def visualizeSpark(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
//    val WIDTH = 360
//    val HEIGHT = 180
//    val latOffset: Double = -90
//    val lonOffset: Double = -180
//    val tempsDs = spark.createDataset(temperatures.toSeq)
//
//    def computePixels(temps: Dataset[(Location, Temperature)], location: Dataset[Location]): Dataset[Pixel] = {
//      val tempsInterpolated = predictTemperatureSpark(temps, location)
//      for {
//        temp <- tempsInterpolated
//      } yield {
//        val color = interpolateColor(colors, temp)
//        Pixel(color.red, color.green, color.blue, 255)
//      }
//    }
//
//    val locations: Dataset[Location] = spark.range(WIDTH * HEIGHT)
//      .map(i => Location(-(i / WIDTH + latOffset), i % WIDTH + lonOffset)))
//    val pixelsDs = computePixels(tempsDs, locations)
//
//    val pixels = pixelsDs.collect()
//    Image(WIDTH, HEIGHT, pixels)
//  }
}

