package observatory.helpers.par

import com.sksamuel.scrimage.{Image, Pixel}
import observatory._
import observatory.helpers.VisualizationMath
import observatory.helpers.VisualizationMath._

object ParVisualizer {
    def computePixels(temps: Iterable[(Location, Temperature)], locations: Iterable[Location],
                      colors: Iterable[(Temperature, Color)], transparency: Int): Array[Pixel] = {
      val colorsSorted = sortPoints(colors.toSeq)
      val tempsInterpolated: Iterable[Temperature] = predictTemperatures(temps, locations)
      val pixels = new Array[Pixel](locations.size)
      for {
        (temp, i) <- tempsInterpolated.zipWithIndex.par
      } {
        val color = interpolateColor(colorsSorted, temp)
        pixels(i) = Pixel(color.red, color.green, color.blue, transparency)
      }
      pixels
    }

    def locationsGenerator(WIDTH: Int, HEIGHT: Int)(i: Int): Location = {
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

  def sortPoints(colors: Seq[(Temperature, Color)]): Seq[(Temperature, Color)] = {
    colors.sortBy{case (temp, _) => temp}
  }

  val epsilon = 1E-5
  val DEFAULT_P = 3.0

  def interpolateColor(points: Seq[(Temperature, Color)], value: Temperature): Color = {
    VisualizationMath.interpolateColor(sortPoints(points), value)
  }

  def predictTemperatures(temperatures: Iterable[(Location, Temperature)],
                          locations: Iterable[Location]): Iterable[Temperature] = {
    for {
      location <- locations
    } yield predictTemperature(temperatures, location)
  }

  def visualize(WIDTH: Int, HEIGHT: Int, transparency: Int = COLOR_MAX)
               (temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)])
               (locationsGenerator: (Int, Int) => Int => Location,
                computePixels: (Iterable[(Location, Temperature)], Iterable[Location],
                  Iterable[(Temperature, Color)], Int) => Array[Pixel]): Image = {
    val locations = Range(0, WIDTH * HEIGHT).par
      .map(i => locationsGenerator(WIDTH, HEIGHT)(i)).seq
    val pixels = computePixels(temperatures, locations, colors, transparency)
    Image(WIDTH, HEIGHT, pixels)
  }

  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image =
    visualize(360, 180)(temperatures, colors)(locationsGenerator, computePixels)

  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature =
    predictTemperature(temperatures, location, DEFAULT_P)

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location,
                         P: Double = DEFAULT_P): Temperature = {
    val (nominator, denominator) = temperatures.par.aggregate((0.0, 0.0))({
      case ((nomAcc: Distance, denomAcc: Temperature), (xi: Location, ui: Temperature)) =>
        val d = sphereDistance(location, xi) max epsilon
        val wi = w(location, d, P)
        (nomAcc + wi * ui, denomAcc + wi)
    }, {
      (a: (Temperature, Temperature), b: (Temperature, Temperature)) =>
        (a._1 + b._1, a._2 + b._2)
    })
    nominator / denominator
  }
}
