package observatory


import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait VisualizationTest extends FunSuite with Checkers {
  test("Prediction of an known point returns the point") {
    val thePoint = Location(37.35, -78.433)
    val theTemp = 27.3
    val temps = Iterable(
      (thePoint, theTemp),
    (Location(37.358, -78.438), 1.0))

    val predictedTemp = Visualization.predictTemperature(temps, thePoint)
    assert(predictedTemp === theTemp)
  }

  test("Prediction of unknown point returns a new point") {
    val thePoint = Location(0, 5)
    val expectedTemp = 10
    val temps: Iterable[(Location, Temperature)] = Iterable(
      (Location(0, 0), 0),
      (Location(0, 10), 20))

    val predictedTemp = Visualization.predictTemperature(temps, thePoint)
    assert(predictedTemp === expectedTemp)
  }

  test("Interpolation of an known point returns the point") {
    val thePoint: Temperature = 32
    val theColor = Color(255, 0, 0)
    val temps: Iterable[(Temperature, Color)] = Iterable(
      (60, Color(255, 255, 255)),
      (thePoint, theColor))

    val interpolatedColor = Visualization.interpolateColor(temps, thePoint)
    assert(interpolatedColor === theColor)
  }

  test("Interpolation of unknown point returns the point in the middle") {
    val thePoint: Temperature = 5
    val expectedColor = Color(0, 0, 50)
    val colors: Iterable[(Temperature, Color)] = Iterable(
      (10, Color(0, 0, 100)),
      (0, Color(0, 0, 0)))

    val interpolatedColor = Visualization.interpolateColor(colors, thePoint)
    assert(interpolatedColor === expectedColor)
  }

  test("Interpolation works correct on unsorted data") {
    val thePoint: Temperature = 5
    val expectedColor = Color(0, 0, 50)
    val colors: Iterable[(Temperature, Color)] = Iterable(
      (10, Color(0, 0, 100)),
      (11, Color(0, 0, 156)),
      (0, Color(0, 0, 0)),
      (-2, Color(0, 255, 101)),
      (17, Color(13, 13, 13)))

    val interpolatedColor = Visualization.interpolateColor(colors, thePoint)
    assert(interpolatedColor === expectedColor)
  }

  test("Visualize generates an image with correct size") {
    val expectedWidth = 360
    val expectedHeigth = 180
    val temps: Iterable[(Location, Temperature)] = Iterable(
      (Location(0, 0), 0),
      (Location(0, 10), 20))
    val colors: Iterable[(Temperature, Color)] = Iterable(
      (10, Color(0, 0, 100)),
      (11, Color(0, 0, 156)),
      (0, Color(0, 0, 0)),
      (-2, Color(0, 255, 101)),
      (17, Color(13, 13, 13)))

    val image = Visualization.visualize(temps, colors)
    assert(image.width === expectedWidth)
    assert(image.height === expectedHeigth)
  }

  test("Each point is not empty") {
    val temps: Iterable[(Location, Temperature)] = Iterable(
      (Location(0, 0), 0),
      (Location(0, 10), 20))
    val colors: Iterable[(Temperature, Color)] = Iterable(
      (10, Color(0, 0, 100)),
      (11, Color(0, 0, 156)),
      (0, Color(0, 0, 0)),
      (-2, Color(0, 255, 101)),
      (17, Color(13, 13, 13)))

    val image = Visualization.visualize(temps, colors)
    assert(image.forall{case (_, _, pixel) => pixel.productIterator.exists(component => component != 0)})
  }
}
