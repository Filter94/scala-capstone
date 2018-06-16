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
    val temps: Seq[(Temperature, Color)] = Seq(
      (60, Color(255, 255, 255)),
      (thePoint, theColor))

    val interpolatedColor = Visualization.interpolateColor(temps, thePoint)
    assert(interpolatedColor === theColor)
  }

  test("Interpolation of unknown point returns the point in the middle") {
    val thePoint: Temperature = 5
    val expectedColor = Color(0, 0, 50)
    val colors: Seq[(Temperature, Color)] = Seq(
      (10, Color(0, 0, 100)),
      (0, Color(0, 0, 0)))

    val interpolatedColor = Visualization.interpolateColor(colors, thePoint)
    assert(interpolatedColor === expectedColor)
  }

  test("Interpolation works correct on unsorted data") {
    val thePoint: Temperature = 5
    val expectedColor = Color(0, 0, 50)
    val colors: Seq[(Temperature, Color)] = Seq(
      (10, Color(0, 0, 100)),
      (11, Color(0, 0, 156)),
      (0, Color(0, 0, 0)),
      (-2, Color(0, 255, 101)),
      (17, Color(13, 13, 13)))

    val interpolatedColor = Visualization.interpolateColor(colors, thePoint)
    assert(interpolatedColor === expectedColor)
  }

  test("Interpolation works with exact colors") {
    val thePoint: Temperature = 10
    val expectedColor = Color(0, 0, 100)
    val colors: Seq[(Temperature, Color)] = Seq(
      (thePoint, expectedColor),
      (11, Color(0, 0, 156)),
      (0, Color(0, 0, 0)),
      (-2, Color(0, 255, 101)),
      (17, Color(13, 13, 13)))

    val interpolatedColor = Visualization.interpolateColor(colors, thePoint)
    assert(interpolatedColor === expectedColor)
  }

  test("Interpolation works correct on large numbers") {
    val thePoint: Temperature = 1.073741823E9
    val expectedColor = Color(128,0,128)
    val colors: List[(Temperature, Color)] = List((-1.0, Color(255, 0, 0)), (2.147483647E9, Color(0, 0, 255)))

    val interpolatedColor = Visualization.interpolateColor(colors, thePoint)
    assert(interpolatedColor === expectedColor)
  }

  test("Visualize generates an image with correct size") {
    val expectedWidth = 360
    val expectedHeigth = 180
    val temps: Seq[(Location, Temperature)] = Seq(
      (Location(0, 0), 0),
      (Location(0, 10), 20))
    val colors: Seq[(Temperature, Color)] = Seq(
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
    val temps: Seq[(Location, Temperature)] = Seq(
      (Location(0, 0), 0),
      (Location(0, 10), 20))
    val colors: Seq[(Temperature, Color)] = Seq(
      (10, Color(0, 0, 100)),
      (11, Color(0, 0, 156)),
      (0, Color(0, 0, 0)),
      (-2, Color(0, 255, 101)),
      (17, Color(13, 13, 13)))

    val image = Visualization.visualize(temps, colors)
    assert(image.forall { case (_, _, pixel) => pixel.productIterator.exists(component => component != 0) })
  }

  test("Test gradient") {
    val temps: Seq[(Location, Temperature)] = Seq(
      (Location(90, -180), 0),
      (Location(90, 179), 20),
      (Location(-89, -180), 0),
      (Location(-89, 179), 20),
      (Location(0, -180), 0),
      (Location(0, 179), 20))
    val colors: Seq[(Temperature, Color)] = Seq(
      (0, Color(0, 0, 0)),
      (20, Color(0, 0, 100)))

    val image = Visualization.visualize(temps, colors)
    assert(image.forall { case (x, _, pixel) =>
      val diff = pixel.blue - 50
      (x > 180) == (diff > 0)})
  }

  test("Test locations on image") {
    val temps: Seq[(Location, Temperature)] = Seq(
      (Location(90, -180), 0),
      (Location(90, 179), 20),
      (Location(-89, -180), 0),
      (Location(-89, 179), 20),
      (Location(0, -180), 0),
      (Location(0, 179), 20))
    val colors: Seq[(Temperature, Color)] = Seq(
      (0, Color(0, 0, 0)),
      (20, Color(0, 0, 100)))

    val image = Visualization.visualize(temps, colors)
    assert(image.pixel((0, 90)).blue == 0)
    assert(image.pixel((359, 90)).blue == 100)
  }

  test("Triangle inequality test") {
    val temps: Seq[(Location, Temperature)] = Seq(
      (Location(45.0, -90.0), -1.0),
      (Location(-45.0, 0.0), -100.0))
    val colors: Seq[(Temperature, Color)] = Seq(
      (-1.0, Color(255, 0, 0)),
      (-100.0, Color(0, 0, 255)))
    val image = Visualization.visualize(temps, colors)
    image.output("1.bmp")
    val thePixel = image.pixel((0, 117))
    assert(thePixel.red > thePixel.blue)
  }
}
