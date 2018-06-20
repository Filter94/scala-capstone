package observatory

import observatory.helpers.Grid
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalactic.Tolerance._

trait Visualization2Test extends FunSuite with Checkers {
  private val epsilon = 1E-05
  test("Bilinear interpolation works correct for the middles") {
    val res = Visualization2.bilinearInterpolation(CellPoint(0.5, 0.5), 0, 10, 0, 10)
    assert(res === 5.0 +- epsilon)
  }

  test("Bilinear interpolation works correct for shifted values") {
    val res = Visualization2.bilinearInterpolation(CellPoint(0.7, 0.5), 0, 10, 0, 10)
    assert(res === 5.0 +- epsilon)
  }

  test("One complex case") {
    val res = Visualization2.bilinearInterpolation(CellPoint(0.7, 0.3), 0, 10, 0, 100)
    assert(res === 21.0 +- 1)
  }

  test("Image test for grid") {
    val temps: Seq[(Location, Temperature)] = Seq(
      (Location(45.0, -90.0), -1.0),
      (Location(-45.0, 0.0), -100.0))
    val colors: Seq[(Temperature, Color)] = Seq(
      (-1.0, Color(255, 0, 0)),
      (-100.0, Color(0, 0, 255)))
    val grid = Grid(temps)
    val tile = Tile(0, 0, 0)
    val image = Visualization2.visualizeGrid(grid.getTemperature, colors, tile)
//    image.output("Pepsi grid.png")
  }

  test("Image test grid with big data") {
    val temperaturesByDate = Extraction.locateTemperaturesSpark(1975, "/stations.csv", "/1975.csv")
    val temperatures = Extraction.locationYearlyAverageRecordsSpark(temperaturesByDate).collect().toIterable
    val colors: Seq[(Temperature, Color)] = Seq(
      (60, Color(255, 255, 255)),
      (32, Color(255, 0, 0)),
      (12, Color(255, 255, 0)),
      (0, Color(0, 255, 255)),
      (-15, Color(0, 0, 255)),
      (-27, Color(255, 0, 255)),
      (-50, Color(33, 255, 107)),
      (-60, Color(0, 0, 0)))
    val grid = Grid(temperatures)
    val tile = Tile(0, 0, 0)
    val image = Visualization2.visualizeGrid(grid.getTemperature, colors, tile)
//    image.output("Big data grid implementation.png")
  }
}
