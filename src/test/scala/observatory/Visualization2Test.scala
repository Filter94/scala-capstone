package observatory

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

  test("Corners are ok") {
    val d00 = 0.0
    val d01 = 10.0
    val d10 = -1.0
    val d11 = -10.0
    def performTest(cellPoint: CellPoint): Temperature =
      Visualization2.bilinearInterpolation(cellPoint, d00, d01, d10, d11)
    assert(performTest(CellPoint(0, 0)) === d00 +- epsilon, "Top left corners is incorrect")
    assert(performTest(CellPoint(1, 1)) === d11 +- epsilon, "Bottom right corner is incorrect")
    assert(performTest(CellPoint(1, 0)) === d10 +- epsilon, "Top right corner is incorrect")
    assert(performTest(CellPoint(0, 1)) === d01 +- epsilon, "Bottom left corner is incorrect")
  }

  test("Image test for grid") {
    val temperatures: Seq[(Location, Temperature)] = Seq(
      (Location(45.0, -90.0), 60.0),
      (Location(-45.0, 0.0), -60.0))
    val colors: Seq[(Temperature, Color)] = Seq(
      (60, Color(255, 255, 255)),
      (32, Color(255, 0, 0)),
      (12, Color(255, 255, 0)),
      (0, Color(0, 255, 255)),
      (-15, Color(0, 0, 255)),
      (-27, Color(255, 0, 255)),
      (-50, Color(33, 255, 107)),
      (-60, Color(0, 0, 0)))
    val grid: GridLocation => Temperature = Manipulation.makeGrid(temperatures)
    val tile = Tile(0, 0, 0)
    val image = Visualization2.visualizeGrid(grid, colors, tile)
    image.output("Pepsi grid.png")
  }
}
