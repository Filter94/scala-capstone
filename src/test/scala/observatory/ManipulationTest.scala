package observatory

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalactic.Tolerance._

trait ManipulationTest extends FunSuite with Checkers {
  private val epsilon = 1E-05

  test("Grid constructs and works") {
    val temps: Iterable[(Location, Temperature)] = Seq(
      (Location(90, -180), 0),
      (Location(90, 179), 20),
      (Location(-89, -180), 0),
      (Location(-89, 179), 20),
      (Location(0, -180), 0),
      (Location(0, 179), 20))
    val grid = Manipulation.makeGrid(temps)
    assert(grid(GridLocation(0, 0)) === (10.0 +- 0.01))
    assert(grid(GridLocation(0, -180)) === (0.0 +- epsilon))
    assert(grid(GridLocation(0, 179)) === (20.0 +- epsilon))
    assert(grid(GridLocation(90, 179)) === (10.0 +- epsilon))  // contradiction in data
  }

  test("Deviation constructs and works") {
    val tempsNormal: Iterable[(Location, Temperature)] = Seq(
      (Location(90, -180), 0),
      (Location(90, 179), 20),
      (Location(-89, -180), 0),
      (Location(-89, 179), 20),
      (Location(0, -180), 0),
      (Location(0, 179), 20))
    val newTemps: Iterable[(Location, Temperature)] = Seq(
      (Location(90, -180), 10),
      (Location(90, 179), 30),
      (Location(-89, -180), 10),
      (Location(-89, 179), 30),
      (Location(0, -180), 10),
      (Location(0, 179), 30))
    val normalGrid = Manipulation.makeGrid(tempsNormal)
    val deviationGrid = Manipulation.deviation(newTemps, normalGrid)
    assert(deviationGrid(GridLocation(0, 0)) === (10.0 +- epsilon))
    assert(deviationGrid(GridLocation(0, -180)) === (10.0 +- epsilon))
    assert(deviationGrid(GridLocation(0, 179)) === (10.0 +- epsilon))
    assert(deviationGrid(GridLocation(90, 179)) === (10.0 +- epsilon))  // contradiction in data
  }
}