package observatory

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalactic.Tolerance._
import org.scalameter.{Key, Warmer, config}

trait ManipulationTest extends FunSuite with Checkers {
  private val epsilon = 0.5

  test("Grid constructs and works") {
    val temps: Iterable[(Location, Temperature)] = Seq(
      (Location(0, -180), 0),
      (Location(0, 179), 20))
    val grid = Manipulation.makeGrid(temps)
    assert(grid(GridLocation(0, 0)) === (10.0 +- epsilon))
    assert(grid(GridLocation(0, -180)) === (0.0 +- epsilon))
    assert(grid(GridLocation(0, 179)) === (20.0 +- epsilon))
    assert(grid(GridLocation(0, 60)) === (10.0 +- epsilon))
  }

  test("Average grid works") {
    val temps: Iterable[Iterable[(Location, Temperature)]] = Seq(
      Seq(
        (Location(90, -180), 0),
        (Location(30, 60), 20),
        (Location(-89, -180), 0),
        (Location(-89, 179), 20),
        (Location(0, -180), 0),
        (Location(0, 179), 20)),
      Seq(
        (Location(90, -180), 2),
        (Location(30, 60), 22),
        (Location(-89, -180), 2),
        (Location(-89, 179), 40),
        (Location(0, -180), 2),
        (Location(0, 179), 20)))
    val averageGrid = Manipulation.average(temps)
    assert(averageGrid(GridLocation(90, -180)) === (1.0 +- epsilon))
    assert(averageGrid(GridLocation(30, 60)) === (21.0 +- epsilon))
    assert(averageGrid(GridLocation(-89, 179)) === (30.0 +- epsilon))
    assert(averageGrid(GridLocation(0, 179)) === (20.0 +- epsilon))
  }

  test("Deviation  works") {
    val tempsNormal: Iterable[(Location, Temperature)] = Seq(
      (Location(30, 60), 20),
      (Location(90, 179), 20),
      (Location(-89, -180), 0),
      (Location(-89, 179), 20),
      (Location(0, -180), 0),
      (Location(0, 179), 20))
    val newTemps: Iterable[(Location, Temperature)] = Seq(
      (Location(30, 60), 10),
      (Location(90, 179), 30),
      (Location(-89, -180), 10),
      (Location(-89, 179), 30),
      (Location(0, -180), 10),
      (Location(0, 179), 40))
    val normalGrid = Manipulation.makeGrid(tempsNormal)
    val deviationGrid = Manipulation.deviation(newTemps, normalGrid)
    assert(deviationGrid(GridLocation(30, 60)) === (-10.0 +- epsilon))
    assert(deviationGrid(GridLocation(0, -180)) === (10.0 +- epsilon))
    assert(deviationGrid(GridLocation(0, 179)) === (20.0 +- epsilon))
    assert(deviationGrid(GridLocation(90, 179)) === (10.0 +- epsilon))
  }

  test("Grid implementations effectiveness comparison") {
    val standardConfig = config(
      Key.exec.minWarmupRuns -> 20,
      Key.exec.maxWarmupRuns -> 40,
      Key.exec.benchRuns -> 25,
      Key.verbose -> true
    ) withWarmer new Warmer.Default

    val temps: Iterable[Iterable[(Location, Temperature)]] = Seq(
      Seq(
        (Location(90, -180), 0),
        (Location(30, 60), 20),
        (Location(-89, -180), 0),
        (Location(-89, 179), 20),
        (Location(0, -180), 0),
        (Location(0, 179), 20)),
      Seq(
        (Location(90, -180), 2),
        (Location(30, 60), 22),
        (Location(-89, -180), 2),
        (Location(-89, 179), 40),
        (Location(0, -180), 2),
        (Location(0, 179), 20)))
    val colors: Seq[(Temperature, Color)] = Seq(
      (-1.0, Color(255, 0, 0)),
      (-100.0, Color(0, 0, 255)))
    val tile = Tile(0, 0, 0)

    val time = standardConfig measure {
      val grid: GridLocation => Temperature = Manipulation.average(temps)
      Visualization2.visualizeGrid(grid, colors, tile)
      Visualization2.visualizeGrid(grid, colors, tile)
      Visualization2.visualizeGrid(grid, colors, tile)
    }
    println(s"Grid time: $time ms")
  }
}