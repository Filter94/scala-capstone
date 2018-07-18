package observatory

import org.junit.runner.RunWith
import org.scalameter.{Key, Warmer, config}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Benchmarking extends FunSuite {
  test("Average temperature ends in reasonable time on big data") {
    val res = Extraction.locateTemperatures(2018, "/stations.csv", "/1975.csv")
    print("Finished reading.")
    Extraction.locationYearlyAverageRecords(res)
  }

  test("Average temperature ends in reasonable time on big data spark") {
    val res = Extraction.locateTemperaturesSpark(2018, "/stations.csv", "/1975.csv")
    print("Finished reading.")
    Extraction.locationYearlyAverageRecordsSpark(res).collect()
  }

  test("Tile image with big data") {
    val temperaturesByDate = Extraction.locateTemperaturesSpark(1975, "/stations.csv", "/1975.csv")
    val temperatures = Extraction.locationYearlyAverageRecordsSpark(temperaturesByDate).collect()
    val colors: Seq[(Temperature, Color)] = Seq(
      (60, Color(255, 255, 255)),
      (32, Color(255, 0, 0)),
      (12, Color(255, 255, 0)),
      (0, Color(0, 255, 255)),
      (-15, Color(0, 0, 255)),
      (-27, Color(255, 0, 255)),
      (-50, Color(33, 255, 107)),
      (-60, Color(0, 0, 0)))
    val tile = Tile(0, 0, 0)
    val image = Interaction.tile(temperatures, colors, tile)
    image.output(s"tile big data.png")
  }

  test("Grid image with big data") {
    val temperaturesByDate = Extraction.locateTemperatures(1975, "/stations.csv", "/1975.csv")
    val temperatures = Extraction.locationYearlyAverageRecords(temperaturesByDate)
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
    val tile = Tile(0, 0, 1)
    val image = Visualization2.visualizeGrid(grid, colors, tile)
    image.output("Big data grid implementation.png")
  }

  test("Grid implementations effectiveness") {
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

  test("Visualization implementations tile vs grid effectiveness comparison") {
    val standardConfig = config(
      Key.exec.minWarmupRuns -> 20,
      Key.exec.maxWarmupRuns -> 40,
      Key.exec.benchRuns -> 25,
      Key.verbose -> true
    ) withWarmer new Warmer.Default

    val temps: Seq[(Location, Temperature)] = Seq(
      (Location(45.0, -90.0), -1.0),
      (Location(-45.0, 0.0), -100.0))
    val colors: Seq[(Temperature, Color)] = Seq(
      (-1.0, Color(255, 0, 0)),
      (-100.0, Color(0, 0, 255)))
    val tile = Tile(0, 0, 0)

    val tiletime = standardConfig measure {
      Interaction.tile(temps, colors, tile)
      Interaction.tile(temps, colors, tile)
      Interaction.tile(temps, colors, tile)
    }
    println(s"Usual time: $tiletime ms")

    val gridtime = standardConfig measure {
      val grid: GridLocation => Temperature = Manipulation.makeGrid(temps)
      Visualization2.visualizeGrid(grid, colors, tile)
      Visualization2.visualizeGrid(grid, colors, tile)
      Visualization2.visualizeGrid(grid, colors, tile)
    }
    println(s"Grid time: $gridtime ms")
    println(s"speedup: ${tiletime / gridtime}")
  }
}
