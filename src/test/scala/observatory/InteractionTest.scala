package observatory

import java.io.File

import com.sksamuel.scrimage.Image
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers


trait InteractionTest extends FunSuite with Checkers {
  test("Tile to location primitive test wo zoom") {
    val tile = Tile(0, 0, 1)
    val expectedLocation = Location(85.05112877980659, -180)
    assert(expectedLocation === Interaction.tileLocation(tile))
  }

  test("We can get 90 degrees in principle") {
    val tile = Tile(0, Int.MinValue, 1)
    val expectedLocation = Location(90, -180)
    assert(expectedLocation === Interaction.tileLocation(tile))
  }

  test("Tile image test") {
    val temps: Seq[(Location, Temperature)] = Seq(
      (Location(45.0, -90.0), -1.0),
      (Location(-45.0, 0.0), -100.0))
    val colors: Seq[(Temperature, Color)] = Seq(
      (-1.0, Color(255, 0, 0)),
      (-100.0, Color(0, 0, 255)))
    val tile = Tile(0, 0, 0)
    val image = Interaction.tile(temps, colors, tile)
    image.output(s"tile.png")
  }

  test("Generate tiles") {
    val colors: Seq[(Temperature, Color)] = Seq(
      (60, Color(255, 255, 255)),
      (32, Color(255, 0, 0)),
      (12, Color(255, 255, 0)),
      (0, Color(0, 255, 255)),
      (-15, Color(0, 0, 255)),
      (-27, Color(255, 0, 255)),
      (-50, Color(33, 255, 107)),
      (-60, Color(0, 0, 0)))
    def generateImage(year: Year, tile: Tile, data: Iterable[(Location, Temperature)]): Unit = {
      val image: Image = Interaction.tile(data, colors, tile)
      val directory = s"./target/temperatures/$year/${tile.zoom}/"
      new File(directory).mkdirs()
      val fileName = s"${tile.x}-${tile.y}.png"
      val file = new File(directory + fileName)
      image.output(file)
    }
    val temsByLocations = Extraction.locateTemperaturesSpark(1975, "/stations.csv", "/1975.csv")
    val data = Extraction.locationYearlyAverageRecordsSpark(temsByLocations).collect().toIterable
    val yearlyData = Iterable((1975, data))
    Interaction.generateTiles(yearlyData, generateImage)
  }
}
