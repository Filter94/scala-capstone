package observatory

import java.io.File

import com.sksamuel.scrimage.Image

object Main extends App {
//  val colors: Seq[(Temperature, Color)] = Seq(
//    (60, Color(255, 255, 255)),
//    (32, Color(255, 0, 0)),
//    (12, Color(255, 255, 0)),
//    (0, Color(0, 255, 255)),
//    (-15, Color(0, 0, 255)),
//    (-27, Color(255, 0, 255)),
//    (-50, Color(33, 255, 107)),
//    (-60, Color(0, 0, 0)))
//  def generateImage(year: Year, tile: Tile, data: Iterable[(Location, Temperature)]): Unit = {
//    val image: Image = Interaction.tile(data, colors, tile)
//    val directory = s"./target/temperatures/$year/${tile.zoom}/"
//    new File(directory).mkdirs()
//    val fileName = s"${tile.x}-${tile.y}.png"
//    val file = new File(directory + fileName)
//    image.output(file)
//  }
//  val tempsByLocations = Extraction.locateTemperaturesSpark(1975, "/stations.csv", "/1975.csv")
//  val data = Extraction.locationYearlyAverageRecordsSpark(tempsByLocations).collect().toIterable
//  val yearlyData = Iterable((1975, data))
//  Interaction.generateTiles(yearlyData, generateImage)
}
