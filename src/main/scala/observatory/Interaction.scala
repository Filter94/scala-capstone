package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import math._
import SparkContextKeeper.spark
import org.apache.spark.sql.Dataset

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {
  import spark.implicits._

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    val n = pow(2.0, tile.zoom)
    val lon = tile.x / n * 360.0 - 180.0
    val lat = toDegrees(atan(sinh(Pi * (1 - 2 * (tile.y / n)))))
    Location(lat, lon)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val IMAGE_SIZE_X = 256
    val IMAGE_SIZE_Y = 256
    tilePar(temperatures, colors, tile)(IMAGE_SIZE_X, IMAGE_SIZE_Y)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tileSpark(temperatures: Dataset[TempByLocation], colors: Iterable[(Temperature, Color)], tile: Tile)
               (IMAGE_SIZE_X: Int, IMAGE_SIZE_Y: Int): Image = {
    val startLocation = tileLocation(tile)
    val endLocation = tileLocation(Tile(tile.x + 1, tile.y + 1, tile.zoom))
    val latLength = startLocation.lat - endLocation.lat
    val lonLength = endLocation.lon - startLocation.lon
    Visualization.visualizeSpark(temperatures, colors)(startLocation.lat, latLength, startLocation.lon, lonLength,
      IMAGE_SIZE_X, IMAGE_SIZE_Y, 127)
  }

  def tilePar(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile)
               (IMAGE_SIZE_X: Int, IMAGE_SIZE_Y: Int): Image = {
    val startLocation = tileLocation(tile)
    val endLocation = tileLocation(Tile(tile.x + 1, tile.y + 1, tile.zoom))
    val latLength = startLocation.lat - endLocation.lat
    val lonLength = endLocation.lon - startLocation.lon
    Visualization.visualizePar(temperatures, colors)(startLocation.lat, latLength, startLocation.lon, lonLength,
      IMAGE_SIZE_X, IMAGE_SIZE_Y, 127)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    for {
      (year, data) <- yearlyData.par
      zoomLevel <- Range(0, 4).par
      tileX <- Range(0, pow(2, zoomLevel).toInt).par
      tileY <- Range(0, pow(2, zoomLevel).toInt).par
    } {
//      println(s"Processing year $year, zoom level: $zoomLevel, x: $tileX, y: $tileY")
      val tile = Tile(tileX, tileY, zoomLevel)
      generateImage(year, tile, data)
    }
  }
}
