package observatory

import com.sksamuel.scrimage.Image
import observatory.helpers.VisualizerConfiguration
import observatory.helpers.generators.TileLocationsGenerator
import observatory.helpers.visualizers.par.Visualizer

import math.{pow, _}

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {
  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = tile.location

  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val upscaleFactor = 1
    val width = 256 / upscaleFactor
    val height = 256 / upscaleFactor
    val transparency = 127
    val configuration = new VisualizerConfiguration.Builder()
      .setWidth(width)
      .setHeight(height)
      .setTransparency(transparency)
      .setLocationsGenerator(TileLocationsGenerator(width, height, tile))
      .build
    Visualizer(temperatures, colors, configuration).visualize()
      .scale(upscaleFactor)
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
      subtiles = pow(2, zoomLevel).toInt
      tileX <- Range(0, subtiles).par
      tileY <- Range(0, subtiles).par
    } {
//      println(s"Processing year $year, zoom level: $zoomLevel, x: $tileX, y: $tileY")
      val tile = Tile(tileX, tileY, zoomLevel)
      generateImage(year, tile, data)
    }
  }
}
