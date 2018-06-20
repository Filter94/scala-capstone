package observatory

import observatory.helpers.par.ParInteractor
import observatory.helpers.spark.SparkInteractor

import math._

/**
  * 3rd milestone: interactive visualization
  */
object Interaction extends SparkInteractor {
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
