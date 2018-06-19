package observatory

import com.sksamuel.scrimage.Image

import scala.math._

trait Interactor {
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
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image
}
