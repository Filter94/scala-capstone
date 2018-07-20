package observatory.visualizers.common.generators

import observatory.{Location, Tile}

import scala.math.{log, pow}

object TileLocationsGenerator {
  def apply(width: Int, height: Int, tile: Tile): TileLocationsGenerator =
    new TileLocationsGenerator(width, height, tile)
}

class TileLocationsGenerator(width: Int, height: Int, tile: Tile) extends LocationsGenerator with Serializable {
  private val targetZoom = (log(width) / log(2)).toInt
  private val zoomedTiles = pow(2, targetZoom).toInt
  private val xStart = zoomedTiles * tile.x
  private val yStart = zoomedTiles * tile.y

  def get(i: Int): Location = {
    val latIdx = i / width
    val lonIdx = i % width
    val zoomedTile = Tile(xStart + lonIdx, yStart + latIdx, targetZoom + tile.zoom)
    zoomedTile.location
  }
}
