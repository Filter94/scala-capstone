package observatory.common.generators

import observatory.common.ImageConfiguration
import observatory.{Location, Tile}

import scala.math.{log, pow}

object TileLocationGenerator {
  def apply(imageConfiguration: ImageConfiguration, tile: Tile): TileLocationGenerator =
    new TileLocationGenerator(imageConfiguration, tile)
}

class TileLocationGenerator(private val imageConfiguration: ImageConfiguration, private val  tile: Tile)
  extends LocationGenerator {
  private val targetZoom = (log(imageConfiguration.width) / log(2)).toInt
  private val zoomedTiles = pow(2, targetZoom).toInt
  private val xStart = zoomedTiles * tile.x
  private val yStart = zoomedTiles * tile.y

  def get(i: Int): Location = {
    val latIdx = i / imageConfiguration.width
    val lonIdx = i % imageConfiguration.width
    val zoomedTile = Tile(xStart + lonIdx, yStart + latIdx, targetZoom + tile.zoom)
    zoomedTile.location
  }
}
