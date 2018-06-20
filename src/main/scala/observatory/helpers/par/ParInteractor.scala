package observatory.helpers.par

import com.sksamuel.scrimage.Image
import observatory._
import observatory.helpers.Interactor

import scala.math._

trait ParInteractor extends Interactor with ParVisualizer {
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val IMAGE_SIZE_X = 256
    val IMAGE_SIZE_Y = 256
    visualizeTile(IMAGE_SIZE_X, IMAGE_SIZE_Y)(temperatures, colors, tile)
  }

  def visualizeTile(sizeX: Int, sizeY: Int)(temperatures: Iterable[(Location, Temperature)],
                                            colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    import Implicits.computePixels
    implicit def locationsGenerator(width: Int, height: Int)(i: Int): Location = {
      val latIdx = i / width
      val lonIdx = i % width
      val precision = (log(width) / log(2)).toInt
      val targetZoom = precision
      val xStart = (pow(2, precision) * tile.x).toInt
      val yStart = (pow(2, precision) * tile.y).toInt
      val zoomedTile = Tile(xStart + lonIdx, yStart + latIdx, targetZoom + tile.zoom)
      zoomedTile.location
    }
    val upscaleFactor = 2
    visualize(sizeX / upscaleFactor, sizeY / upscaleFactor, 127)(temperatures, colors).scale(upscaleFactor)
  }
}
