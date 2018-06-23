package observatory.helpers.par

import com.sksamuel.scrimage.{Image, Pixel}
import observatory._
import observatory.helpers.Interactor

import scala.collection.parallel.immutable.ParIterable
import scala.math._
import ParVisualizer.Implicits.computePixels

object ParInteractor extends Interactor {
  val IMAGE_SIZE_X = 256
  val IMAGE_SIZE_Y = 256

  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    visualizeTile(IMAGE_SIZE_X, IMAGE_SIZE_Y)(temperatures, colors, tile)
  }

  def visualizeTile(sizeX: Int, sizeY: Int)
                       (temperatures: Iterable[(Location, Temperature)],
                        colors: Iterable[(Temperature, Color)], tile: Tile)
                       (implicit computePixels: (Iterable[(Location, Temperature)], ParIterable[Location],
                         Iterable[(Temperature, Color)], Int) => Array[Pixel]): Image = {
    val targetZoom = (log(sizeX) / log(2)).toInt
    val zoomedTiles = pow(2, targetZoom).toInt
    val xStart = zoomedTiles * tile.x
    val yStart = zoomedTiles * tile.y

    implicit def locationsGenerator(WIDTH: Int, HEIGHT: Int)(i: Int): Location = {
      val latIdx = i / WIDTH
      val lonIdx = i % WIDTH
      val zoomedTile = Tile(xStart + lonIdx, yStart + latIdx, targetZoom + tile.zoom)
      zoomedTile.location
    }
    ParVisualizer.visualize(sizeX, sizeY, 127)(temperatures, colors)
  }
}
