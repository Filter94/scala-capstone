package observatory

import com.sksamuel.scrimage.Image

import scala.math.{log, pow}


trait SparkInteractor extends Interactor with SparkVisualizer {

  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val IMAGE_SIZE_X = 256
    val IMAGE_SIZE_Y = 256
    visualizeTile(IMAGE_SIZE_X, IMAGE_SIZE_Y)(temperatures, colors, tile)
  }

  def visualizeTile(IMAGE_SIZE_X: Int, IMAGE_SIZE_Y: Int)(temperatures: Iterable[(Location, Temperature)],
                                                      colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val upscaleFactor = 2
    implicit def locationsGenerator(WIDTH: Int, HEIGHT: Int)(i: Long): Location = {
      val latIdx = (i / WIDTH).toInt
      val lonIdx = (i % WIDTH).toInt
      val precision = (log(WIDTH) /  log(2)).toInt
      val targetZoom = precision
      val xStart = (pow(2, precision) * tile.x).toInt
      val yStart = (pow(2, precision) * tile.y).toInt
      val zoomedTile = Tile(xStart + lonIdx, yStart + latIdx, targetZoom + tile.zoom)
      tileLocation(zoomedTile)
    }
    visualize(IMAGE_SIZE_X / upscaleFactor, IMAGE_SIZE_Y / upscaleFactor, 127)(temperatures, colors)
      .scale(upscaleFactor)
  }
}
