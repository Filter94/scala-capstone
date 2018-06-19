package observatory.helpers.spark

import com.sksamuel.scrimage.Image
import observatory._
import observatory.helpers.Interactor

import scala.math.{log, pow}


trait SparkInteractor extends Interactor with SparkVisualizer {
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val IMAGE_SIZE_X = 256
    val IMAGE_SIZE_Y = 256
    visualizeTile(IMAGE_SIZE_X, IMAGE_SIZE_Y)(temperatures, colors, tile)
  }

  def visualizeTile(sizeX: Int, sizeY: Int)(temperatures: Iterable[(Location, Temperature)],
                                                      colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val upscaleFactor = 2

    visualizeTile(sizeX / upscaleFactor, sizeY / upscaleFactor, 127, tile)(temperatures, colors)
      .scale(upscaleFactor)
  }
}
