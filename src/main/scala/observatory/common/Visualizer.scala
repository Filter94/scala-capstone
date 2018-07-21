package observatory.common

import com.sksamuel.scrimage.Image

abstract class Visualizer(protected val imageConfiguration: ImageConfiguration) {
  val pixelsComputer: PixelsComputer

  def visualize(): Image = {
    val pixels = pixelsComputer.computePixels()
    Image(imageConfiguration.width, imageConfiguration.height, pixels)
  }
}
