package observatory.common

import com.sksamuel.scrimage.Pixel

trait PixelsComputer {
  def computePixels(): Array[Pixel]
}
