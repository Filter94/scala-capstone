package observatory.parimpl

import com.sksamuel.scrimage.Pixel
import observatory.common.interpolators.ColorsInterpolator
import observatory.common.{ImageConfiguration, PixelsComputer}

object IterablePixelsComputer {
  def apply(interpolator: ColorsInterpolator, locationsGenerator: ParLocationsGenerator, predictor: ParPredictor,
            imageConfiguration: ImageConfiguration, transparency: Int): IterablePixelsComputer =
    new IterablePixelsComputer(interpolator, locationsGenerator, predictor, imageConfiguration, transparency)
}

class IterablePixelsComputer(val interpolator: ColorsInterpolator,
                             val locationsGenerator: ParLocationsGenerator,
                             val predictor: ParPredictor,
                             val imageConfiguration: ImageConfiguration,
                             val transparency: Int) extends PixelsComputer {
  def computePixels(): Array[Pixel] = {
    val locations = locationsGenerator.generateLocations()
    val tempsInterpolated = predictor.predictTemperatures(locations)
    val pixels = new Array[Pixel](locations.size)
    for {
      (temp, i) <- tempsInterpolated.zipWithIndex
    } {
      val color = interpolator.interpolateColor(temp)
      pixels(i) = Pixel(color.red, color.green, color.blue, transparency)
    }
    pixels
  }
}
