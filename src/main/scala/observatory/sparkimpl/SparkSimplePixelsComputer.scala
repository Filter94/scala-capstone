package observatory.sparkimpl

import com.sksamuel.scrimage.Pixel
import observatory.common.{ImageConfiguration, PixelsComputer}
import observatory.common.interpolators.ColorsInterpolator
import observatory.Temperature
import observatory.SparkContextKeeper.spark.implicits._
import observatory.sparkimpl.predictors.SparkPredictor

object SparkSimplePixelsComputer {
  def apply(interpolator: ColorsInterpolator, locationsGenerator: SparkLocationsGenerator, predictor: SparkPredictor,
            imageConfiguration: ImageConfiguration, transparency: Int ): SparkSimplePixelsComputer =
    new SparkSimplePixelsComputer(interpolator, locationsGenerator, predictor, imageConfiguration, transparency)
}

class SparkSimplePixelsComputer(val interpolator: ColorsInterpolator,
                                val locationsGenerator: SparkLocationsGenerator,
                                val predictor: SparkPredictor,
                                val imageConfiguration: ImageConfiguration,
                                val transparency: Int) extends PixelsComputer {
  def computePixels(): Array[Pixel] = {
    val locations = locationsGenerator.generateLocations()
    val tempsInterpolated = predictor.predictTemperatures(locations)
      .select($"temperature".as[Temperature])
      .collect()
    for {
      temp <- tempsInterpolated
    } yield {
      val color = interpolator.interpolateColor(temp)
      Pixel(color.red, color.green, color.blue, transparency)
    }
  }
}
