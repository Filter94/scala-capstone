package observatory.visualizers.spark

import observatory.common.generators.NaiveGenerator
import observatory.common.interpolators.LinearInterpolator
import observatory.{Color, Location, TempByLocation, Temperature}
import observatory.common.{ImageConfiguration, InverseWeightingConfiguration, Visualizer}
import observatory.sparkimpl.predictors.BroadcastPredictor
import observatory.sparkimpl.{SparkSimpleLocationsGenerator, SparkSimplePixelsComputer}
import org.apache.spark.sql.Dataset

object DefaultVisualizer {
  def apply(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): DefaultVisualizer =
    new DefaultVisualizer(ImageConfiguration.Builder().build, 255,
      InverseWeightingConfiguration.Builder().build, Helper.toDs(temperatures), colors)
}

class DefaultVisualizer(override val imageConfiguration: ImageConfiguration, private val transparency: Int,
                        private val predictorConfiguration: InverseWeightingConfiguration,
                        private val temperatures: Dataset[TempByLocation], private val colors: Iterable[(Temperature, Color)])
  extends Visualizer(imageConfiguration) {
  val pixelsComputer = SparkSimplePixelsComputer(
    LinearInterpolator(colors),
    SparkSimpleLocationsGenerator(NaiveGenerator(), imageConfiguration),
    BroadcastPredictor(temperatures, predictorConfiguration),
    imageConfiguration, transparency)
}
