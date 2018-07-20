package observatory.visualizers.spark

import com.sksamuel.scrimage.Pixel
import observatory._
import SparkContextKeeper.spark
import SparkContextKeeper.spark.implicits._
import observatory.visualizers.common.VisualizerConfiguration
import observatory.visualizers.common.interpolators.LinearInterpolator
import observatory.visualizers.spark.predictors.BroadcastPredictor
import org.apache.spark.sql.Dataset

object DataVisualizer {
  def apply(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)],
            configuration: VisualizerConfiguration): DataVisualizer =
    new DataVisualizer(toDs(temperatures), colors, configuration)

  def toDs(data: Iterable[(Location, Temperature)]): Dataset[TempByLocation] = spark.createDataset(
    data.map {
      case (location, temp) => TempByLocation(location, temp)
    }.toSeq)
}

class DataVisualizer(val temps: Dataset[TempByLocation], val colors: Iterable[(Temperature, Color)],
                     val configuration: VisualizerConfiguration) extends Serializable with ConfigurableSparkVisualizer {
  private val interpolator = LinearInterpolator(colors)
  private val predictor = BroadcastPredictor(configuration.epsilon, configuration.p)

  protected def computePixels(locations: Dataset[Location]): Array[Pixel] = {
    val tempsInterpolated = predictor.predictTemperatures(temps, locations)
      .select($"temperature".as[Temperature])
      .collect()
    for {
      temp <- tempsInterpolated
    } yield {
      val color = interpolator.interpolateColor(temp)
      Pixel(color.red, color.green, color.blue, configuration.transparency)
    }
  }
}
