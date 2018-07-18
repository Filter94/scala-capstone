package observatory.helpers.visualizers.spark

import com.sksamuel.scrimage.Pixel
import observatory._
import observatory.helpers.SparkContextKeeper.spark
import observatory.helpers.SparkContextKeeper.spark.implicits._
import observatory.helpers.{VisualizationHelper, VisualizerConfiguration}
import observatory.helpers.predictors.spark.BroadcastPredictor
import org.apache.spark.sql.Dataset

object Visualizer {
  def apply(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)],
            configuration: VisualizerConfiguration): Visualizer =
    new Visualizer(toDs(temperatures), colors, configuration)

  def toDs(data: Iterable[(Location, Temperature)]): Dataset[TempByLocation] = spark.createDataset(
    data.map {
      case (location, temp) => TempByLocation(location, temp)
    }.toSeq)
}

class Visualizer(val temps: Dataset[TempByLocation], val colors: Iterable[(Temperature, Color)],
                 val configuration: VisualizerConfiguration) extends ConfigurableVisualizer with Serializable {
  private val colorsSorted = VisualizationHelper.sortPoints(colors.toSeq)
  private val predictor = BroadcastPredictor(configuration.epsilon, configuration.p)

  protected def computePixels(locations: Dataset[Location]): Array[Pixel] = {

    val tempsInterpolated = predictor.predictTemperatures(temps, locations)
      .select($"temperature".as[Temperature])
      .collect()
    for {
      temp <- tempsInterpolated
    } yield {
      val color = VisualizationHelper.interpolateColor(colorsSorted, temp)
      Pixel(color.red, color.green, color.blue, configuration.transparency)
    }
  }
}
