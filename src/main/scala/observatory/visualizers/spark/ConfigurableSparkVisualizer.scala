package observatory.visualizers.spark

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Location
import observatory.SparkContextKeeper.spark
import observatory.SparkContextKeeper.spark.implicits._
import observatory.visualizers.common.{Visualizer, VisualizerConfiguration}
import org.apache.spark.sql.Dataset

trait ConfigurableSparkVisualizer extends Visualizer {
  val configuration: VisualizerConfiguration

  def visualize(): Image = {
    val locations: Dataset[Location] = spark.range(configuration.width * configuration.height)
      .map(i => configuration.locationsGenerator.get(i.toInt))
    val pixels = computePixels(locations)
    Image(configuration.width, configuration.height, pixels)
  }

  protected def computePixels(locations: Dataset[Location]): Array[Pixel]
}
