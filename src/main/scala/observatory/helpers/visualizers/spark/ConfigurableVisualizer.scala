package observatory.helpers.visualizers.spark

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Location
import observatory.helpers.SparkContextKeeper.spark
import observatory.helpers.SparkContextKeeper.spark.implicits._
import observatory.helpers.VisualizerConfiguration
import org.apache.spark.sql.Dataset

trait ConfigurableVisualizer {
  val configuration: VisualizerConfiguration

  def visualize(): Image = {
    val locations: Dataset[Location] = spark.range(configuration.width * configuration.height)
      .map(i => configuration.locationsGenerator.get(i.toInt))
    val pixels = computePixels(locations)
    Image(configuration.width, configuration.height, pixels)
  }

  protected def computePixels(locations: Dataset[Location]): Array[Pixel]
}
