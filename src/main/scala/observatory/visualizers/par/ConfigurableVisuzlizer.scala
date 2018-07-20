package observatory.visualizers.par

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Location
import observatory.visualizers.common.{Visualizer, VisualizerConfiguration}

import scala.collection.parallel.ParIterable

trait ConfigurableVisuzlizer extends Visualizer {
  val configuration: VisualizerConfiguration
  def visualize(): Image = {
    val locations = Range(0, configuration.width * configuration.height).par
      .map(i => configuration.locationsGenerator.get(i))
    val pixels = computePixels(locations)
    Image(configuration.width, configuration.height, pixels)
  }

  protected def computePixels(locations: ParIterable[Location]): Array[Pixel]
}
