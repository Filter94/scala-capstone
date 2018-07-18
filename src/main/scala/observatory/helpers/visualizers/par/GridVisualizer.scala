package observatory.helpers.visualizers.par

import com.sksamuel.scrimage.Pixel
import observatory.Visualization2.bilinearInterpolation
import observatory._
import observatory.helpers.{VisualizationHelper, VisualizerConfiguration}

import scala.collection.parallel.ParIterable

object GridVisualizer {
  def apply(colors: Iterable[(Temperature, Color)], grid: GridLocation => Temperature,
            configuration: VisualizerConfiguration): GridVisualizer =
    new GridVisualizer(grid, colors, configuration)
}

/**
  * Grid visualizer. More robust implementation.
  */
class GridVisualizer(val grid: GridLocation => Temperature, val colors: Iterable[(Temperature, Color)],
                     val configuration: VisualizerConfiguration) extends ConfigurableVisuzlizer {
  private val colorsSorted = VisualizationHelper.sortPoints(colors.toSeq)

  def computePixels(locations: ParIterable[Location]): Array[Pixel] = {
    val pixels = new Array[Pixel](locations.size)
    for {
      (location, i: Int) <- locations.par.zipWithIndex
    } {
      val square = location.gridSquare
      val (d00, d01, d10, d11) = (
        grid(square.topLeft), grid(square.bottomLeft),
        grid(square.topRight), grid(square.bottomRight))
      val interpolatedTemp = bilinearInterpolation(location.cellPoint, d00, d01, d10, d11)
      val color = VisualizationHelper.interpolateColor(colorsSorted, interpolatedTemp)
      pixels(i) = Pixel(color.red, color.green, color.blue, configuration.transparency)
    }
    pixels
  }
}
