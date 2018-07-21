package observatory.parimpl

import com.sksamuel.scrimage.Pixel
import observatory.{GridLocation, Temperature}
import observatory.Visualization2.bilinearInterpolation
import observatory.common.{ImageConfiguration, PixelsComputer}
import observatory.common.interpolators.ColorsInterpolator

object GridPixelsComputer {
  def apply(grid: GridLocation => Temperature, interpolator: ColorsInterpolator, locationsGenerator: ParLocationsGenerator,
            imageConfiguration: ImageConfiguration, transparency: Int): GridPixelsComputer =
    new GridPixelsComputer(grid, interpolator, locationsGenerator, imageConfiguration, transparency)
}

class GridPixelsComputer(val grid: GridLocation => Temperature, val interpolator: ColorsInterpolator,
                         val locationsGenerator: ParLocationsGenerator, val imageConfiguration: ImageConfiguration,
                         val transparency: Int) extends PixelsComputer {
  def computePixels(): Array[Pixel] = {
    val locations = locationsGenerator.generateLocations()
    val pixels = new Array[Pixel](locations.size)
    for {
      (location, i: Int) <- locations.par.zipWithIndex
    } {
      val square = location.gridSquare
      val (d00, d01, d10, d11) = (
        grid(square.topLeft), grid(square.bottomLeft),
        grid(square.topRight), grid(square.bottomRight))
      val interpolatedTemp = bilinearInterpolation(location.cellPoint, d00, d01, d10, d11)
      val color = interpolator.interpolateColor(interpolatedTemp)
      pixels(i) = Pixel(color.red, color.green, color.blue, transparency)
    }
    pixels
  }
}
