package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.helpers.par.ParInteractor

import scala.collection.parallel.ParIterable

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00   Top-left value
    * @param d01   Bottom-left value
    * @param d10   Top-right value
    * @param d11   Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
                             point: CellPoint,
                             d00: Temperature,
                             d01: Temperature,
                             d10: Temperature,
                             d11: Temperature
                           ): Temperature = {
    val XTop = (1 - point.x) * d00 + point.x * d10
    val XBottom = (1 - point.x) * d01 + point.x * d11
    (1 - point.y) * XTop + point.y * XBottom
  }

  /**
    * @param grid   Grid to visualize
    * @param colors Color scale to use
    * @param tile   Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
                     grid: GridLocation => Temperature,
                     colors: Iterable[(Temperature, Color)],
                     tile: Tile
                   ): Image = {
    val upscaleFactor = 1
    val width = 256 / upscaleFactor
    val height = 256 / upscaleFactor

    implicit def computePixels(temperatures: Iterable[(Location, Temperature)],
                      locations: ParIterable[Location], colors: Iterable[(Temperature, Color)],
                      transparency: Int): Array[Pixel] = {
      val pixels = new Array[Pixel](locations.size)
      val sortedColors = colors.toSeq.sortBy{case (temp, _) => temp}
      for {
        (location, i: Int) <- locations.zipWithIndex
      } {
        val square = Location.surroundingGridLocations(location)
        val (d00, d01, d10, d11) = (
          grid(square.topLeft), grid(square.bottomLeft),
          grid(square.topRight), grid(square.bottomRight))
        val interpolatedTemp = bilinearInterpolation(Location.cellPoint(location), d00, d01, d10, d11)
        val color = Visualization.interpolateColor(sortedColors, interpolatedTemp)
        pixels(i) = Pixel(color.red, color.green, color.blue, transparency)
      }
      pixels
    }
    ParInteractor.visualizeTile(width, height)(Iterable(), colors, tile).scale(upscaleFactor)
  }
}
