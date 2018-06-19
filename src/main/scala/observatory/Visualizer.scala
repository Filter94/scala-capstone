package observatory

import com.sksamuel.scrimage.Image

import scala.collection.GenIterable

trait Visualizer {
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image

  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature

  def interpolateColor(points: GenIterable[(Temperature, Color)], value: Temperature): Color
}
