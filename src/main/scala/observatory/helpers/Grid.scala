package observatory.helpers

import observatory.{GridLocation, Location, Temperature, Visualization}

object Grid {
  def apply(temperatures: Iterable[(Location, Temperature)]): Grid = new Grid(temperatures)
}

class Grid(temperatures: Iterable[(Location, Temperature)]) {
  private val WIDTH = 360
  private val HEIGHT = 180
  private val discreteTempsCache: Array[Temperature] = Array.fill(WIDTH * HEIGHT)(Double.NaN)

  def locationToIndex(gridLocation: GridLocation): Int = {
    (90 - gridLocation.lat) * WIDTH + (180 + gridLocation.lon)
  }

  def getTemperature(location: GridLocation): Temperature = {
    val i = locationToIndex(location)
    if (!discreteTempsCache(i).isNaN) {
      discreteTempsCache(i)
    } else {
      val res = Visualization.predictTemperature(temperatures, location.toLocation)
      discreteTempsCache(i) = res
      res
    }
  }

  def combine(location: GridLocation, temperature: => Temperature, f: (Temperature, Temperature) => Temperature): Temperature = {
    f(getTemperature(location), temperature)
  }
}
