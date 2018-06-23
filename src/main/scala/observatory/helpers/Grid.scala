package observatory.helpers

import observatory._

import scala.collection.concurrent.TrieMap

object Grid {
  def apply(temperatures: Iterable[(Location, Temperature)]): Grid = new Grid(temperatures)
}

class Grid(temperatures: Iterable[(Location, Temperature)]) {
  private val discreteTempsCache: TrieMap[GridLocation, Temperature] = TrieMap()

  def getTemperature(location: GridLocation): Temperature = {
    discreteTempsCache.getOrElse(location, {
      val temp = Visualization.predictTemperature(temperatures, GridLocation.location(location))
      discreteTempsCache.put(location, temp)
      temp
    })
  }
}
