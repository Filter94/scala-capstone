package observatory

import observatory.visualizers.common.Grid


/**
  * 4th milestone: value-added information
  */
object Manipulation {
  def grid(temperatures: Iterable[(Location, Temperature)]): Grid = {
    Grid(gridLocation =>
      Visualization.predictTemperature(temperatures, Location(gridLocation.lat, gridLocation.lon)))
  }

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    grid(temperatures).apply
  }
  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val grids: Iterable[Grid] = temperaturess.map(temperatures => grid(temperatures))
    val sumGrid = grids.reduce((a, b) => a.combine({(acc, gridLocation) => acc + b(gridLocation)}))
    val averageGrid = sumGrid.map(temperature => temperature / grids.size )
    averageGrid.apply
  }

  /**
    * @param temperatures Known temperatures
    * @param normals      A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)],
                normals: GridLocation => Temperature): GridLocation => Temperature = {
    val newTemps = grid(temperatures)
    val deviationsGrid = newTemps.combine({
      case (newTemp, gridLocation) =>
        newTemp - normals(gridLocation)
    })
    deviationsGrid.apply
  }
}

