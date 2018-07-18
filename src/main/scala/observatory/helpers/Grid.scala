package observatory.helpers

import observatory._

object Grid {
  def apply(f: GridLocation => Temperature, width: Int = 360, height: Int = 180): Grid = {
    new Grid(f)(width, height)
  }
}

/**
  * GridLocation => Temperature function wrapper for a cached access
  * @param f grid function.
  * @param width - width of a grid
  * @param height - height of a grid
  */
class Grid private (f: GridLocation => Temperature)(width: Int, height: Int) {
  private val cache = Array.fill(width * height)(Double.NaN)
  //TODO: generalize relative to size
  private def toIndex(gridLocation: GridLocation): Int = {
    val x = 180 + gridLocation.lon
    val y = 90 - gridLocation.lat
    y * width + x
  }

  def precomputeGrid(grid: Grid): Unit = {
    for {
      lat <- Range(90, -90, -1)
      lon <- -180 until 180
    } {
      val location = GridLocation(lat, lon)
      grid(location)
    }
  }

  def apply(gridLocation: GridLocation): Temperature = {
    val i = toIndex(gridLocation)
    if (!cache(i).isNaN) {
      cache(i)
    } else {
      val res = f(gridLocation)
      cache(i) = res
      res
    }
  }

  def map(f: Temperature => Temperature): Grid = {
    Grid({gridLocation => f(this(gridLocation))}, width, height)
  }

  def combine(f: (Temperature, GridLocation) => Temperature): Grid = {
    Grid(gridLocation => f(this(gridLocation), gridLocation), width, height)
  }
}