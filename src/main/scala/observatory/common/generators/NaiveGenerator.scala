package observatory.common.generators

import observatory.Location

object NaiveGenerator {
  def apply(): NaiveGenerator = new NaiveGenerator()
}

class NaiveGenerator() extends LocationGenerator {
  private val width = 360
  private val height = 180
  private val latLength: Double = height
  private val lonLength: Double = width
  // lat lon coordinates of top left pixel
  private val latStart: Double = 90
  private  val lonStart: Double = -180

  def get(i: Int): Location = {
    val latIdx = i / width
    val lonIdx = i % width
    val latStep = latLength / height
    val lonStep = lonLength / width
    Location(latStart - latIdx * latStep, lonStart + lonIdx * lonStep)
  }
}
