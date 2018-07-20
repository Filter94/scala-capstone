package observatory.visualizers.common.generators

import observatory.Location

trait LocationsGenerator {
  def get(i: Int): Location
}
