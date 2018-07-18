package observatory.helpers.generators

import observatory.Location

trait LocationsGenerator {
  def get(i: Int): Location
}
