package observatory.common.generators

import observatory.Location

trait LocationGenerator extends Serializable {
  def get(i: Int): Location
}
