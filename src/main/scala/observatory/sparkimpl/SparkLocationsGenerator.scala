package observatory.sparkimpl

import observatory.Location
import org.apache.spark.sql.Dataset

trait SparkLocationsGenerator extends Serializable {
  def generateLocations(): Dataset[Location]
}
