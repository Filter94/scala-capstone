package observatory

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

import scala.math._

object Location {
  val epsilon = 1E-5

  def equals(a: Location, b: Location): Boolean = {
    math.abs(a.lon - b.lon) < epsilon && math.abs(a.lat - b.lat) < epsilon
  }

  def fromLocationRow(locationRow: Row): Location = {
    Location(locationRow.getAs[Double]("lat"), locationRow.getAs[Double]("lon"))
  }

  def fromRow[T](row: Row, key: T): Location = {
    val locationRow: Row = key match {
      case field: String => row.getAs[Row](field)
      case idx: Int => row.getAs[Row](idx)
    }
    fromLocationRow(locationRow)
  }
}

/**
  * Introduced in Week 1. Represents a location on the globe.
  *
  * @param lat Degrees of latitude, -90 ≤ lat ≤ 90
  * @param lon Degrees of longitude, -180 ≤ lon ≤ 180
  */
case class Location(lat: Double, lon: Double)

object TempByLocation {
  def convertIterable(iter: Iterable[(Location, Temperature)]): Iterable[TempByLocation] =
    iter.map { case (location, temperature) => TempByLocation(location, temperature) }
}

case class TempByLocation(location: Location, temperature: Temperature)

case class LocationWindow(topLeft: Location, bottomRight: Location)

/**
  * Introduced in Week 3. Represents a tiled web map tile.
  * See https://en.wikipedia.org/wiki/Tiled_web_map
  * Based on http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
  *
  * @param x    X coordinate of the tile
  * @param y    Y coordinate of the tile
  * @param zoom Zoom level, 0 ≤ zoom ≤ 19
  */
case class Tile(x: Int, y: Int, zoom: Int) {
  lazy val location: Location = {
    val n = pow(2.0, zoom)
    val lon = x / n * 360.0 - 180.0
    val lat = toDegrees(atan(sinh(Pi * (1 - 2 * (y / n)))))
    Location(lat, lon)
  }
}

/**
  * Introduced in Week 4. Represents a point on a grid composed of
  * circles of latitudes and lines of longitude.
  *
  * @param lat Circle of latitude in degrees, -89 ≤ lat ≤ 90
  * @param lon Line of longitude in degrees, -180 ≤ lon ≤ 179
  */
case class GridLocation(lat: Int, lon: Int)

/**
  * Introduced in Week 5. Represents a point inside of a grid cell.
  *
  * @param x X coordinate inside the cell, 0 ≤ x ≤ 1
  * @param y Y coordinate inside the cell, 0 ≤ y ≤ 1
  */
case class CellPoint(x: Double, y: Double)

/**
  * Introduced in Week 2. Represents an RGB color.
  *
  * @param red   Level of red, 0 ≤ red ≤ 255
  * @param green Level of green, 0 ≤ green ≤ 255
  * @param blue  Level of blue, 0 ≤ blue ≤ 255
  */
case class Color(red: Int, green: Int, blue: Int)

case class Id(stnId: Option[STN], wbanId: Option[WBAN]) {
  def validate: Boolean = stnId.nonEmpty || wbanId.nonEmpty
}

object TemperatureRow {
  type FlatTemp = (Option[STN], Option[WBAN], Month, Day, Temperature)

  def flatSchema: StructType = StructType(
    Seq(
      StructField("stn", IntegerType, nullable = false),
      StructField("wban", IntegerType, nullable = true),
      StructField("month", IntegerType, nullable = false),
      StructField("day", IntegerType, nullable = false),
      StructField("temp", DoubleType, nullable = false)
    )
  )

  def farenheitToCelcius(f: Double): Double =
    (f - 32) * 5 / 9

  def parseFarenheit(flatTemp: FlatTemp): TemperatureRow = flatTemp match {
    case (stn, wban, month, day, temp) =>
      parse((stn, wban, month, day, farenheitToCelcius(temp)))
  }

  def parse(flatTemp: FlatTemp): TemperatureRow = flatTemp match {
    case (stn, wban, month, day, temp) =>
      TemperatureRow(Id(stn, wban), month, day, temp)
  }
}

case class TemperatureRow(id: Id, month: Month, day: Day, temp: Temperature) {
  def validate: Boolean = id.validate && day.isValidInt && month.isValidInt && !temp.isNaN
}

object StationRow {
  type FlatStation = (Option[STN], Option[WBAN], Double, Double)

  def flatSchema: StructType = StructType(
    Seq(
      StructField("stn", IntegerType, nullable = true),
      StructField("wban", IntegerType, nullable = true),
      StructField("lat", DoubleType, nullable = false),
      StructField("lon", DoubleType, nullable = false)
    )
  )

  def parse(flatStation: FlatStation): StationRow = flatStation match {
    case (stn, wban, lat, lon) =>
      StationRow(Id(stn, wban), Location(lat, lon))
  }
}

case class StationRow(id: Id, location: Location) {
  def validate: Boolean = id.validate && !location.lat.isNaN && !location.lon.isNaN
}

