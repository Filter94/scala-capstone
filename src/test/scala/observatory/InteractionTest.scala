package observatory

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers


trait InteractionTest extends FunSuite with Checkers {
  test("Tile to location primitive test wo zoom") {
    val tile = Tile(0, 0, 1)
    val expectedLocation = Location(85.05112877980659, -180)
    assert(expectedLocation === Interaction.tileLocation(tile))
  }

  test("Tile image test") {
    val temps: Seq[(Location, Temperature)] = Seq(
      (Location(45.0, -90.0), 60.0),
      (Location(-45.0, 0.0), -60.0))
    val colors: Seq[(Temperature, Color)] = Seq(
      (60, Color(255, 255, 255)),
      (32, Color(255, 0, 0)),
      (12, Color(255, 255, 0)),
      (0, Color(0, 255, 255)),
      (-15, Color(0, 0, 255)),
      (-27, Color(255, 0, 255)),
      (-50, Color(33, 255, 107)),
      (-60, Color(0, 0, 0)))
    val tile = Tile(0, 0, 0)
    val image = Interaction.tile(temps, colors, tile)
    image.output(s"tile.png")
  }
}
