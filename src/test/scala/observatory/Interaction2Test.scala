package observatory

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait Interaction2Test extends FunSuite with Checkers {
  val testLayers = List(
    Layer(LayerName.Temperatures,
      Seq(), Range(1975, 2015).inclusive),
    Layer(LayerName.Deviations,
      Seq(), Range(1990, 2015).inclusive))

  test("Available layers") {
    assert(testLayers.size === 2)
  }

  test("Year bounds test") {
    val layerSignal = Var(testLayers.head)
    val boundsSignal = Interaction2.yearBounds(layerSignal)
    assert(boundsSignal().size === testLayers.head.bounds.length)
    layerSignal() = testLayers.last
    assert(boundsSignal().size === testLayers.last.bounds.length)
  }

  test("Year selection test") {
    val boundsSignal = Signal(testLayers.last)
    val year = 1990
    val slider = Var(year)
    val selectedYear = Interaction2.yearSelection(boundsSignal, slider)
    assert(selectedYear() === 1990)
    slider() = 1890
    assert(selectedYear() === 1990)
    slider() = 1999
    assert(selectedYear() === 1999)
    slider() = 2299
    assert(selectedYear() === 2015)
  }

  test("Caption test") {
    val layerSignal = Var(testLayers.head)
    val year = 1990
    val slider = Var(year)
    val captionSignal = Interaction2.caption(layerSignal, slider)
    assert(captionSignal() === "Temperatures (1990)")
    layerSignal() = testLayers.last
    assert(captionSignal() === "Deviations (1990)")
    slider() = 2007
    assert(captionSignal() === "Deviations (2007)")
  }

  test("Url test") {
    val layerSignal = Var(testLayers.head)
    val year = 1990
    val slider = Var(year)
    val urlSignal = Interaction2.layerUrlPattern(layerSignal, slider)
    assert(urlSignal() === "target/temperatures/1990")
    layerSignal() = testLayers.last
    assert(urlSignal() === "target/deviations/1990")
    slider() = 2007
    assert(urlSignal() === "target/deviations/2007")
  }
}
