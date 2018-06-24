package observatory

/**
  * 6th (and last) milestone: user interface polishing
  */
object Interaction2 {

  /**
    * @return The available layers of the application
    */
  def availableLayers: Seq[Layer] = {
    Seq(
      Layer(LayerName.Temperatures,
        Seq(
          (60, Color(255, 255, 255)),
          (32, Color(255, 0, 0)),
          (12, Color(255, 255, 0)),
          (0, Color(0, 255, 255)),
          (-15, Color(0, 0, 255)),
          (-27, Color(255, 0, 255)),
          (-50, Color(33, 255, 107)),
          (-60, Color(0, 0, 0))), Range(1975, 2015).inclusive),
      Layer(LayerName.Deviations,
        Seq(
          (0, Color(255, 255, 255)),
          (4, Color(255, 0, 0)),
          (2, Color(255, 255, 0)),
          (-2, Color(0, 255, 255)),
          (-7, Color(0, 0, 255)),
          (7, Color(0, 0, 0))), Range(1990, 2015).inclusive))
  }

  /**
    * @param selectedLayer A signal carrying the layer selected by the user
    * @return A signal containing the year bounds corresponding to the selected layer
    */
  def yearBounds(selectedLayer: Signal[Layer]): Signal[Range] = {
    Signal(selectedLayer().bounds)
  }

  /**
    * @param selectedLayer The selected layer
    * @param sliderValue   The value of the year slider
    * @return The value of the selected year, so that it never goes out of the layer bounds.
    *         If the value of `sliderValue` is out of the `selectedLayer` bounds,
    *         this method should return the closest value that is included
    *         in the `selectedLayer` bounds.
    */
  def yearSelection(selectedLayer: Signal[Layer], sliderValue: Signal[Year]): Signal[Year] = {
    Signal{
      val year = sliderValue()
      val boundsRange = yearBounds(selectedLayer)()
      if (boundsRange.contains(year)) {
        year
      } else {
       if (year < boundsRange.head) {
         boundsRange.head
       } else {
         boundsRange.last
       }
      }
    }
  }

  /**
    * @param selectedLayer The selected layer
    * @param selectedYear  The selected year
    * @return The URL pattern to retrieve tiles
    */
  def layerUrlPattern(selectedLayer: Signal[Layer], selectedYear: Signal[Year]): Signal[String] = {
    Signal{
      val layer = selectedLayer()
      val layerName = layer.layerName.id
      val year = selectedYear()
      s"target/$layerName/$year"
    }
  }

  /**
    * @param selectedLayer The selected layer
    * @param selectedYear  The selected year
    * @return The caption to show
    */
  def caption(selectedLayer: Signal[Layer], selectedYear: Signal[Year]): Signal[String] = {
    Signal{
      val layerName = selectedLayer().layerName
      val year = selectedYear()
      s"$layerName ($year)"
    }
  }

}

sealed abstract class LayerName(val id: String)

object LayerName {

  case object Temperatures extends LayerName("temperatures")

  case object Deviations extends LayerName("deviations")

}

/**
  * @param layerName  Name of the layer
  * @param colorScale Color scale used by the layer
  * @param bounds     Minimum and maximum year supported by the layer
  */
case class Layer(layerName: LayerName, colorScale: Seq[(Temperature, Color)], bounds: Range)

