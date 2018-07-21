package observatory.common

object ImageConfiguration {
  object Builder {
    def apply(): Builder = new Builder()
  }
  /**
    * Mutable Builder using prototype pattern.
    */
  class Builder {
    var configuration = ImageConfiguration(360, 180)

    def setWidth(width: Int): Builder = {
      configuration = configuration.copy(width = width)
      this
    }

    def setHeight(height: Int): Builder = {
      configuration = configuration.copy(height = height)
      this
    }

    def build: ImageConfiguration = configuration
  }

  def apply(): ImageConfiguration = Builder().build
}

case class ImageConfiguration(width: Int, height: Int)

