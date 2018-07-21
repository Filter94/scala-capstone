package observatory.common

object InverseWeightingConfiguration {
  object Builder {
    def apply(): Builder = new Builder
  }
  class Builder {
    var configuration = InverseWeightingConfiguration(3, 1E-5)

    def setP(p: Double): Builder = {
      configuration = configuration.copy(p = p)
      this
    }

    def setEpsilon(epsilon: Double): Builder = {
      configuration = configuration.copy(epsilon = epsilon)
      this
    }

    def build: InverseWeightingConfiguration = configuration
  }
}

case class InverseWeightingConfiguration(p: Double, epsilon: Double)


