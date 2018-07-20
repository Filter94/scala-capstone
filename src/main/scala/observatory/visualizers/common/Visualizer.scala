package observatory.visualizers.common

import com.sksamuel.scrimage.Image

trait Visualizer {
    // todo: Implement strategy pattern.
  def visualize(): Image
}
