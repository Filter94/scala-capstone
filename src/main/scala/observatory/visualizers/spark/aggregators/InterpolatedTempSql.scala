package observatory.visualizers.spark.aggregators

import observatory.Location
import observatory.visualizers.common.InverseWeighting.{sphereDistance, w}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

class InterpolatedTempSql(val P: Double, val epsilon: Double) extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(
      Seq(StructField("location", Location.schema, nullable = false),
        StructField("known", Location.schema, nullable = false),
        StructField("temperature", DoubleType, nullable = false)))

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("nominator", DoubleType, nullable = false) ::
      StructField("denominator", DoubleType, nullable = false) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val (location, xi, ui) = (Location.fromRow(input, 0),
      Location.fromRow(input, 1),
      input.getAs[Double](2))
    val d = sphereDistance(location, xi) max epsilon
    val wi = w(d, P)
    buffer(0) = buffer.getAs[Double](0) + wi * ui
    buffer(1) = buffer.getAs[Double](1) + wi
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Double](0) / buffer.getAs[Double](1)
  }
}
