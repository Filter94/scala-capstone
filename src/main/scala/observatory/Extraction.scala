package observatory

import java.nio.file.Paths
import java.time.LocalDate

import observatory.Extraction.locationYearlyAverageRecordsSpark
import org.apache.spark.rdd.RDD
import observatory.helpers.SparkContextKeeper.spark
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.avg

/**
  * 1st milestone: data extraction
  */
object Extraction {

  import spark.implicits._

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975_bacup.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    locateTemperaturesSpark(year, stationsFile, temperaturesFile).collect()
  }

  def locateTemperaturesSpark(year: Year, stationsFile: String, temperaturesFile: String): RDD[(LocalDate, Location, Temperature)] = {
    def filepathHelper(filePath: String) =
      Paths.get(getClass.getResource(filePath).toURI).toString

    def parseStations = {
      val stationSchema = StationRow.flatSchema
      val stationsLines = spark.read
        .schema(stationSchema)
        .option("mode", "DROPMALFORMED")
        .csv(filepathHelper(stationsFile))
        .na.drop(Seq("lon", "lat"))
        .as[StationRow.FlatStation]
      stationsLines
        .map(StationRow.parse)
        .filter((station: StationRow) => station.validate)
    }

    def parseTemps = {
      val tempsSchema = TemperatureRow.flatSchema
      val tempsLines = spark.read
        .schema(tempsSchema)
        .option("mode", "DROPMALFORMED")
        .csv(filepathHelper(temperaturesFile))
        .na.drop(Seq("temp", "day", "month"))
        .as[TemperatureRow.FlatTemp]
      tempsLines
        .map(TemperatureRow.parseFarenheit)
        .filter((temp: TemperatureRow) => temp.validate)
    }

    val stations = parseStations
    val temps = parseTemps
    val joinedData = stations.join(temps, "id")
    for {
      row <- joinedData.rdd
    } yield {
      (LocalDate.of(year, row.getAs[Month]("month"), row.getAs[Day]("day")),
        Location.fromRow(row, "location"),
        row.getAs[Temperature]("temp"))
    }
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    records.par
      .map{case (_, location, temperature) => TempByLocation(location, temperature)}
      .groupBy(_.location)
      .mapValues {
        location =>
          location.foldLeft(0.0)(
            (t, r) => t + r.temperature) / location.size
      }.seq
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecordsSpark(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    locationYearlyAverageRecordsSpark(spark.sparkContext.parallelize(records.toSeq)).collect()
  }

  def locationYearlyAverageRecordsSpark(records: RDD[(LocalDate, Location, Temperature)]): Dataset[(Location, Temperature)] = {
    val locationTempRdd = records.map { case (_, location, temp) =>
      TempByLocation(location, temp) }
    locationTempRdd.toDS()
      .groupBy($"location")
      .agg(avg($"temperature").as("average temp"))
      .select($"location".as[Location], $"average temp".as[Temperature])
  }
}
