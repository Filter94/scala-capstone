package observatory

import java.time.LocalDate

import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.FunSuite

trait ExtractionTest extends FunSuite {
  implicit val equality: Equality[Temperature] = TolerantNumerics.tolerantDoubleEquality(0.001)

  test("Extraction works correct on trivial data") {
    val res =
      Extraction.locateTemperatures(2018, "/test_stations.csv", "/test_temp.csv")
        .toSeq.sortBy { case (date, _, _) => date.toEpochDay }
    val expected = Seq(
      (LocalDate.of(2018, 1, 29), Location(37.358, -78.438), 2.0),
      (LocalDate.of(2018, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2018, 12, 6), Location(37.358, -78.438), 0.0))
    assert(
      expected.zipWithIndex.forall {
        case ((date, location, temp), i) =>
          res(i)._1 === date &&
            res(i)._2 === location &&
            res(i)._3 === temp
      })
  }

  test("Average temperature is correct works correct for trivial data") {
    val locateTemps = Seq(
      (LocalDate.of(2018, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2018, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2018, 1, 29), Location(37.358, -78.438), 2.0))
    val res =
      Extraction.locationYearlyAverageRecords(locateTemps)
    val expected = Seq(
      (Location(37.35, -78.433), 27.3),
      (Location(37.358, -78.438), 1.0))
    assert(expected.toSet === res.toSet)
  }

  test("Average temperature ends in reasonable time on big data") {
    val res =
      Extraction.locateTemperaturesSpark(2018, "/stations.csv", "/1975_bacup.csv").persist()
    assert(res.count() != 0)
    val avg = Extraction.locationYearlyAverageRecordsSpark(res)
    assert(avg.count() != 0)
  }
}