package observatory

import observatory.Extraction.{locateTemperatures, locationYearlyAverageRecords}
import org.apache.log4j.{Level, Logger}
import org.junit.Assert._
import org.junit.Test

import java.time.LocalDate

trait ExtractionTest extends MilestoneSuite {
  private val milestoneTest = namedMilestoneTest("data extraction", 1) _

  // Implement tests for the methods of the `Extraction` object
  @Test def `locateTemperatures and locationYearlyAverageRecords test`(): Unit = {
    val year = 2021
    val root = "/"
    val stationsPath = s"${root}test_stations.csv"
    val temperaturePath = s"${root}test_temperatures.csv"

    // test temperatures
    def assertTemperatures(t1: (LocalDate, Location, Temperature), t2: (LocalDate, Location, Temperature)): Unit = {
      assertTrue(t1._1.compareTo(t2._1) == 0)
      assertEquals(t1._2, t2._2)
      assertEquals(t1._3, t2._3, 1e-4)
    }

    val correctTemperature = Seq(
      (LocalDate.of(year, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(year, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(year, 1, 29), Location(37.358, -78.438), 2.0)
    )

    val resultTemperature = locateTemperatures(year, stationsPath, temperaturePath)
    correctTemperature.zip(resultTemperature).foreach(t => assertTemperatures(t._1, t._2))

    // test averages
    def assertAverages(t1: (Location, Temperature), t2: (Location, Temperature)): Unit = {
      assertEquals(t1._1, t2._1)
      assertEquals(t1._2, t2._2, 1e-4)
    }

    val correctAverages = Seq(
      (Location(37.35, -78.433), 27.3),
      (Location(37.358, -78.438), 1.0)
    )

    val resultAverages = locationYearlyAverageRecords(resultTemperature)
    correctAverages.zip(resultAverages).foreach(t => assertAverages(t._1, t._2))
  }

}
