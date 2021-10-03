package observatory

import java.time.LocalDate
import scala.io.Source
import scala.collection.parallel.ParIterable

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {
  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] =
    parLocateTemperatures(year, stationsFile, temperaturesFile).seq

  def parLocateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): ParIterable[(LocalDate, Location, Temperature)] = {
    val stations = readResource(stationsFile).par
      .map(Station)
      .filter(p => p.lat.isDefined && p.lon.isDefined)
      .map(p => (p.stn_id, p.wban_id, p.lat.get, p.lon.get))

    val temperatures = readResource(temperaturesFile).par
      .map(StationTemperature(_, year))
      .map(p => (p.stn_id, p.wban_id, p.date, p.temperature))
      .groupBy(p => (p._1, p._2))

    for {
      station <- stations
      group <- temperatures
      if station._1 == group._1._1 && station._2 == group._1._2
      temperature <- group._2
    } yield {
      val loc = Location(station._3, station._4)
      (temperature._3, loc, temperature._4)
    }
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] =
    parLocationYearlyAverageRecords(records.par).seq

  def parLocationYearlyAverageRecords(records: ParIterable[(LocalDate, Location, Temperature)]): ParIterable[(Location, Temperature)] = {
    def computeTemperateAverages(temperatures: ParIterable[(LocalDate, Location, Temperature)]): Temperature = {
      val reduced = temperatures
        .map(t => (t._3, 1))
        .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

      reduced._1 / reduced._2
    }

    records
      .groupBy(t => t._2)
      .map(t => (t._1, computeTemperateAverages(t._2)))
      .toSeq
  }

  /**
    * @param resource Resource to read
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def readResource(resource: String): Seq[String] = {
    val fileStream = Source.getClass.getResourceAsStream(resource)
    Source.fromInputStream(fileStream).getLines().filter(_ != "").toList
  }
}
