package observatory


import observatory.Extraction.parLocationYearlyAverageRecords

import java.time.LocalDate
import scala.collection.parallel.ParSeq
import scala.io.Source

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

  def parLocateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): ParSeq[(LocalDate, Location, Temperature)] = {
    val stations = parReadResource(stationsFile).map(Station)
      .filter(station => station.lat.isDefined && station.lon.isDefined)
    val temperatures = parReadResource(temperaturesFile).map(StationTemperature(_, year))

    for {
      station <- stations
      temperature <- temperatures
      if station.stn_id == temperature.stn_id && station.wban_id == temperature.wban_id
    } yield (
      LocalDate.parse(s"${temperature.year}-${temperature.month}-${temperature.day}"),
      Location(station.lat.get, station.lon.get),
      temperature.temperature
    )
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] =
    parLocationYearlyAverageRecords(records.toSeq.par).seq

  def parLocationYearlyAverageRecords(records: ParSeq[(LocalDate, Location, Temperature)]): ParSeq[(Location, Temperature)] = {
    def computeTemperateAverages(temperatures: ParSeq[(LocalDate, Location, Temperature)]): Temperature = {
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

  def readResource(resource: String): Seq[String] = {
    val fileStream = Source.getClass.getResourceAsStream(resource)
    Source.fromInputStream(fileStream).getLines().filter(_ != "").toList
  }

  def parReadResource(resource: String): ParSeq[String] =
    readResource(resource).par
}
