package observatory

import com.sksamuel.scrimage.writer
import observatory.Extraction._
import observatory.Interaction._
import observatory.Visualization.parVisualize
import scala.collection.parallel.ParIterable

object Main extends App {
  def printToc(t: Long): Unit = {
    val duration: Int = ((System.nanoTime - t) / 1e9d).round.toInt
    val minutes: Int = duration / 60
    val seconds: Int = duration % 60
    println(s"\telapsed time: ${minutes}min ${seconds}sec")
  }

  val year = 2015
  val root = "/"
  val stationsPath = s"${root}stations.csv"
  val temperaturePath = s"${root}${year}.csv"
  val temperatureColorsPath = s"${root}temperature_colors.csv"
  val mode = "tiles"

  println("Locating temperatures and computing averages...")
  var tic = System.nanoTime
  val locations = parLocateTemperatures(year, stationsPath, temperaturePath)
  val temperatures = parLocationYearlyAverageRecords(locations)
  val colors = readResource(temperatureColorsPath)
    .map(TemperatureColors)
    .map(c => (c.temperature, Color(c.red, c.green, c.blue)))
  printToc(tic)

  mode match {
    case "visualize" =>
      println("Visualizing...")
      tic = System.nanoTime
      val file = new java.io.File(s"target/temp2-$year.png")
      parVisualize(6)(temperatures, colors).output(file)
      printToc(tic)
    case "tiles" =>
      println("Generating tiles...")

      def generateImage(year: Year, tile: Tile, temperatures: ParIterable[(Location, Temperature)]): Unit = {
        val tic = System.nanoTime
        println(s"year=$year, tile=$tile")
        val image = parTile(4)(temperatures, colors, tile)
        val file = new java.io.File(s"target/temperatures/$year/${tile.zoom}/${tile.x}-${tile.y}.png")
        if (!file.getParentFile.exists) file.getParentFile.mkdirs
        image.output(file)
        printToc(tic)
      }

      tic = System.nanoTime
      val yearlyData = Seq((year, temperatures))
      generateTiles(0 to 3)(yearlyData, generateImage)
      printToc(tic)
  }


}
