package observatory

import com.sksamuel.scrimage.writer
import observatory.Extraction.{parLocateTemperatures, parLocationYearlyAverageRecords, readResource}
import observatory.Interaction.{generateTiles, parTile}
import observatory.Manipulation.{averageNoGC, deviationNoGC}
import observatory.Visualization.parVisualize
import observatory.Visualization2.visualizeGridNoGC

object Main extends App {


  def readColors(colorsPath: String): Iterable[(Temperature, Color)] = {
    readResource(colorsPath)
      .map(TemperatureColors)
      .map(c => (c.temperature, Color(c.red, c.green, c.blue)))
  }

  def printToc(t: Long): Unit = {
    val duration: Int = ((System.nanoTime - t) / 1e9d).round.toInt
    val minutes: Int = duration / 60
    val seconds: Int = duration % 60
    println(s"\telapsed time: ${minutes}min ${seconds}sec")
  }

  def temperatureAverages(year: Year): Iterable[(Location, Temperature)] = {
    println(s"[Year = $year] Locating temperatures and computing averages...")
    val tic = System.nanoTime
    val temperaturePath = s"/${year}.csv"
    val locations = parLocateTemperatures(year, stationsPath, temperaturePath)
    val temperatures = parLocationYearlyAverageRecords(locations).seq
    printToc(tic)
    temperatures
  }

  def visualize(): Unit = {
    val temperatures = temperatureAverages(year)
    val colors = readColors(temperatureColorsPath)

    println("Visualizing...")
    val tic = System.nanoTime
    val file = new java.io.File(s"target/visualize/$year.png")
    if (!file.getParentFile.exists) file.getParentFile.mkdirs
    parVisualize(temperatures, colors).output(file)
    printToc(tic)
  }

  def tiles(): Unit = {
    val colors = readColors(temperatureColorsPath)

    def generateTemperaturesImage(year: Year, tile: Tile, temperatures: Iterable[(Location, Temperature)]): Unit = {
      val tic = System.nanoTime
      println(s"\tyear=$year, tile=$tile")
      val image = parTile(temperatures, colors, tile)
      val file = new java.io.File(s"target/temperatures/$year/${tile.zoom}/${tile.x}-${tile.y}.png")
      if (!file.getParentFile.exists) file.getParentFile.mkdirs
      image.output(file)
      printToc(tic)
    }

    val temperatures = temperatureAverages(year)
    println("Generating tiles...")
    val tic = System.nanoTime
    val yearlyData = Seq((year, temperatures))
    generateTiles(0 to 0)(yearlyData, generateTemperaturesImage)
    printToc(tic)
  }

  def deviations(): Unit = {
    val colors = readColors(deviationColorsPath)

    def generateDeviationsImage(year: Year, tile: Tile, deviationGrid: GridLocation => Temperature): Unit = {
      val tic = System.nanoTime
      println(s"\tyear=$year, tile=$tile")

      val image = visualizeGridNoGC(deviationGrid, colors, tile)
      val file = new java.io.File(s"target/deviations/$year/${tile.zoom}/${tile.x}-${tile.y}.png")
      if (!file.getParentFile.exists) file.getParentFile.mkdirs
      image.output(file)
      printToc(tic)
    }

    // Read inputs
    val normalTemperatures = for {
      y <- normalYears
    } yield temperatureAverages(y)
    val temperatures = temperatureAverages(year)

    println("Generating deviation tiles...")
    val tic = System.nanoTime

    // Compute grids
    val normalsGrid = averageNoGC(normalTemperatures)
    val deviationGrid = deviationNoGC(temperatures, normalsGrid)

    // Generate tiles
    val yearlyData = Seq((year, deviationGrid))
    generateTiles(0 to 1)(yearlyData, generateDeviationsImage)
    printToc(tic)
  }

  val year = 2015
  val normalYears = 1975 to 1978
  val stationsPath = "/stations.csv"
  val temperatureColorsPath = "/colors_temperatures.csv"
  val deviationColorsPath = "/colors_deviations.csv"
  val mode = "deviations"

  println(s"Running mode = $mode...")
  mode match {
    case "visualize" => visualize()
    case "tiles" => tiles()
    case "deviations" => deviations()
    case _ => println("Wrong mode")
  }
}
