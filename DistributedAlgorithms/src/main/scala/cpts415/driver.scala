package cpts415

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.graphframes.GraphFrame

object driver {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def main(args: Array[String]) {
    /**
     * Start the Spark session
     */
    val spark = SparkSession
      .builder()
      .appName("cpts415-bigdata")
      .config("spark.some.config.option", "some-value")//.master("local[*]")
      .getOrCreate()
    spark.sparkContext.addFile("/home/ubuntu/DistributedAlgorithms/data/airports-extended.csv")
    spark.sparkContext.addFile("/home/ubuntu/DistributedAlgorithms/data/airlines.csv")
    spark.sparkContext.addFile("/home/ubuntu/DistributedAlgorithms/data/NewRouteAirports.csv")
    spark.sparkContext.addFile("/home/ubuntu/DistributedAlgorithms/data/NewAirports.csv")

    /**
     * Test Datasets to showcase scalability
     */
    spark.sparkContext.addFile("/home/ubuntu/DistributedAlgorithms/data/testEdges1Mil.csv")
    spark.sparkContext.addFile("/home/ubuntu/DistributedAlgorithms/data/testEdges5Mil.csv")
    spark.sparkContext.addFile("/home/ubuntu/DistributedAlgorithms/data/NewTestNodes.csv")
    spark.sparkContext.addFile("/home/ubuntu/DistributedAlgorithms/data/testNodes1Mil.csv")
    spark.sparkContext.addFile("/home/ubuntu/DistributedAlgorithms/data/testNodes5Mil.csv")
    spark.sparkContext.addFile("/home/ubuntu/DistributedAlgorithms/data/testEdges10Mil.csv")
    /**
     * Load CSV file into Spark SQL
     */
    // You can replace the path in .csv() to any local path or HDFS path
    var airports = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("airports-extended.csv"))
    var airlines = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("airlines.csv"))
    var routeEdges = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("NewRouteAirports.csv"))
    var routeAirports = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("NewAirports.csv"))
    airports.createOrReplaceTempView("airports")
    airlines.createOrReplaceTempView("airlines")
    /**
     * Test Datasets to showcase scalability
     */
    var testEdges1Mil = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("testEdges1Mil.csv"))
    var testEdges5Mil = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("testEdges5Mil.csv"))
    var testEdges10Mil = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("testEdges10Mil.csv"))
    var testNode100Thou = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("NewTestNodes.csv"))
    var testNode1Mil = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("testNodes1Mil.csv"))
    var testNode5Mil = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(SparkFiles.get("testNodes5Mil.csv"))
    testNode100Thou.createOrReplaceTempView("testNode100Thou")
    testNode1Mil.createOrReplaceTempView("testNode1Mil")
    testNode5Mil.createOrReplaceTempView("testNode5Mil")
    /**
     * The basic Spark SQL functions
     */
    println("Find the names of Airports in a given Country")
    val t1 = System.nanoTime
    var countryInfo = spark.sql(
      """
        |SELECT Name
        |FROM airports
        |WHERE airports.Country = 'United States'
        |""".stripMargin)
    println((System.nanoTime - t1)/1e9d)
    countryInfo.show()
    
    /**
     * Top 5 Countries
     */
    println("Top Countries by Airport")
    val t4 = System.nanoTime
    var freq = spark.sql(
      """
        |SELECT Country, COUNT(*)
        |FROM airports
        |GROUP BY airports.Country
        |ORDER BY 2 DESC
        |LIMIT 5
        |""".stripMargin
    )
    println((System.nanoTime - t4)/1e9d)
    freq.show()
    
    //Create a Vertex DataFrame with unique ID column "id"
    print("Load Time to generate Airport Graph: ")
    val t7 = System.nanoTime
    val v = routeAirports
    // Create an Edge DataFrame with "src" and "dst" columns
    val e = routeEdges
    // Create a GraphFrame
    val g = GraphFrame(v, e)
    print(((System.nanoTime - t7)/1e9d)+"\n")
    
    /**
     * BFS Shortest Path, Seattle to Denver, should be a direct flight
     */
    print("Airport BFS: ")
    val t10 = System.nanoTime
    val results = g.bfs.fromExpr("id='SEA'").toExpr("id='DEN'").run()
    print(((System.nanoTime - t10)/1e9d)+"\n")
    results.show()
    
    /**
    * Airports within K hops
    */
    print("Airport K Hops: ")
    val t13 = System.nanoTime
    var airlinePat = createPattern(4)
    var airlineFill = createAirportsFilter(4, "'SEA'", "'DEN'")
    val paths = g.find(airlinePat).filter(airlineFill)
    print(((System.nanoTime - t13)/1e9d)+"\n")
    paths.show()
    
     /**
     * K Nearest Neighbor, 1 hop
     */
    /**
     * Sample of what the filter is supposed to do.
     * Pattern Matching given n hops
     */
    //    val pattern = "(x1) - [a] -> (x2); (x2) - [b] -> (x3)"
    print("airport reachability k hops: ")
    val t16 = System.nanoTime
    val airportReach = g.bfs.fromExpr("id='SEA'").toExpr("id<>'SEA'").maxPathLength(2).run()
    print(((System.nanoTime - t16)/1e9d)+"\n")

    /**
    // Testing Scale
    // Finding all rows with country == to given value
    println("Mid size search test")
    val t2 = System.nanoTime
    var testNodeMidInfo = spark.sql(
      """
        |SELECT testNode100Thou.Name
        |FROM testNode100Thou
        |WHERE testNode100Thou.Country = 50
        |""".stripMargin)
    println((System.nanoTime - t2)/1e9d)
    testNodeMidInfo.show()

    println("Large Scale Search Test")
    val t3 = System.nanoTime
    var testNodeLargeInfo = spark.sql(
      """
        |SELECT Name
        |FROM testNode1Mil
        |WHERE testNode1Mil.Country = 25
        |""".stripMargin)
    println((System.nanoTime - t3)/1e9d)
    testNodeLargeInfo.show()

    println("Huge Scale Search Test")
    val t30 = System.nanoTime
    var testNodeHugeInfo = spark.sql(
      """
        |SELECT Name
        |FROM testNode5Mil
        |WHERE testNode5Mil.Country = 25
        |""".stripMargin)
    println((System.nanoTime - t30)/1e9d)
    testNodeHugeInfo.show()

    // Test finding top k countries
    println("Mid Size Top K Countries")
    val t5 = System.nanoTime
    var midfreq = spark.sql(
      """
        |SELECT Country, COUNT(*)
        |FROM testNode100Thou
        |GROUP BY testNode100Thou.Country
        |ORDER BY 2 DESC
        |LIMIT 5
        |""".stripMargin
    )
    println((System.nanoTime - t5)/1e9d)

    println("Large Size Top K Countries")
    val t6 = System.nanoTime
    var largefreq = spark.sql(
      """
        |SELECT Country, COUNT(*)
        |FROM testNode1Mil
        |GROUP BY testNode1Mil.Country
        |ORDER BY 2 DESC
        |LIMIT 5
        |""".stripMargin
    )
    println((System.nanoTime - t6)/1e9d)

    println("Huge Size Top K Countries")
    val t60 = System.nanoTime
    var Hugefreq = spark.sql(
      """
        |SELECT Country, COUNT(*)
        |FROM testNode5Mil
        |GROUP BY testNode5Mil.Country
        |ORDER BY 2 DESC
        |LIMIT 5
        |""".stripMargin
    )
    println((System.nanoTime - t60)/1e9d)

    /**
     * The GraphFrame function.
     */
    // Creating Test graphs 
    //100000 Node graph with 1 million edges
    print("Load Time to generate Mid Size Graph: ")
    val t8 = System.nanoTime
    val vMidTest = testNode100Thou
    // Create an Edge DataFrame with "src" and "dst" columns
    val eMidTest = testEdges1Mil
    // Create a GraphFrame
    val gMidTest = GraphFrame(vMidTest, eMidTest)
    print(((System.nanoTime - t8)/1e9d)+"\n")

    /**
    / 1 million Nodes with 5 million edges
   */
    print("Load Time to generate Large Size Graph: ")
    val t9 = System.nanoTime
    val vLargeTest = testNode100Thou
    // Create an Edge DataFrame with "src" and "dst" columns
    val eLargeTest = testEdges5Mil
    // Create a GraphFrame
    val gLargeTest = GraphFrame(vLargeTest, eLargeTest)
    print(((System.nanoTime - t9)/1e9d)+"\n")

    /**
    / 5 million Nodes with 10 million edges
    */
    print("Load Time to generate Large Size Graph: ")
    val t20 = System.nanoTime
    val vHugeTest = testNode5Mil
    // Create an Edge DataFrame with "src" and "dst" columns
    val eHugeTest = testEdges10Mil
    // Create a GraphFrame
    val gHugeTest = GraphFrame(vHugeTest, eHugeTest)
    print(((System.nanoTime - t20)/1e9d)+"\n")

    /**
     * BFS Shortest Path, Seattle to Denver, should be a direct flight
     */
    // BFS on test datasets
    print("Mid size BFS: ")
    val t11 = System.nanoTime
    val midBfs = gMidTest.bfs.fromExpr("id= 1").toExpr("id=5").run()
    print(((System.nanoTime - t11)/1e9d)+"\n")

    print("Large Size BFS: ")
    val t12 = System.nanoTime
    val largeBfs = gLargeTest.bfs.fromExpr("id= 1").toExpr("id=5").run()
    print(((System.nanoTime - t12)/1e9d)+"\n")

    print("Huge Size BFS: ")
    val t21 = System.nanoTime
    val hugeBfs = gHugeTest.bfs.fromExpr("id= 1").toExpr("id=3").run()
    print(((System.nanoTime - t21)/1e9d)+"\n")


    /**
     * All neighbors of a node (only one hop)
     */
    /**
     * Sample of what the filter is supposed to do.
     * Pattern Matching given n hops
     */
    //    val pattern = "(x1) - [a] -> (x2); (x2) - [b] -> (x3)"
   /**
     * Testing on larger graphs
     */
    print("Mid size k hops: ")
    val t14 = System.nanoTime
    var midPat = createPattern(4)
    var midFill = createTestFilter(4, "'1'", "'5'")
    val testMidPaths = gMidTest.find(midPat).filter(midFill)
    print(((System.nanoTime - t14)/1e9d)+"\n")
    //testMidPaths.show()

    /**
     * 1 million Nodes with 5 million edges
     */
    print("Large size k hops: ")
    val t15 = System.nanoTime
    var largePat = createPattern(4)
    var largeFill = createTestFilter(4, "'1'", "'5'")
    val testLargePaths = gLargeTest.find(largePat).filter(largeFill)
    print(((System.nanoTime - t15)/1e9d)+"\n")
    //testLargePaths.show()

    /**
     * 5 million Nodes with 10 million edges
     */
    print("Huge size k hops: ")
    val t50 = System.nanoTime
    var hugePat = createPattern(4)
    var hugeFill = createTestFilter(4, "'1'", "'5'")
    val testHugePaths = gHugeTest.find(hugePat).filter(hugeFill)
    print(((System.nanoTime - t50)/1e9d)+"\n")


    /**
     * K Nearest Neighbor, 1 hop
     */
    print("Mid reachability k hops: ")
    val t17 = System.nanoTime
    val midReach = gMidTest.bfs.fromExpr("id=1").toExpr("id<>1").maxPathLength(2).run()
    print(((System.nanoTime - t17)/1e9d)+"\n")

    print("Large reachability k hops: ")
    val t18 = System.nanoTime
    val largeReach = gLargeTest.bfs.fromExpr("id=1").toExpr("id<>1").maxPathLength(2).run()
    print(((System.nanoTime - t18)/1e9d)+"\n")

    print("Huge reachability k hops: ")
    val t80 = System.nanoTime
    val hugeeReach = gHugeTest.bfs.fromExpr("id=1").toExpr("id<>1").maxPathLength(2).run()
    print(((System.nanoTime - t80)/1e9d)+"\n")
    */

    /**
     * Stop the Spark session
     */
    spark.stop()
  }

  def createPattern(hops:Int): String={
    val alpha = "abcdefghijklmnopqrstuvwxyz"
    var pat = "(x1) - [a] -> (x2)"
    for (a <- 2 to hops){
      pat = pat + "; (x" + a.toString + ") - [" + alpha.charAt(a-1) + "] -> " + "(x" + (a+1).toString + ")"
    }
    return pat
  }

  def createTestFilter(max:Int, src:String, dst:String): String={
    var fill = "x1.id=="+src+" and x"+(max+1).toString+".id=="+dst+" and x"+(max+1).toString+".id!="+src
    return fill
  }

  def createAirportsFilter(hops:Int, src:String, dst:String): String={
    val alpha = "abcdefghijklmnopqrstuvwxyz"
    var fill = "x1.id=="+src+" and x"+(hops+1).toString+".id=="+dst+" and x"+(hops+1).toString+".id!="+src
    for (a <- 2 to hops){
          fill = fill + " and " + alpha.charAt(a-2) + ".airline == "+alpha.charAt(a-1)+".airline"
    }
    return fill
  }
}
