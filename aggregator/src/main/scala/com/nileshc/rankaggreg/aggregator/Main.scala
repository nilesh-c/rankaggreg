package com.nileshc.rankaggreg.aggregator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

/**
 * Created by nilesh on 8/5/14.
 */
object Main {
  def main(args: Array[String]) {
    //    if(args.length < 1) {
    //      println("Usage: mvn scala:run -Dlauncher=Main <rank-list-file>")
    //      sys.exit(-1)
    //    }
    //    val sc = getSparkContext()
    //    val rankFile = args(0)
    //    val blockedMatrix = TransitionMatrixBuilder(sc).run(sc.textFile(rankFile))
    val sc = getSparkContext()
    val rdd = sc.textFile("/home/nilesh/rankaggreg/sushi3or/test.txt", 4)
//    val rdd = sc.parallelize(Array("0, 1, 2, 3",
//      "0, 3, 1, 2",
//      "0, 3, 2, 1"), 1)
    val tmb = new TransitionMatrixBuilder(sc)
    val a = tmb.run(rdd, 500, 4).collect()
    a.foreach(x => println(x._1._1 + "," + x._1._2 + "," + x._2))
//    a.foreach(x => println(x._1._1, x._1._2, x._2))

  }

  def getSparkContext(): SparkContext = {
    val conf = new SparkConf().setMaster("local[8]").setAppName("rank-aggregation")
    conf.setSparkHome("/home/nilesh/utils/spark-0.9.1-bin-hadoop2.2.0")
    conf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    //conf.set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrator", "com.nileshc.scalagraphfu.serialize.KryoExtractionRegistrator")
    conf.set("spark.kryoserializer.buffer.mb", "100")
    setSparkLogLevels()
    new SparkContext(conf)
  }

  /**
   * Set all loggers to the given log level.  Returns a map of the value of every logger
   * @param level
   * @param loggers
   * @return
   */
  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) =
  {
    loggers.map
      {
        loggerName =>
          val logger = Logger.getLogger(loggerName)
          val prevLevel = logger.getLevel()
          logger.setLevel(level)
          loggerName -> prevLevel
      }.toMap
  }

  /**
   * Sets log levels for Spark and its peripheral libraries to DistConfig.sparkLogLevel.
   */
  def setSparkLogLevels() =
  {
    setLogLevels(Level.WARN, Seq("org.apache", "spark", "org.eclipse.jetty", "akka"))
  }
}
