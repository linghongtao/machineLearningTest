package SparkStreaming.networkstream

import org.apache.spark.internal.Logging
import org.apache.log4j.{Level, Logger}
/**
  * Created by root on 12/6/17.
  */
object StreamingExamples extends  Logging{
  def main(args: Array[String]): Unit = {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
