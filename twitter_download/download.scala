import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.net.URL
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import org.apache.commons.codec.binary.Base64

object TwitterDownload {
  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println("Usage: You must supply exactly 6 arguments in the following order: " +
        "<username>" +
        "<password>" +
        "<month>" +
        "<day>" +
        "<start hour>" +
        "<end hour, inclusive>")
      System.exit(1)
    }

    val Array(username, password, m_0, d_0, h_0, h_1) = args.take(6)
    val month = m_0.toInt
    val day = d_0.toInt
    val hour0 = h_0.toInt
    val hour1 = h_1.toInt

    val sparkConf = new SparkConf().setAppName("TwitterDownload")
    val sc = new SparkContext(sparkConf)

    // This is to stop the noisy output in the console.
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    val authString = username + ":" + password
    val authEncBytes = Base64.encodeBase64(authString.getBytes())
    val auth = new String(authEncBytes)
    val collectedTweets = new ListBuffer[String]()

    for (h <- hour0 to hour1) {
      for (m <- 0 to 59) {
        for (s <- Range(0, 60, 15)) {
          val hh = if (s == 45) { if (m == 59) { if (h == 23) h else h + 1 } else h } else h
          val mm = if (s == 45) { if (m == 59) { if (h == 23) m else 0 } else m + 1 } else m
          val ss = if (s == 45) { if (m == 59) { if (h == 23) s + 14 else 0 } else 0 } else s + 15
          val timeWindow = "2016-%02d-%02dT%02d:%02d:%02dZ,2016-%02d-%02dT%02d:%02d:%02dZ".
                           format(month, day, h, m, s, month, day, hh, mm, ss)
          val queryString = "lang:en%20and%20posted:" + timeWindow
          val insightsUrl = "https://cdeservice.mybluemix.net/api/v1/messages/search?size=500&q=" +
                            queryString
          val connection = new URL(insightsUrl).openConnection
          connection.setRequestProperty("Authorization", "Basic " + auth)
          val response = Source.fromInputStream(connection.getInputStream).mkString
          collectedTweets += response
          Thread.sleep(3000)
        }
        println("%02d:%02d is done...".format(h, m))
        if (m % 10 == 9) {
          val rdd = sc.parallelize(collectedTweets.toList).repartition(1)
          val fileName = "/home/twitter/archive/file_2016%02d%02d_%02d:%02d".
                         format(month, day, h, m - 9)
          rdd.saveAsTextFile(fileName)
          println("%s has been saved.".format(fileName))
          collectedTweets.clear()
        }
      }
    }
  }
}

