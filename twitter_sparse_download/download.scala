import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.net.URL
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import org.apache.commons.codec.binary.Base64

object MyDateTimeRange {
  def iter(from: ZonedDateTime, to: ZonedDateTime, stepInSeconds: Long): Iterator[ZonedDateTime] = Iterator.iterate(from)(_.plusSeconds(stepInSeconds)).takeWhile(!_.isAfter(to))
}

object TwitterSparseDownload {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: You must supply exactly 4 arguments in the following order: " +
        "<username>" +
        "<password>" +
        "<start datetime in UTC, inclusive>" +
        "<end datetime in UTC, inclusive>")
      System.exit(1)
    }

    val Array(username, password, fromString, toString) = args.take(4)
    val fromDT = ZonedDateTime.parse(fromString)
    val toDT = ZonedDateTime.parse(toString)

    val sparkConf = new SparkConf().setAppName("TwitterSparseDownload")
    val sc = new SparkContext(sparkConf)

    // This is to stop the noisy output in the console.
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val authString = username + ":" + password
    val authEncBytes = Base64.encodeBase64(authString.getBytes())
    val auth = new String(authEncBytes)
    val collectedTweets = new ListBuffer[String]()
    val newFileNames = new ListBuffer[String]()

    for (t <- MyDateTimeRange.iter(fromDT, toDT, 300)) {
      val t0 = t.format(DateTimeFormatter.ISO_INSTANT)
      val endTime = if (t.plusSeconds(299).isBefore(toDT)) t.plusSeconds(299) else toDT
      val t1 = endTime.format(DateTimeFormatter.ISO_INSTANT)
      val timeWindow = t0 + "," + t1
      val queryString = "lang:en%20and%20posted:" + timeWindow
      val insightsUrl = "https://cdeservice.mybluemix.net/api/v1/messages/search?size=500&q=" +
                        queryString
      val connection = new URL(insightsUrl).openConnection
      connection.setRequestProperty("Authorization", "Basic " + auth)
      val response = Source.fromInputStream(connection.getInputStream).mkString
      collectedTweets += response

      println("%s is done...".format(t0))

      newFileNames += "/home/twitter/archive/file_%s".
                      format(t.format(DateTimeFormatter.ofPattern("yyyyMMdd_HH:mm")))

      if (t.getMinute() % 10 == 5) {
        val rdd = sc.parallelize(collectedTweets.toList).repartition(1)
        rdd.saveAsTextFile(newFileNames(0))
        println("%s has been saved.".format(newFileNames(0)))
        collectedTweets.clear()
        newFileNames.clear()
      }
      Thread.sleep(2000)
    }

    // Final flush if there is any unsaved tweet.
    if (newFileNames.nonEmpty) {
      val rdd = sc.parallelize(collectedTweets.toList).repartition(1)
      rdd.saveAsTextFile(newFileNames(0))
      println("%s has been saved.".format(newFileNames(0)))
      collectedTweets.clear()
      newFileNames.clear()
    }
    println("All downloads have finished!")
  }
}

