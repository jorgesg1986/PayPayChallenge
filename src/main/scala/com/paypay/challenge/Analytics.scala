package com.paypay.challenge

import java.io.File
import java.nio.charset.Charset

import com.paypay.challenge.model.{Config, Fields, SessionEvent, SlimEvent}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

object Analytics {

  type SessionId = (String, Long) // Type alias to identify a session
  val avgSessionTimeFile = "avgSessionTime.txt"
  val mostEngagedUsersFile = "mostEngagedUsers"
  val uniqueURLsPerSessionFile = "uniqueURLsPerSession"

  val logger: Logger = LoggerFactory.getLogger("com.paypay.challenge.Analytics")

  def main(args: Array[String]): Unit = {


    //      .master("local")
    //      .config("spark.sql.shuffle.partitions", 20)
    val config = Config.getParser.parse(args, Config())

    config match {
      case None => logger.error("Error while parsing configuration. Exiting...")
      case Some(config) =>
        val session = SparkSession
          .builder()
          .appName("PayPayChallenge")
          .getOrCreate()
        process(config, session)
        session.stop()
    }

  }

  /**
   * Process method where all the operations will be called and the results stored on disk.
   * Ideally storing the data should be separated in its own method, in order to easily test the process function altogether.
   * @param config Parsed configuration from the command line
   * @param session Spark Session to compute the results
   */
  def process(config: Config, session: SparkSession): Unit = {
    val ds = getData(session, config.source)

    val sessionizeData = sessionize(ds, getSessionSpec(Fields.clientIp, Fields.timestamp, config.window))

    val mostEngagedUsers = getMaxDuration(sessionizeData, config.sessions)

    val groupedData = groupBySession(sessionizeData)

    val avgSession = getAvgSessionTime(groupedData, session)

    val uniqueURLs = getUniqueHitsPerSession(groupedData, session)

    val engagedUsersFile = new File(config.output + "/" + mostEngagedUsersFile)

    val append = true

    mostEngagedUsers.foreach(tuple => FileUtils
      .writeStringToFile(engagedUsersFile, tuple.toString + "\n", Charset.forName("UTF-8"), append))

    val avgSessionTime = new File(config.output + "/" + avgSessionTimeFile)

    FileUtils.writeStringToFile(avgSessionTime, avgSession.toString, Charset.forName("UTF-8"))

    uniqueURLs.write.parquet(config.output + "/" + uniqueURLsPerSessionFile)
  }

  def getData(session: SparkSession, path: String): Dataset[SlimEvent] = {

    import session.implicits._

    val csvDF = session.read
      .option("sep", " ")
      .option("enforceSchema", false)
      .csv(path)

    val columns = Array(csvDF.columns(0), csvDF.columns(2), csvDF.columns(11))

    csvDF
      .select(columns.head, columns.tail : _*)
      .withColumnRenamed(columns(0), Fields.timestamp)
      .withColumnRenamed(columns(1), Fields.clientIp)
      .withColumnRenamed(columns(2), Fields.URL)
      .repartition(col(Fields.clientIp))
      .map(r => SlimEvent(r.getString(0), r.getString(1), r.getString(2)))

  }

  def getSessionSpec(partitionColumn: String, orderingColumn: String, windowMillis: Long): WindowSpec = {
    Window
      .partitionBy(partitionColumn) // clientIP
      .orderBy(orderingColumn) // timestamp
      .rangeBetween(windowMillis * -1, windowMillis)
  }

  /**
   * Function to get sessions and several requested metrics
   * @param ds The dataset with of SlimEvent with the data to sessionize and get insights from
   * @param sessionSpec The window specification used to sessionize this dataset
   * @return The DataFrame with sessions and insights
   */
  def sessionize(ds: Dataset[SlimEvent], sessionSpec: WindowSpec): DataFrame = {
    import ds.sparkSession.implicits._
    ds
      .withColumn("min", min(Fields.timestamp).over(sessionSpec).as[Long])
      .withColumn("max", max(Fields.timestamp).over(sessionSpec).as[Long])
      .withColumn(Fields.duration, ($"max" - $"min").as[Long])
  }

  def getUniqueHitsPerSession(ds: KeyValueGroupedDataset[SessionId, SessionEvent], session: SparkSession): Dataset[(SessionId, Long)] = {
    import session.implicits._
    ds
      .agg(countDistinct(col(Fields.URL)).as("unique_hits").as[Long]) // Aggregate by URL
  }

  /**
   * Function that returns the longest sessions
   * @param df Dataframe with sessions
   * @param numberOfSessions Number of sessions to be returned
   */
  def getMaxDuration(df: DataFrame, numberOfSessions: Int): Array[(SessionId, Long)] = {
    df
      .sort(col(Fields.duration).desc)
      .select(col(Fields.clientIp), col("min"), col(Fields.duration))
      .head(numberOfSessions)
      .map(row => ((row.getAs[String](Fields.clientIp), row.getAs[Long]("min")), row.getAs[Long](Fields.duration)))
  }

  /**
   * Function to get the Average Session Time
   * @param ds Dataset with a map of SessionId -> SessionEvent
   * @param session SparkSession used to run the computations
   * @return Average Session Time in ms
   */
  def getAvgSessionTime(ds: KeyValueGroupedDataset[SessionId, SessionEvent], session: SparkSession): Long = {
    import session.implicits._

    val totalDuration = ds.mapGroups{ (_, iter) =>
      iter.next().duration // duration is always same for each group
    }.reduce(_ + _)

    totalDuration / ds.keys.count()
  }

  /**
   * Function where the data will be actually sessionized
   * @param df The Dataframe to be sessionized.
   * @return Dataset with a mapping of SessionId -> SessionEvent
   */
  def groupBySession(df: DataFrame): KeyValueGroupedDataset[SessionId, SessionEvent] = {
    import df.sparkSession.implicits._

    df.groupByKey{row => // A session can be identified by client Ip and start time
      (row.getAs[String](Fields.clientIp), row.getAs[Long]("min"))
    }.mapValues{row =>
      SessionEvent(
        row.getAs[Long](Fields.timestamp),
        row.getAs[String](Fields.clientIp),
        row.getAs[String](Fields.URL),
        row.getAs[Long]("max"),
        row.getAs[Long](Fields.duration))
    }
  }

}

