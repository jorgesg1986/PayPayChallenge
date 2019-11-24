package com.paypay.challenge

import com.paypay.challenge.Analytics.{SessionId, getData, getSessionSpec, sessionize}
import com.paypay.challenge.model.{Fields, SessionEvent}
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class AnalyticsTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val expectedUniqueHits = Array(
    (("117.241.97.140", 1469178028019L), 2L),
    (("117.239.195.66", 1437559228033L), 5L),
    (("223.176.154.91", 1437642028002L), 2L),
    (("123.242.248.130", 1437555627885L), 4L),
    (("117.195.91.36", 1437642028057L), 3L)
  )

  val expectedMaxDurations = Array(
    (("123.242.248.130", 1437555627885L), 163L),
    (("223.176.154.91", 1437642028002L), 66L)
  )

  var session: SparkSession = _

  var dataSource: DataFrame = _

  var groupedData: KeyValueGroupedDataset[SessionId, SessionEvent] = _

  override def beforeAll(): Unit = {

    session = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.shuffle.partitions", 20)
      .appName("PayPayChallengeTest")
      .getOrCreate()

    val ds = getData(session, "data/test.log")

    dataSource = sessionize(ds, getSessionSpec(Fields.clientIp, Fields.timestamp, 120000L))

    groupedData = Analytics.groupBySession(dataSource)
  }

  override def afterAll(): Unit = {
    session.stop()
  }

  "Analytics" should "return the unique URLs per session" in {

    val uniqueHits = Analytics.getUniqueHitsPerSession(groupedData, dataSource.sparkSession)

    uniqueHits.collect().foreach( elem => assert(expectedUniqueHits.contains(elem)))

  }

  it should "return the average session time" in {

    val avgSessionTime = Analytics.getAvgSessionTime(groupedData, dataSource.sparkSession)

    assert(avgSessionTime == 62)

  }

  it should "return the max durations" in {

    val maxDurations = Analytics.getMaxDuration(dataSource, 2)

    maxDurations.foreach(elem => assert(expectedMaxDurations.contains(elem)))

  }

}
