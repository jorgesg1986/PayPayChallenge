package com.paypay.challenge.model

import java.time.ZonedDateTime

case class SlimEvent(timestamp: Long, clientIp: String, URL: String)

case class SessionEvent(timestamp: Long, clientIp: String, URL: String, max: Long, duration: Long)

object SlimEvent {

  def apply(timestamp: String, clientIp: String, URL: String): SlimEvent = {
    SlimEvent(
      ZonedDateTime.parse(timestamp).toInstant.toEpochMilli,
      clientIp.split(':').apply(0),
      URL.split(' ').apply(1))
  }
}