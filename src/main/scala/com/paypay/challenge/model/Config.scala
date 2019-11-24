package com.paypay.challenge.model

import scopt.OptionParser

/**
 * Config case class with some options to be configured from the command line via Scopt library
 * @param source
 * @param window
 * @param sessions
 * @param output
 */
case class Config(source: String = "data/2015_07_22_mktplace_shop_web_log_sample.log",
                  window: Long = 900000L,
                  sessions: Int = 15,
                  output: String = "output")

object Config {
  def getParser: OptionParser[Config] = {
    new OptionParser[Config]("PayPayChallenge") {
      head("scopt", "3.x")

      opt[String]('s', "source")
        .required()
        .action((x, c) => c.copy(source = x))
        .text("source is the input data to be processed.")

      opt[Long]('w', "window")
        .action((x, c) => c.copy(window = x))
        .text("window is the time in ms to sessionize the data")

      opt[Int]('m', "sessions")
        .action((x, c) => c.copy(sessions = x))
        .text("sessions is the number of most engaged users to show")

      opt[String]('o', "output")
        .action((x, c) => c.copy(output = x))
        .text("output is the path where output files will be stored.")

    }

  }
}