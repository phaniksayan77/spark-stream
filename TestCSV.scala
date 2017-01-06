package com.humana

import scala.Option.option2Iterable
import scala.util.matching.Regex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestCSV {

  def main(args: Array[String]): Unit = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("TestCSV").setMaster("local")
    val sc = new SparkContext(conf)

    val file = sc.textFile("C:\\Dev\\Transactions.csv")

    val grossories = Seq("KROGER", "COSTCO", "WM", "TOM THUMB", "TARGET")
    val indianGrossories = Seq("PATEL", "INDIA")
    val restaurants = Seq("BAWARCHI", "SARAVANA", "URBAN", "KUMAR", "HOT")
    val entertainments = Seq("NETFLIX", "REGAL")
    val utils = Seq("AZUMA", "TWC", "ENERGY")

    val keys = file.map(line => {
      val l = line.asInstanceOf[String]
      val values: Array[String] = l.split(",")

      val billPay = values(2)

      val grossoryItem = grossories.filter(a => billPay.contains(a))
      val indianGrossory = indianGrossories.filter(a => billPay.contains(a))
      val restaurant = restaurants.filter(a => billPay.contains(a))
      val entertainment = entertainments.filter(a => billPay.contains(a))
      val utilityPayments = utils.filter(a => billPay.contains(a))

      if (grossoryItem.size > 0) {
        values(1) = "Grossories"
      } else if (indianGrossory.size > 0) {
        values(1) = "Indian Grossories"
      } else if (entertainment.size > 0) {
        values(1) = "Entertainment"
      } else if (restaurant.size > 0) {
        values(1) = "Restaurant"
      } else if (utilityPayments.size > 0) {
        values(1) = "Utilities"
      } else {
        values(1) = "Other"
      }

      //println(values(0) + ":" + values(1) + ":" + values(4))

      (values(1), (values(4)).toDouble)
    }).filter(v => (v._2 < 0)).reduceByKey(_ + _)

    keys.foreach(v => {
      println(v._1 + ":" + Math.abs(v._2))
    })
  }
}
