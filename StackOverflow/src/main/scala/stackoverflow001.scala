package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.mutable

object stackoverflow001 extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("error")
  val df = spark.read.json("F:\\devops\\stackoverlfow_qna\\StackOverflow\\src\\main\\resources\\001_data.txt")

  val category_Mapping = Map("Category1" -> Array("A", "B"), "Category2" -> Array("C"), "Category3" -> Array("B", "D"))

  val broadcast_category_Mapping = spark.sparkContext.broadcast(category_Mapping)

  println(broadcast_category_Mapping.value)

  def function(lst: mutable.WrappedArray[String], category_Mapping: Map[String, Array[String]]): Map[String, Int] = {
    var map: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map()
    lst.foreach { l =>
      category_Mapping.keys.foreach { key =>
        if(!map.contains(key))
          map(key) = 0
        if (category_Mapping(key).contains(l))
          map(key) = 1
      }
    }
    map.toMap
  }

  def output (category_Mapping: Map[String, Array[String]]) = udf { (lst: mutable.WrappedArray[String]) => {
    function(lst,category_Mapping)
  }
  }

  df.printSchema()
  df.withColumn("output", output(category_Mapping)(col("my_list"))).show(false)
}
