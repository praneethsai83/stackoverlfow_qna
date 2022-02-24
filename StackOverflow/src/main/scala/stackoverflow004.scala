package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object stackoverflow004 extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("error")

  spark.read.json("C:\\Users\\HP\\Desktop\\StackOverflow\\StackOverflow\\src\\main\\resources\\004_data.txt")
    .selectExpr("stack(1,'123',`123`) as (ID,value)")
    .withColumn("Party1",col("value.Partyl"))
    .withColumn("Party2",col("value.Party2"))
    .selectExpr("ID","stack(2,'Party1',Party1,'Party2',Party2) as (key1,value1)")
    .select(col("ID"),col("value1.*")).show
}
