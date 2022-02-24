package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

import scala.collection.mutable.ListBuffer

object stackoverflow003 extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("error")

  val df1 = spark.read.json("C:\\Users\\HP\\Desktop\\StackOverflow\\StackOverflow\\src\\main\\resources\\003_df1data.txt")
  val df2 = spark.read.json("C:\\Users\\HP\\Desktop\\StackOverflow\\StackOverflow\\src\\main\\resources\\003_df2data.txt")

  df1.show()
  df2.show()

  val df1_columns = df1.columns.to[ListBuffer].-=("ID")
  val df2_columns = df2.columns.to[ListBuffer].-=("ID")

  val df1_columns_count = df1_columns.length
  val df2_columns_count = df2_columns.length

  var df1_stack_str = ""
  var df2_stack_str = ""

  df1_columns.foreach { column =>
    df1_stack_str += s"'$column',$column,"
  }
  df1_stack_str = df1_stack_str.substring(0,df1_stack_str.lastIndexOf(","))

  df2_columns.foreach { column =>
    df2_stack_str += s"'$column',$column,"
  }
  df2_stack_str = df2_stack_str.substring(0,df2_stack_str.lastIndexOf(","))

  val df11 = df1.selectExpr("id",s"stack($df1_columns_count,$df1_stack_str) as (column_name,value_from_df1)")
  val df21 = df2.selectExpr("id id_",s"stack($df2_columns_count,$df2_stack_str) as (column_name_,value_from_df2)")

  /*val df11 = df1.selectExpr("id","stack(3,'colA',colA,'colB',colB,'colC',colC) as (column_name,value_from_df1)")
  val df21 = df2.selectExpr("id id_","stack(3,'colA',colA,'colB',colB,'colC',colC) as (column_name_,value_from_df2)")*/

  df11.as("df11").join(df21.as("df21"),expr("df11.id=df21.id_ and df11.column_name=df21.column_name_"))
    .drop("id_","column_name_")
    .filter("value_from_df1!=value_from_df2")
    .show
}
