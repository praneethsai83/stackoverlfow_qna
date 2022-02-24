package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, from_json, udf}
import org.json.JSONArray

object stackoverflow002 extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  sc.setLogLevel("error")

  val toArray = udf { (data: String) => {
    val jsonArray = new JSONArray(data)
    var arr: Array[String] = Array()
    val objects = (0 until jsonArray.length).map(x => jsonArray.getJSONObject(x))
    objects.foreach { elem =>
      arr :+= elem.toString
    }
    arr
  }
  }

  val df = spark.read.json(sc.parallelize(Seq("""{"column_name":"[{\"id\":28,\"type\":\"Home\"},{\"id\":18,\"type\":\"Kitchen\"}]"}""")))
  df.show(false)

  df.printSchema()

  df.withColumn("column_name", toArray(col("column_name"))).show(false)
  df.withColumn("column_name", toArray(col("column_name"))).printSchema

  val df1 = df.withColumn("column_name", toArray(col("column_name")))
    .withColumn("column_name", explode(col("column_name")))

  val schema = spark.read.json(df1.select("column_name").rdd.map(x => x(0).toString)).schema
  df1.withColumn("column_name", from_json(col("column_name"), schema))
    .select(col("column_name.*"))
    .show(false)
}
