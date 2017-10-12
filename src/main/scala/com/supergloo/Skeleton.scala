package com.supergloo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Skeleton {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Skeleton")
    conf.setIfMissing("spark.master", "local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.sqlContext.implicits._

    val df2 = spark.range(1,100)
    val df3 = df2.map( i => i + 1)

    df3.collect().foreach(println)

    spark.stop()
    sys.exit(0)
  }
}
