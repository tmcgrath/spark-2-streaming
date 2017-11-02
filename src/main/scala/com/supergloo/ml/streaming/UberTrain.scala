package com.supergloo.ml.streaming

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans

/**
  * Refactor of
  * https://github.com/caroljmcdonald/spark-ml-kmeans-uber/blob/master/ClusterUber.scala
  * Not that the original is bad or incorrect, I just wanted to play with it and
  * adjust it so I can learn from it.
  *
  * Consider this refactor a hat tip to the original
  *
  * Assumes the training data csv has been downloaded from
  * https://github.com/caroljmcdonald/spark-ml-kmeans-uber/tree/master/data
  * and saved in /tmp/csv as you'll see in code below
  */
object UberTrain {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Uber Train")
    conf.setIfMissing("spark.master", "local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read.option("header", "false")
      .csv("file:///tmp/csv/*.csv")
      .withColumnRenamed("_c0", "dt")
      .withColumnRenamed("_c1", "lat")
      .withColumnRenamed("_c2", "lon")
      .withColumnRenamed("_c3", "base")
      .withColumn("dt", to_date($"dt"))
      .withColumn("lat", $"lat".cast("decimal"))
      .withColumn("lon", $"lon".cast("decimal"))
      .withColumn("base", $"base")
      .as[Uber]

    df.cache
    df.show
    df.schema

    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(df)
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), 5043)

    val kmeans = new KMeans().setK(10).setFeaturesCol("features").setMaxIter(3)
    val model = kmeans.fit(trainingData)

    println("Final Centers: ")
    model.clusterCenters.foreach(println)

    val categories = model.transform(testData)

    categories.show
    categories.createOrReplaceTempView("uber")

    //Which cluster had highest number of pickups by month, day, hour?
    categories.select(month($"dt").alias("month"), dayofmonth($"dt")
      .alias("day"), hour($"dt").alias("hour"), $"prediction")
      .groupBy("month", "day", "hour", "prediction").
      agg(count("prediction").alias("count")).orderBy("day", "hour", "prediction").show

    //Which cluster had highest number of pickups by hour?
    categories.select(hour($"dt").alias("hour"), $"prediction")
      .groupBy("hour", "prediction").agg(count("prediction")
      .alias("count")).orderBy(desc("count")).show

    // number of pickups per cluster
    categories.groupBy("prediction").count().show()


    // pick your preference DataFrame API above or can use SQL directly
    spark.sql(" select prediction, count(prediction) as count from uber group by prediction").show
    spark.sql("SELECT hour(uber.dt) as hr,count(prediction) as ct FROM uber group By hour(uber.dt)").show

    // to save the categories dataframe as json data
    //  categories.select("dt", "base", "prediction").write.format("json").save("uberclusterstest")
    //  to save the model
    //  model.write.overwrite().save("/user/user01/data/savemodel")
    //  to re-load the model
    //  val sameModel = KMeansModel.load("/user/user01/data/savemodel")
  }
}
case class Uber(dt: java.sql.Timestamp, lat: BigDecimal,
                lon: BigDecimal, base: String)
