package com.supergloo

import com.amazonaws.auth.{BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.supergloo.util.Logging
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisUtils

/**
  * // TODO
  * See http://www.supergloo.com/fieldnotes
  */
object SparkKinesisExample extends Logging {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Kinesis Read Sensor Data")
    conf.setIfMissing("spark.master", "local[*]")

//    // Check that all required args were passed in.
//    if (args.length != 3) {
//      System.err.println(
//        """
//          |Usage: KinesisWordCountASL <app-name> <stream-name> <endpoint-url> <region-name>
//          |
//          |    <app-name> is the name of the consumer app, used to track the read data in DynamoDB
//          |    <stream-name> is the name of the Kinesis stream
//          |    <endpoint-url> is the endpoint of the Kinesis service
//          |                   (e.g. https://kinesis.us-east-1.amazonaws.com)
//          |
//          |Generate input data for Kinesis stream using the example KinesisWordProducerASL.
//          |See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more
//          |details.
//        """.stripMargin)
//      System.exit(1)
//    }

 //   StreamingExamples.setStreamingLogLevels()
    //TODO - typesafe config rather than command
    val Array(appName, streamName, endpointUrl) = args

    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials != null,
      "No AWS credentials found. See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size

    val numStreams = numShards

    val batchInterval = Milliseconds(2000)

    val kinesisCheckpointInterval = batchInterval

    // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
    // DynamoDB of the same region as the Kinesis stream
    val regionName = RegionUtils.getRegionByEndpoint(endpointUrl).getName()

    val ssc = new StreamingContext(conf, batchInterval)

    // Create the Kinesis DStreams
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, appName, streamName, endpointUrl, regionName,
        InitialPositionInStream.LATEST, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
    }

    // Union all the streams
    val unionStreams = ssc.union(kinesisStreams)

    // Convert each line of Array[Byte] to String, and split into words

    val words = unionStreams.map { byteArray =>
      val Array(sensorId, temp, status) = new String(byteArray).split(",")
      SensorData(sensorId, temp, status)
    }

    // TODO - save raw data to cassandra

    // TODO - save aggregated to someplace else

    println(s"WORDS: ${words} size: ${words.count()}")
    // Map each word to a (word, 1) tuple so we can reduce by key to count the words
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)

    // TODO - add windowing?  sensor's highest value over the past 10 min?

    // Print the first 10 wordCounts
    wordCounts.print()

    // Start the streaming context and await termination
    ssc.start()
    ssc.awaitTermination()
  }
}
case class SensorData(id: String, currentTemp: String, status: String)