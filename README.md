## WIP

Update of old Streaming examples to Spark 2
Add new ones too
[Spark Streaming tutorials](https://supergloo.com/spark-streaming/)


### Kinesis example
Run `SparkKinesisExample` with arguments; i.e

com.supergloo.kinesis.SparkKinesisExample KinesisApp wm https://kinesis.us-east-1.amazonaws.com

Run/Debug Configuration Screenshot from IntelliJ Example

![alt text](https://raw.githubusercontent.com/tmcgrath/spark-2-streaming/master/reference/intellij-kinesis.png "Logo Title Text 1")

Running via `spark-submit` example

`spark-submit --class com.supergloo.kinesis.SparkKinesisExample ./target/scala-2.11/spark-2-streaming-assembly-1.0.jar KinesisApp wm https://kinesis.us-east-1.amazonaws.com`

In DSE 5.1.3 and above `spark-submit` example

`dse --framework spark-2.0 spark-submit --class com.supergloo.kinesis.SparkKinesisExample ./target/scala-2.11/spark-2-streaming-assembly-1.0.jar KinesisApp wm https://kinesis.us-east-1.amazonaws.com`

