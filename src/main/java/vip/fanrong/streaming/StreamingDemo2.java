package vip.fanrong.streaming;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import scala.Tuple3;

public class StreamingDemo2 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("Spark Streaming Test on Java");
        sparkConf.setMaster("spark://ubuntu-host-01:7077");
        //        sparkConf.set("spark.driver.allowMultipleContexts", "true");

        // RDD campaignIdToName
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, String> campaignIdToName =
                context.textFile("hdfs://ubuntu-host-01:9000/spark_stream_in/campaign").mapToPair(line -> {
                    String[] array = line.split("\\|");
                    return new Tuple2<String, String>(array[0], array[1]);
                }).partitionBy(new HashPartitioner(2)).cache();

        // Streaming
        JavaStreamingContext streamCtx = new JavaStreamingContext(context, Durations.minutes(1));

        streamCtx.checkpoint("hdfs://ubuntu-host-01:9000/spark_checkpoint");

        JavaDStream<String> streamIn =
                streamCtx.textFileStream("hdfs://ubuntu-host-01:9000/spark_stream_in/streaming");

        JavaDStream<Tuple3<String, String, String>> campaignJoined = streamIn.mapToPair(line -> {
            String[] array = line.split("\\|");
            System.out.println("########## line = " + line + " length=" + array.length);
            return new Tuple2<String, String>(array[0], array[2]);
        }).transform(rdd -> rdd.leftOuterJoin(campaignIdToName).map(record -> {
            String campaignID = record._1;
            String adName = record._2._1;
            Optional<String> campaignName = record._2._2;
            return new Tuple3<String, String, String>(campaignID, adName, campaignName.get());
        }));

        campaignJoined.checkpoint(Durations.minutes(1));

        campaignJoined.foreachRDD(rdd -> {
            rdd.saveAsTextFile("hdfs://ubuntu-host-01:9000/spark_stream_out");
        });

        streamCtx.start();

        streamCtx.awaitTermination();

        streamCtx.close();
    }
}
