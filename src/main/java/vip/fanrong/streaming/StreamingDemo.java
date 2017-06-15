package vip.fanrong.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("Spark Streaming Test on Java");
        sparkConf.setMaster("spark://ubuntu-host-01:7077");

        // Streaming
        JavaStreamingContext streamCtx = new JavaStreamingContext(sparkConf, Durations.minutes(1));

        //        streamCtx.checkpoint("hdfs://ubuntu-host-01:9000/spark_checkpoint");

        JavaDStream<String> streamIn =
                streamCtx.textFileStream("hdfs://ubuntu-host-01:9000/spark_stream_in/streaming");

        JavaPairDStream<String, String> campaignAd = streamIn.mapToPair(line -> {
            String[] array = line.split("\\|");
            System.out.println("########## line = " + line + " length=" + array.length);
            return new Tuple2<String, String>(array[0], array[2]);
        });

        //        campaignAd.checkpoint(Durations.minutes(1));

        campaignAd.print();

        streamCtx.start();

        streamCtx.awaitTermination();

        streamCtx.close();
    }
}
