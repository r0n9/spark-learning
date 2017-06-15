package vip.fanrong.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class CoreDemo {
    public static void main(String[] args) {
        //        System.setProperty("hadoop.home.dir", "C:\\winutils");

        Configuration haddopConf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(haddopConf);
            fs.deleteOnExit(new Path("hdfs://ubuntu-host-01:9000/test/result"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        SparkConf conf = new SparkConf().setAppName("Spark Core Test on Java");
        conf.setMaster("spark://ubuntu-host-01:7077");

        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> input = context.textFile("hdfs://ubuntu-host-01:9000/test/INSTALL.LOG");
        input.persist(StorageLevel.MEMORY_ONLY());
        // input.cache();

        input.filter(line -> line.startsWith("Source"))
                .mapToPair(line -> new Tuple2<String, String>(line.split("\\|")[3], line)).sortByKey(true)
                .reduceByKey((line1, line2) -> (line1 + "; " + line2)).values()
                .saveAsTextFile("hdfs://ubuntu-host-01:9000/test/result");

        context.close();
    }
}
