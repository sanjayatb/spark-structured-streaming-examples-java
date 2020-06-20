package com.stb.source_sink;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class SocketStream {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("Socket stream")
                .getOrCreate();

        sparkSession
                .readStream()
                .format("socket")
                .option("host","localhost")
                .option("port",9999)
                .load()
                .writeStream()
                .queryName("SocketStream")
                .format("console")
                .start()
                .awaitTermination();
    }
}
