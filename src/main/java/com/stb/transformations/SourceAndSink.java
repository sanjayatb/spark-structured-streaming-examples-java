package com.stb.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeoutException;

public class SourceAndSink {

    private static SparkSession sparkSession;
    private static String _appName;
    static {
        sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .getOrCreate();
        sparkSession.sql("set spark.sql.streaming.schemaInference=true");
    }

    public static Dataset<Row> readStreamCsv(String appName,String folder){
        sparkSession.conf().set("spark.app.name",appName);
        _appName = appName;
        Dataset<Row> customerData = sparkSession
                .readStream()
                .option("header", true)
                .csv("src/main/resources/"+folder);

        return customerData;
    }

    public static Dataset<Row> readStreamText(String appName,String folder){
        sparkSession.conf().set("spark.app.name",appName);
        _appName = appName;
        Dataset<Row> customerData = sparkSession
                .readStream()
                .text("src/main/resources/"+folder);

        return customerData;
    }

    public static void sinkConsole(Dataset<?> dataset){
        try {
            dataset.writeStream()
                    .queryName(_appName+" stream")
//                    .outputMode("complete")
                    .format("console")
                    .start()
                    .processAllAvailable();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     * Need output mode complete or watermark for aggregation
     * @param dataset
     */
    public static void sinkConsoleForAggregation(Dataset<?> dataset){
        try {
            dataset.writeStream()
                    .queryName(_appName+" stream")
                    .outputMode("complete")
                    .format("console")
                    .start()
                    .processAllAvailable();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }


}
