package com.stb.source_sink;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

import java.util.concurrent.TimeoutException;

public class CsvFileStream {

    public static void main(String[] args) throws TimeoutException {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("Csv stream")
                .getOrCreate();

        StructType schema = new StructType()
                .add("name", "string")
                .add("age", DataTypes.IntegerType,false, Metadata.empty());
    
        sparkSession
                .readStream()
                .option("sep",";")
                .option("header",true)
                .schema(schema)
                .csv("src/main/resources/csv/source_sinks")
                .writeStream()
                .queryName("CsvStream")
                .format("console")
                .start()
                .processAllAvailable();

        /*
        By default, Structured Streaming from file based sources requires you to specify the schema,
        rather than rely on Spark to infer it automatically.
         */

    }

    /* Output
    20/06/20 12:40:50 INFO CodeGenerator: Code generated in 22.8785 ms
    +-------+---+
    |   name|age|
    +-------+---+
    |Sanjaya| 30|
    |   Tony| 25|
    +-------+---+
     */


}
