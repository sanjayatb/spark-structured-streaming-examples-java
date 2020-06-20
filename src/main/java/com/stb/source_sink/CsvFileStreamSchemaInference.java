package com.stb.source_sink;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class CsvFileStreamSchemaInference {

    public static void main(String[] args) throws TimeoutException {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("Csv schema inference stream")
                .getOrCreate();

        sparkSession.sql("set spark.sql.streaming.schemaInference=true");

        sparkSession
                .readStream()
                .option("sep",";")
                .option("header",true)
                .csv("src/main/resources/csv/")
                .writeStream()
                .queryName("Csv schema inference stream")
                .format("console")
                .start()
                .processAllAvailable();
    }

    /*  Output
    -------------------------------------------
    Batch: 0
    -------------------------------------------
    20/06/20 12:43:57 INFO CodeGenerator: Code generated in 14.5699 ms
    +-------+---+
    |   name|age|
    +-------+---+
    |Sanjaya| 30|
    |   Tony| 25|
    +-------+---+
     */
}
