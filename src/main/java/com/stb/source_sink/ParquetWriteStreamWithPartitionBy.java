package com.stb.source_sink;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class ParquetWriteStreamWithPartitionBy {

    public static void main(String[] args) throws TimeoutException {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("Parquet write stream")
                .getOrCreate();

        StructType schema = new StructType()
                .add("year", DataTypes.StringType,false)
                .add("name", "string")
                .add("age", DataTypes.IntegerType);
    
        sparkSession
                .readStream()
                .option("sep",";")
                .option("header",true)
                .schema(schema)
                .csv("src/main/resources/csv/source_sinks/")
                .writeStream()
                .queryName("Parquet write stream")
                .format("parquet")
                .partitionBy("year")
                .option("path","src/main/resources/parquet")
                .option("checkpointLocation","src/main/resources/checkpoint")
                .start()
//                .start("src/main/resources/parquet") //Can use without path option
                .processAllAvailable();

        /*
        By default, Structured Streaming from file based sources requires you to specify the schema,
        rather than rely on Spark to infer it automatically.
         */

    }
}
