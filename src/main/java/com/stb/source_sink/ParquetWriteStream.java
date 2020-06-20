package com.stb.source_sink;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class ParquetWriteStream {

    public static void main(String[] args) throws TimeoutException {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("Parquet write stream")
                .getOrCreate();

        StructType schema = new StructType()
                .add("name", "string")
                .add("age", DataTypes.IntegerType,false, Metadata.empty());
    
        sparkSession
                .readStream()
                .option("sep",";")
                .option("header",true)
                .schema(schema)
                .csv("src/main/resources/csv/source_sinks/")
                .writeStream()
                .queryName("Parquet write stream")
                .format("parquet")
                .option("path","src/main/resources/parquet")
                .option("checkpointLocation","src/main/resources/checkpoint")
                .start()
                .processAllAvailable();

        /*
        By default, Structured Streaming from file based sources requires you to specify the schema,
        rather than rely on Spark to infer it automatically.
         */

    }
}
