package com.stb.source_sink;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class ParquetReadStream {

    public static void main(String[] args) throws TimeoutException {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("Parquet Read stream")
                .getOrCreate();

        StructType schema = new StructType()
                .add("name", "string")
                .add("age", DataTypes.IntegerType,false, Metadata.empty());
    
        sparkSession
                .readStream()
                .schema(schema)
                .parquet("src/main/resources/parquet")
                .writeStream()
                .queryName("Parquet Read stream")
                .format("console")
                .start()
                .processAllAvailable();

    }

    /* Output
    20/06/20 12:39:33 INFO WriteToDataSourceV2Exec: Data source write support org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite@6370b109 is committing.
    -------------------------------------------
    Batch: 0
    -------------------------------------------
    20/06/20 12:39:34 INFO CodeGenerator: Code generated in 11.3104 ms
    20/06/20 12:39:34 INFO CodeGenerator: Code generated in 16.9033 ms
    +-------+---+
    |   name|age|
    +-------+---+
    |Sanjaya| 30|
    |   Tony| 25|
    +-------+---+

     */

}
