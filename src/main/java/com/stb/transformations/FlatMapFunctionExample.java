package com.stb.transformations;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.Arrays;

public class FlatMapFunctionExample {

    public static void main(String[] args){

        Dataset<Row> input = SourceAndSink.readStreamText("FlatMap Function","text");

        Dataset<String> words = input
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String,String>) //String input string output
                        line -> Arrays.asList(line.split(" ")).iterator()
                        ,Encoders.STRING());//Output encode to string

//        Dataset<Row> count = words.groupBy("value").count();
        SourceAndSink.sinkConsole(words);
    }
}

/* Output
-------------------------------------------
Batch: 0
-------------------------------------------
20/06/20 13:26:23 INFO CodeGenerator: Code generated in 9.5886 ms
+----------+
|     value|
+----------+
|      This|
|     lines|
| DataFrame|
|represents|
|        an|
| unbounded|
|     table|
 */