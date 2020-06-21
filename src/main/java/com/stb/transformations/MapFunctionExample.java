package com.stb.transformations;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

public class MapFunctionExample {

    public static void main(String[] args){

        Dataset<Row> customerData = SourceAndSink.readStreamCsv("Map Function", "csv/transformations");

        //Map function applies to each element of Dataset and it returns the result as new Dataset
        //below example i'm creating a combined column of CustomerID and  Genre
        Dataset<String> mappedData = customerData
                .map((MapFunction<Row,String>) data ->
                        data.getAs("CustomerID")+"_"+data.getAs("Genre")
                        , Encoders.STRING());

//        SourceAndSink.sinkConsole(customerData);
        SourceAndSink.sinkConsole(mappedData);
    }
}
/* customerData output
    20/06/20 14:45:51 INFO CodeGenerator: Code generated in 21.3649 ms
    +----------+------+---+------------------+----------------------+
    |CustomerID| Genre|Age|Annual Income (k$)|Spending Score (1-100)|
    +----------+------+---+------------------+----------------------+
    |      0001|  Male| 19|                15|                    39|
    |      0002|  Male| 21|                15|                    81|
    |      0003|Female| 20|                16|                     6|
 */

/*  mappedData output
20/06/20 14:47:17 INFO CodeGenerator: Code generated in 12.1442 ms
+-----------+
|      value|
+-----------+
|  0001_Male|
|  0002_Male|
|0003_Female|
|0004_Female|
 */