package com.stb.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class GroupByExample {
    public static void main(String[] args) {
        Dataset<Row> customerData = SourceAndSink.readStreamCsv("GroupBy Function","csv/functions");
//        SourceAndSink.sinkConsole(customerData);

        Dataset<Row> groupByGenre = customerData.groupBy("Genre").count();

        //SourceAndSink.sinkConsole(customerData); //This will give below error
        /*org.apache.spark.sql.AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;;
            Aggregate [Genre#27], [Genre#27, count(1) AS count#42L]*/

        SourceAndSink.sinkConsoleForAggregation(groupByGenre);//With output mode complete
    }
}
/*  customerData  output
-------------------------------------------
20/06/20 17:49:03 INFO CodeGenerator: Code generated in 18.1969 ms
+----------+------+---+------------------+----------------------+
|CustomerID| Genre|Age|Annual Income (k$)|Spending Score (1-100)|
+----------+------+---+------------------+----------------------+
|      0001|  Male| 19|                15|                    39|
|      0002|  Male| 21|                15|                    81|
|      0003|Female| 20|                16|                     6|
|      0004|Female| 23|                16|                    77|
|      0005|Female| 31|                17|                    40|
|      0006|Female| 22|                17|                    76|
|      0007|Female| 35|                18|                     6|
|      0008|Female| 23|                18|                    94|
|      0009|  Male| 64|                19|                     3|
*/
//#### After groupBy function
/*  groupByGenre   output
20/06/20 17:58:21 INFO CodeGenerator: Code generated in 13.7192 ms
+------+-----+
| Genre|count|
+------+-----+
|Female|  112|
|  Male|   88|
+------+-----+
*/
