package com.stb.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class OrderByExample {
    public static void main(String[] args) {
        Dataset<Row> customerData = SourceAndSink.readStreamCsv("OrderBy Function","csv/functions");
//        SourceAndSink.sinkConsole(customerData);

        Dataset<Row> orderByAge = customerData
                .selectExpr("CAST(Age as bigint)","CustomerID")
                .groupBy("CustomerID","Age")
                .avg("Age")
                .orderBy("Age");

//        Dataset<Row> sort = customerData
//                .selectExpr("CAST(Age as bigint)","CustomerID")
//                .groupBy("CustomerID","Age")
//                .avg("Age")
//                .sort("Age");
//
        SourceAndSink.sinkConsoleForAggregation(orderByAge);//With output mode complete
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
/*  orderByAge   output
-------------------------------------------
+----------+---+--------+
|CustomerID|Age|avg(Age)|
+----------+---+--------+
|      0034| 18|    18.0|
|      0115| 18|    18.0|
|      0066| 18|    18.0|
|      0092| 18|    18.0|
|      0114| 19|    19.0|
|      0062| 19|    19.0|
|      0139| 19|    19.0|
|      0116| 19|    19.0|
|      0112| 19|    19.0|
|      0001| 19|    19.0|
|      0163| 19|    19.0|
|      0069| 19|    19.0|
|      0135| 20|    20.0|
|      0100| 20|    20.0|
|      0040| 20|    20.0|
|      0003| 20|    20.0|
|      0018| 20|    20.0|
|      0106| 21|    21.0|
|      0002| 21|    21.0|
|      0036| 21|    21.0|
+----------+---+--------+
only showing top 20 rows
*/
