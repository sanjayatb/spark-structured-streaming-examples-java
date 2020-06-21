package com.stb.transformations;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class FilterExample {
    public static void main(String[] args) {
        Dataset<Row> customerData = SourceAndSink.readStreamCsv("Filter Function", "csv/transformations");
//        SourceAndSink.sinkConsole(customerData);

        Dataset<Row> filterAgeUntyped = customerData
                        .where("Age > 20 AND Age < 30"); // using untyped APIs

        Dataset<Row> filterAge = customerData
                        .filter((FilterFunction<Row>)
                                row -> Integer.valueOf(row.getAs("Age")) >= 50
                                        && Integer.valueOf(row.getAs("Age")) <= 60);

//        SourceAndSink.sinkConsole(filterAgeUntyped);
        SourceAndSink.sinkConsole(filterAge);
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
//#### After filter function
/*  Untyped apis : where | filterAgeUntyped output
-------------------------------------------
20/06/20 18:08:49 INFO CodeGenerator: Code generated in 14.5039 ms
+----------+------+---+------------------+----------------------+
|CustomerID| Genre|Age|Annual Income (k$)|Spending Score (1-100)|
+----------+------+---+------------------+----------------------+
|      0002|  Male| 21|                15|                    81|
|      0004|Female| 23|                16|                    77|
|      0006|Female| 22|                17|                    76|
|      0008|Female| 23|                18|                    94|
|      0014|Female| 24|                20|                    77|
|      0016|  Male| 22|                20|                    79|
|      0022|  Male| 25|                24|                    73|
|      0026|  Male| 29|                28|                    82|
*/
//######### Filter function
/*   filterAge   output
-------------------------------------------
20/06/20 18:13:50 INFO CodeGenerator: Code generated in 15.7166 ms
+----------+------+---+------------------+----------------------+
|CustomerID| Genre|Age|Annual Income (k$)|Spending Score (1-100)|
+----------+------+---+------------------+----------------------+
|      0013|Female| 58|                20|                    15|
|      0019|  Male| 52|                23|                    29|
|      0025|Female| 54|                28|                    14|
|      0031|  Male| 60|                30|                     4|
|      0033|  Male| 53|                33|                     4|
|      0047|Female| 50|                40|                    55|
|      0054|  Male| 59|                43|                    60|
|      0055|Female| 50|                43|                    45|
|      0057|Female| 51|                44|                    50|
|      0060|  Male| 53|                46|                    46|
 */
