package com.stb.functions;

import com.stb.transformations.SourceAndSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class JsonFunctions {

    public static void main(String[] args) {
        Dataset<Row> customerData = SourceAndSink.readStreamCsv("Json Functions", "csv/transformations");

        StructType schema = new StructType()
                .add(new StructField("Genre", DataTypes.StringType,false, Metadata.empty()))
                .add(new StructField("Age", DataTypes.StringType,false, Metadata.empty()));

        Dataset<Row> withJsonFunctions = customerData
                .withColumn("to_json", to_json(struct(col("Genre"),col("Age"))))
                .withColumn("from_json",from_json(col("to_json"),schema));

                SourceAndSink.sinkConsole(withJsonFunctions);
    }

}
/* to_json
-------------------------------------------
20/06/22 10:47:05 INFO CodeGenerator: Code generated in 42.0862 ms
20/06/22 10:47:05 INFO CodeGenerator: Code generated in 66.5145 ms
+----------+------+---+------------------+----------------------+-----------------------------+------------+
|CustomerID|Genre |Age|Annual Income (k$)|Spending Score (1-100)|to_json                      |from_json   |
+----------+------+---+------------------+----------------------+-----------------------------+------------+
|0001      |Male  |19 |15                |39                    |{"Genre":"Male","Age":"19"}  |[Male, 19]  |
|0002      |Male  |21 |15                |81                    |{"Genre":"Male","Age":"21"}  |[Male, 21]  |
|0003      |Female|20 |16                |6                     |{"Genre":"Female","Age":"20"}|[Female, 20]|
|0004      |Female|23 |16                |77                    |{"Genre":"Female","Age":"23"}|[Female, 23]|
|0005      |Female|31 |17                |40                    |{"Genre":"Female","Age":"31"}|[Female, 31]|
*/


