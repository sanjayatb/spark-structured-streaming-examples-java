package com.stb.functions;

import com.stb.transformations.SourceAndSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.HashMap;

import static org.apache.spark.sql.functions.*;

public class CsvFunctions {

    public static void main(String[] args) {
        Dataset<Row> customerData = SourceAndSink.readStreamCsv("Json Functions", "csv/transformations");

        StructType schema = new StructType()
                .add(new StructField("Genre", DataTypes.StringType,false, Metadata.empty()))
                .add(new StructField("Age", DataTypes.StringType,false, Metadata.empty()));

        scala.collection.immutable.Map<String,String> mapOptions = new HashMap();

        Dataset<Row> withCsvFunctions = customerData
                .withColumn("to_json", to_csv(struct(col("Genre"),col("Age"))))
                .withColumn("from_json",from_csv(col("to_json"),schema,mapOptions));

        SourceAndSink.sinkConsole(withCsvFunctions);
    }

}
/* Output
-------------------------------------------
20/06/25 12:11:17 INFO CodeGenerator: Code generated in 20.4061 ms
20/06/25 12:11:17 INFO CodeGenerator: Code generated in 28.7014 ms
+----------+------+---+------------------+----------------------+---------+------------+
|CustomerID|Genre |Age|Annual Income (k$)|Spending Score (1-100)|to_json  |from_json   |
+----------+------+---+------------------+----------------------+---------+------------+
|0001      |Male  |19 |15                |39                    |Male,19  |[Male, 19]  |
|0002      |Male  |21 |15                |81                    |Male,21  |[Male, 21]  |
|0003      |Female|20 |16                |6                     |Female,20|[Female, 20]|
|0004      |Female|23 |16                |77                    |Female,23|[Female, 23]|
|0005      |Female|31 |17                |40                    |Female,31|[Female, 31]|
|0006      |Female|22 |17                |76                    |Female,22|[Female, 22]|
|0007      |Female|35 |18                |6                     |Female,35|[Female, 35]|
 */