package org.familysearch.spark.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.familysearch.spark.java.util.SparkUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class created by dalehulse on 4/5/17.
 */
public class CreateBibleDataFrameFromRawText {
  public static void main(String[] args) throws IOException {
    final JavaSparkContext sc = SparkUtil.createSparkContext(CreateBibleDataFrameFromRawText.class.getName());
    final SparkSession spark = new SparkSession(sc.sc());
    final String input = SparkUtil.getInputDir("bible-books-text");
    final String output = SparkUtil.prepareOutputDir("bible-books-parquet");

    System.out.println("Reading parquet files from " + input);
    System.out.println("Writing result to " + output);
    run(sc, spark, input, output);
    spark.stop();
  }

  /**
   * Run the main() method from your IDE when you want to run your code
   *
   * Task 9: Learn how to create SparkSQL DataFrames from raw text and persist it to parquet files.
   *
   *   For this task read in raw text and convert it to a DataFrame
   *
   *   There are several ways to create a DataFrame from raw text.
   *     1) Inferring the schema using reflection: http://spark.apache.org/docs/latest/sql-programming-guide.html#inferring-the-schema-using-reflection
   *     2) Programmatically specifying the schema: http://spark.apache.org/docs/latest/sql-programming-guide.html#programmatically-specifying-the-schema
   *
   *   You may use which ever method you like. The end result needs to have the following fields for each row: "word", "book", and "testament".
   *
   *   The input dataset has the following schema: <word>\t<book>\t<testament>
   *     now	esther	old-testament
   *     came	esther	old-testament
   *     pass	esther	old-testament
   *     days	esther	old-testament
   *     ahasuerus	esther	old-testament
   *     ahasuerus	esther	old-testament
   *     reigned	esther	old-testament
   *     india	esther	old-testament
   *
   *   After creating the DataFrame, persist it to parquet files:
   *     final Dataset<Row> result = // code to generate DataFrame
   *     result.write().parquet(output)
   *
   *   Note: You will need to invoke textFile() from the JavaSparkContext to read in the raw text.
   *
   * @param sc configured SparkContext to run locally
   * @param spark configured spark session
   * @param input bible books input directory
   * @param output result output directory
   */
  private static void run(final JavaSparkContext sc, final SparkSession spark, final String input, final String output) {
    // todo write code here
    // Create an RDD
    JavaRDD<String> bible_words = sc
        .textFile(input, 1);

    // The schema is encoded in a string
    String schemaString = "word book testament";

    // Generate the schema based on the string of schema
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : schemaString.split(" ")) {
      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
      fields.add(field);
    }
    StructType schema = DataTypes.createStructType(fields);

    // Convert records of the RDD (bible-words) to Rows
    JavaRDD<Row> rowRDD = bible_words.map((Function<String, Row>) record -> {
      String[] attributes = record.split("\t");
      return RowFactory.create(attributes[0], attributes[1].trim(), attributes[2].trim());
    });

    // Apply the schema to the RDD
    Dataset<Row> result = spark.createDataFrame(rowRDD, schema);

    // Creates a temporary view using the DataFrame
    result.createOrReplaceTempView("bible");
//     SQL can be run over a temporary view created using DataFrames
//    Dataset<Row> results = spark.sql("SELECT book FROM bible");
//
//    // The results of SQL queries are DataFrames and support all the normal RDD operations
//    // The columns of a row in the result can be accessed by field index or by field name
//    Dataset<String> namesDS = results.map(
//        (MapFunction<Row, String>) row -> "Book: " + row.getString(0),
//        Encoders.STRING());
//    namesDS.show();

    result.write().parquet(output);
  }
}
