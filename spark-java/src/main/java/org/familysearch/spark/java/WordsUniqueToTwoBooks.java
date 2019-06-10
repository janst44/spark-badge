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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

/**
 * Class created by dalehulse on 4/5/17.
 */
public class WordsUniqueToTwoBooks {
  public static void main(String[] args) throws IOException {
    final JavaSparkContext sc = SparkUtil.createSparkContext(WordsUniqueToTwoBooks.class.getName());
    final String input = SparkUtil.getInputDir("bible-books-text");
    final String output = SparkUtil.prepareOutputDir("words-unique-to-two-books");

    System.out.println("Reading base input from " + input);
    System.out.println("Writing result to " + output);
    run(sc, input, output);
    sc.stop();
  }

  /**
   * Run the main() method from your IDE when you want to run your code
   *
   * Task 7: Learn distinct() and groupByKey()
   *
   *   For this task use distinct() and groupByKey() to find out which words appear in only two books of the Bible.
   *
   *   Here is the input format: <word>\t<book>\t<old-testament>
   *
   *   Input Example:
   *    hangings	esther	old-testament
   *    fastened	esther	old-testament
   *    cords	esther	old-testament
   *    fine	esther	old-testament
   *    linen	esther	old-testament
   *
   *   distinct() is a method that will remove all duplicate elements from your RDD. It uses the equals() method of the object type.
   *     See http://spark.apache.org/docs/latest/programming-guide.html#transformations
   *     Example1:
   *       Before distinct():
   *         "hi"
   *         "there"
   *         "hi"
   *         "again"
   *       After distinct():
   *         "hi"
   *         "there"
   *         "again"
   *
   *     Example2:
   *       Before distinct():
   *         ("the", "genesis")
   *         ("the", "exodus")
   *         ("the, "genesis")
   *       After distinct():
   *         ("the", "genesis")
   *         ("the", "exodus")
   *
   *   groupByKey() is a function in Spark the behaves very similar to Hadoop's MapReduce.
   *     See http://spark.apache.org/docs/latest/programming-guide.html#transformations
   *
   *   groupByKey() is invoked on a PairRDD and it will run a shuffle to generate a PairRDD[K, Iterable[V]]. This shuffle will gather
   *   all of the values for a certain key into one Tuple2. You can then apply a map() function on this Tuple2 to behave like a reduce
   *   function.
   *
   *   Word Count Example:
   *     sc.textFile(input)
   *       .mapToPair(word -> new Tuple2<>(word, 1L))
   *       .groupByKey()
   *       .map(tuple -> {
   *         long cnt = 0L;
   *         for (long value : tuple._2()) {
   *           cnt += value;
   *         }
   *         return new Tuple2<>(tuple._1(), cnt);
   *       })
   *       .saveAsTextFile(output);
   *
   *   Spark's reduceByKey() function is generally a more efficient solution for a reduce function. However, sometimes an application
   *   requires the logic of groupByKey() or in other cases it is better or more efficient to use groupByKey(). This Spark application
   *   can be implemented using reduceByKey(), but we are going to use groupByKey() to get familiar with it.
   *     See http://spark.apache.org/docs/latest/programming-guide.html#shuffle-operations
   *
   * @param sc configured SparkContext to run locally
   * @param input bible lines input directory
   * @param output result output directory
   */
  private static void run(final JavaSparkContext sc, final String input, final String output) {
    // todo write code here
    final SparkSession spark = new SparkSession(sc.sc());

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
    result.show();
    // Creates a temporary view using the DataFrame
    result.createOrReplaceTempView("bible");

    //Dataset<Row> df = spark.sql("select book, count(*) as word_cnt " + "from bible " + "group by book");
    Dataset<Row> uniqueWords = result.select("word", "book").distinct();
    uniqueWords.show();
    Dataset<Row> df = uniqueWords.groupBy(col("word")).agg(count("book").as("book_cnt"));
    df.show();
    Dataset<Row> uniqueToTwoBooks = df.select("word", "book_cnt").filter("book_cnt = 1");
    uniqueToTwoBooks.show();
  }
}
