package org.familysearch.spark.java;

import org.familysearch.spark.java.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.familysearch.spark.java.SerializableComparator.serialize;

/**
 * Run the main() method from your IDE when you want to run your code
 *
 * Task 1 : Learn textFile(), mapToPair(), reduceByKey(), and saveAsTextFile()
 *   Use textFile(), mapToPair(), and reduceByKey() to implement a Spark application that reads in all of words from the bible
 *   and counts how many times that word occurs. You will also need to understand what an RDD is.
 *   Use the below references to learn about RDD, textFile(), mapToPair(), and reduceByKey()
 *       RDD: http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds
 *            https://www.tutorialspoint.com/apache_spark/apache_spark_rdd.htm
 *       textFile(): http://spark.apache.org/docs/latest/programming-guide.html#external-datasets
 *       mapToPair(): http://spark.apache.org/docs/latest/programming-guide.html#working-with-key-value-pairs
 *       reduceByKey(): http://spark.apache.org/docs/latest/programming-guide.html#transformations
 *       saveAsTextFile(): http://spark.apache.org/docs/latest/programming-guide.html#actions
 *
 *   Below is what the input dataset looks like:
 *     in
 *     the
 *     beginning
 *     god
 *     created
 *     the
 *     heavens
 *
 *   When using the textFile(input) method from the SparkContext, spark will read in all of the files from the "input" directory.
 *   Spark will create an RDD[String] where each element of the RDD is one line of the file. In other words, for reading this dataset Spark
 *   will create a RDD where each element is one word.
 *
 *   Use the saveAsTextFile(output) method on the RDD to save your result. The output should be in this format: <word>\t<count>.
 *   Note that you will have to use a map() function in the end of the Spark application  to transform the elements in your RDD
 *   to match this format <word>\t<count>.
 *
 *   Below is an example of what the output format should look like:
 *     shall	9838
 *     unto	8997
 *     lord	7830
 *     thou	5474
 *     thy	4600
 *     god	4442
 *
 * Task 2: Learn sortByKey()
 *
 *   Improve your Spark Application by sorting your dataset where the most common word appears on the top. To do this
 *   look into using Spark's PairRDD method sortByKey(). Note that sortByKey() is only available to PairRDDs
 *
 *   See http://spark.apache.org/docs/latest/programming-guide.html#transformations
 *
 *   Tip: Scala's Tuple2 has a swap() function which returns a Tuple2 where the first and second values in the tuple are swapped
 *     ("shall", 9838).swap equals (9838, "shall")
 *
 *
 * Task 3: Learn filter() and Broadcast variables
 *   Use filter() and Broadcast variables to remove common stop words from the result
 *
 *   Notice how words that occur the most are the common stop words such as "the", "and", "of", etc. For this next task
 *   improve your Spark Application further by removing these common stop words before doing a word count. A very basic stop words
 *   dataset has been provided, and its directory location has been passed in as a parameter to this method. This stop words dataset
 *   is basic, and you are welcome to add other words if you wish e.g. "thee", "thou", etc.
 *
 *   Read all the stop words into a set, then use that set to create a Broadcast variable. Using a Broadcast variable allows spark to efficiently
 *   transfer large data to each executor in the cluster. It is much better to send large data to worker nodes via Broadcast variable, then it is
 *   to send the data without a Broadcast variable.
 *     See http://spark.apache.org/docs/latest/programming-guide.html#broadcast-variables
 *       (using broadcast variables will not make any difference locally, but can make a huge difference when you run on a cluster with many nodes).
 *
 *   Once the Broadcast variable is setup just reference it in the anonymous function passed to filter().
 *     JavaRDD<String> rdd = // capture all the bible words in a rdd
 *     Set<String> set = // retrieve the stop words
 *     Broadcast<Set<String>> broadcast = sc.broadcast(set)
 *     rdd.filter(word -> {
 *       broadcast.value() // use broadcast variable here
 *     })
 *   In this example above, spark will know to send the broadcast variable to the workers that run the filter function, since we referenced the
 *   broadcast variable in the anonymous function.
 *
 *   Tip: An easy way to read in the stop words would be to use textFile(stopWordsIn).collect(). However you can choose to read in the
 *   stop words using Java libraries if you wish.
 */
public class BibleWordCount {

  private static final Pattern SPACE = Pattern.compile(" ");

  public static String removePunctuation(String s) {
    return s.replaceAll("[^a-zA-Z\\d\\s]", "").trim().toLowerCase();
  }

  public static <T> Set<T> convertListToSet(List<T> list)
  {
    // create a set from the List
    return list.stream().collect(Collectors.toSet());
  }

  public static void main(String[] args) throws IOException {
    final JavaSparkContext sc = SparkUtil.createSparkContext(BibleWordCount.class.getName());
    final String input = SparkUtil.getInputDir("bible-words");
    final String stopWordsIn = SparkUtil.getInputDir("stop-words");
    final String output = SparkUtil.prepareOutputDir("bible-word-count");

    System.out.println("Reading base input from " + input);
    System.out.println("Reading stop words input from " + stopWordsIn);
    System.out.println("Writing result to " + output);

    List<String> stopWords0 = Files.readAllLines(Paths.get(stopWordsIn+"/stop-words-1.txt"));
    Set<String> stopWords = convertListToSet(stopWords0);
    JavaRDD<String> lines = sc.textFile(input);

    Function<String, Boolean> isNotEmpty = s -> s.trim().length() > 0;

    // Broadcast variables allow the programmer to keep a read-only variable
    // cached on each machine rather than shipping a copy of it with tasks.
    Broadcast<List<String>> stopWordsBroadcast = sc.broadcast(stopWords0);

    JavaRDD<String> words = lines.filter(isNotEmpty)
            .map(BibleWordCount::removePunctuation)
            .filter(isNotEmpty)
            .flatMap(s -> Arrays.asList(SPACE.split(s)).iterator()).filter(isNotEmpty)
            .filter(token -> !stopWordsBroadcast.value().contains(token));

    JavaPairRDD<String, Integer> counts = words.mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey((x, y) -> x + y);

    List<Tuple2<String, Integer>> result =
            counts.takeOrdered(10, serialize((wordCountTuple1, wordCountTuple2) -> -Integer.compare(wordCountTuple1._2(), wordCountTuple2._2())));
    System.out.println("");
    for (Tuple2<?, ?> tuple : result) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    counts.saveAsTextFile(output);
    sc.stop();
  }
}
