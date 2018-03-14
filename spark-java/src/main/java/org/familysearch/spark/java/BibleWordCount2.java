package org.familysearch.spark.java;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.familysearch.spark.java.util.SparkUtil;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Class created by dalehulse on 3/27/17.
 */
public class BibleWordCount2 {
  public static void main(String[] args) throws IOException {
    final JavaSparkContext sc = SparkUtil.createSparkContext(BibleWordCount.class.getName());
    final String input = SparkUtil.getInputDir("bible-lines");
    final String stopWordsIn = SparkUtil.getInputDir("stop-words");
    final String output = SparkUtil.prepareOutputDir("bible-word-count-from-lines");

    System.out.println("Reading base input from " + input);
    System.out.println("Reading stop words input from " + stopWordsIn);
    System.out.println("Writing result to " + output);
    run(sc, input, stopWordsIn, output);
    sc.stop();
  }

  /**
   * Run the main() method from your IDE when you want to run your code
   *
   * Task 4: Learn flatMap()
   *   Similar to BibleWordCount, implement a Spark application that reads in all of words from the bible
   *   and counts how many times that word occurs. The difference between BibleWordCount2 and BibleWordCount is the
   *   input format.
   *
   *   In BibleWordCount, the Spark application reads a dataset where each line in the file has only one word.
   *   The input for this Spark application has many words on the same line. Since textFile() parses its input by reading one line at a time,
   *   Spark will create an RDD[String] where each element is a String that contains many words.
   *
   *   Below is what the input dataset looks like:
   *     The Old Testament of the King James Version of the Bible
   *     The First Book of Moses:  Called Genesis
   *     1:1 In the beginning God created the heavens and the earth.
   *     1:2 And the earth was without form, and void; and darkness was upon
   *     the face of the deep. And the Spirit of God moved upon the face of the
   *     waters.
   *
   *   The first step for this Spark Application will be to read in the input, then generate a RDD[String] where
   *   each element is one word. To do this, you will need to use flatMap(). flatMap() reads in one element and can emit 0 to many
   *   elements, where map() or mapToPair() only reads in one element and emits one element.
   *     See http://spark.apache.org/docs/latest/programming-guide.html#transformations
   *
   *   Note: There are many characters in the input to clean up e.g. verse numbers and punctuation. For Task 4 just focus on the flatMap(),
   *   Task 5 will focus on cleaning the input further.
   *
   * Task 5: Use more filter() and map() functions to clean the input
   *   Take the RDD[String] returned from the flatMap and do some cleaning. Notice the verse numbers in the text e.g. "1:1".
   *   Any element that matches this verse number pattern should be removed from the RDD[String]. Use a filter() function
   *   to remove all elements matching this verse number pattern. (You can could actually do the filtering logic inside of the flatMap,
   *   but we are going to use filter() instead just to practice)
   *
   *   The next step is to do some further cleaning. There are some words in the dataset with punctuation characters attached to the word
   *   e.g. "earth.". Remove punctuation characters from the word that make sense to you. You can decide how you want to handle all cases like:
   *   Should "it's" be "it's" or "its". It would be a good idea however to at least remove '.' and ',' at the end of a word.
   *   Removing punctuation can be done using a map() function.
   *
   *   The next step is to transform all of your word elements to either all uppercase characters or all lowercase characters. The reason for
   *   this is to get a more accurate word count since cases change throughout the text e.g. "Let", and "let". Also after cleaning all
   *   of the words, you may end up with empty String elements, make sure to remove empty String elements with a filter() function.
   *
   *   Finally, when you have a RDD[String] where each element is a word and has been prepared for a word count analysis, complete the WordCount
   *   Spark app (you may use your code from the BibleWordCount Spark app).
   *
   *   The output format should be the same format as BibleWordCount, however the content does not need to match 100% since you may have chosen a different
   *   way of handling punctuation characters in the middle of a string e.g. it's
   *
   * @param sc configured SparkContext to run locally
   * @param input bible lines input directory
   * @param stopWordsIn stop words input directory
   * @param output result output directory
   */
  static void run(final JavaSparkContext sc, final String input, final String stopWordsIn, final String output) {

    JavaRDD<String> lines = sc.textFile(input);
    JavaRDD<String> inputWords = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

    JavaRDD<String> filterWords = inputWords.filter(x -> x.matches("[a-zA-Z]+"));  // filter out all the words do not contain a letter, like '1:76'

    JavaRDD<String> removePunctuation = filterWords.map(x -> x.replaceAll(".,?!\'", ""));  //remove punctuations

    JavaRDD<String> upperCaseWords = removePunctuation.map(x -> x.toUpperCase());  //change all words to upper case to count they more accurate

    JavaRDD<String> rdd = sc.textFile(stopWordsIn);
    JavaRDD<String> upper = rdd.map(x -> x.toUpperCase());  //change the stop words list to uppercase too
    Set<String> stopWords = new HashSet(upper.collect());
    Broadcast<Set<String>> broadcast = sc.broadcast(stopWords);
    JavaRDD<String> bibleWords = upperCaseWords.filter(word -> { return !broadcast.value().contains(word); });  //filter out all the words in the stopWords List
    JavaPairRDD<String, Integer> pairs = bibleWords.mapToPair(s -> new Tuple2(s, 1));
    JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
    JavaPairRDD<String, Integer> sorted = counts.mapToPair(x -> x.swap()).sortByKey(false, 1).mapToPair(x -> x.swap());  //sort the words count list by the numbers it appears (sort by value)
    sorted.saveAsTextFile(output);

  }
}
