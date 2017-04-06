package org.familysearch.spark.scala

import org.apache.spark.SparkContext
import org.familysearch.spark.scala.util.SparkUtil

/**
  * Class created by dalehulse on 3/27/17.
  */
object BibleWordCount2 {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.createSparkContext(getClass.getName)
    val input = SparkUtil.getInputDir("bible-lines")
    val stopWordsIn = SparkUtil.getInputDir("stop-words")
    val output = SparkUtil.prepareOutputDir("bible-word-count-from-lines")

    println("Reading base input from " + input)
    println("Reading stop words input from " + stopWordsIn)
    println("Writing result to " + output)
    run(sc, input, stopWordsIn, output)
    sc.stop()
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
    *   The input for this Spark application has many words on the same line. This means when textFile(input) is called,
    *   Spark will create an RDD[String] where each element is String that contains many words.
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
    *   each element is one word. To do this, you will need to use flatMap()
    *     See http://spark.apache.org/docs/latest/programming-guide.html#transformations
    *
    *   Note: There are many characters in the input to clean up e.g. verse numbers and punctuation. For Task 4 just focus on the flatMap(),
    *   Task 5 will focus on cleaning the input further.
    *
    * Task 5: Use more filter() and map() functions to clean the input
    *   Take the RDD[String] returned from the flatMap and do some cleaning. Notice the verse numbers in the text e.g. "1:1".
    *   Any element that matches this verse number pattern should be removed from the RDD[String]. Use a filter() function
    *   to remove all elements matching this verse number pattern.
    *
    *   The next step is to do some further cleaning. There are some words in the dataset with punctuation characters attached to the word
    *   e.g. "earth.". You need to strip all of these punctuation characters from the words in the RDD[String]. This can be done doing a map()
    *   function.
    *
    *   The next step is to transform all of your word elements to either all uppercase characters or all lowercase characters. The reason for
    *   this is to get a more accurate word count since cases change throughout the text e.g. "Let", and "let". Also after cleaning all
    *   of the words, you may end up with empty String elements, make sure to remove empty String elements with a filter() function.
    *
    *   Finally, when you have a RDD[String] where each element is a word and has been prepared for a word count analysis, use your solution
    *   from BibleWordCount to complete this Spark Application.
    *
    *   The output format should be the same format as BibleWordCount
    *
    * @param sc configured SparkContext to run locally
    * @param input bible lines input directory
    * @param stopWordsIn stop words input directory
    * @param output result output directory
    */
  def run(sc: SparkContext, input: String, stopWordsIn: String, output: String): Unit = {
    // write code here
  }
}
