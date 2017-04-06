package org.familysearch.spark.java;

import org.apache.spark.api.java.JavaSparkContext;
import org.familysearch.spark.java.util.SparkUtil;

import java.io.IOException;

/**
 * Class created by dalehulse on 4/5/17.
 */
public class BibleWordCountFromCommandLine {
  /**
   * Class created by dalehulse on 4/5/17.
   *
   * Run the main() method using spark-submit
   *
   * Task 6: Learn how to run a Spark application from the command line using spark-submit
   *
   *   In BibleWordCount and BibleWordCount2, the SparkContext was configured with a local master by invoking setMaster("local[*]").
   *   For this Spark app the master has not been configured. The reason for this is to make the Spark application more flexible for
   *   the environment it is going to run in. The master can be decided when running spark-submit from the command line with the --master option.
   *
   *   This spark application will use the code you implemented in BibleWordCount2.run(), so make sure you have completed BibleWordCount2
   *   before doing this task.
   *
   *   Note that this application expects one program argument: the output directory for saving the result. You can pass application arguments
   *   using spark-submit by specifying them after the jar file.
   *     spark-submit [options....] app.jar /path/to/output/
   *
   *   For this task, build the jar for this project and use that jar with spark-submit to run the Spark application locally.
   *     cd $PROJECT_DIR
   *     mvn clean install
   *     $SPARK_HOME/bin/spark-submit [options....] spark-java/target/spark-java.jar /path/to/output/
   *
   *   See http://spark.apache.org/docs/latest/submitting-applications.html
   *       http://spark.apache.org/docs/latest/submitting-applications.html#master-urls
   *
   *   You will need to download the binaries for spark to run the spark-submit command
   *     1) Go to http://spark.apache.org/downloads.html
   *     2) Choose the version of spark that matches the spark version of this repo (see parent pom.xml spark.version)
   *     3) Choose the Pre-Built version for the most current version of Hadoop
   *     4) Click the download link
   *     5) Extract the binaries from the tarball
   *       tar -xzvf spark-2.1.0-bin-hadoop2.7.tgz
   *
   *   It is useful to become familiar with the spark-submit script because it is used run Spark applications on a cluster.
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Application requires one argument for the output directory");
      System.exit(1);
    }

    final boolean setMaster = false;
    final JavaSparkContext sc = SparkUtil.createSparkContext(BibleWordCountFromCommandLine.class.getName(), setMaster);
    final String input = SparkUtil.getInputDir("bible-lines");
    final String stopWordsIn = SparkUtil.getInputDir("stop-words");
    final String output = args[0];

    System.out.println("Reading base input from " + input);
    System.out.println("Reading stop words input from " + stopWordsIn);
    System.out.println("Writing result to " + output);
    BibleWordCount2.run(sc, input, stopWordsIn, output);
    sc.stop();
  }
}
