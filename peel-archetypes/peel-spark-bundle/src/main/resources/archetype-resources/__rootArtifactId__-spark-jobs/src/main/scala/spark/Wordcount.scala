#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.spark

import org.apache.spark.{SparkConf, SparkContext}

/** A `WordCount` workload job for Spark. */
object Wordcount {

  def main(args: Array[String]) {
    if (args.length != 2) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = new SparkContext(new SparkConf().setAppName("${parentArtifactId}-spark"))
    spark.textFile(inputPath)
      .flatMap(_.toLowerCase.split("${symbol_escape}${symbol_escape}W+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(outputPath)
  }

}
