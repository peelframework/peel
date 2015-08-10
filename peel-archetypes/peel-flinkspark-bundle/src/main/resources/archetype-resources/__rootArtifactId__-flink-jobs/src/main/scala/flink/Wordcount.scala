#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.flink

import org.apache.flink.api.scala._

/** A `WordCount` workload job for Flink. */
object Wordcount {

  def main(args: Array[String]) {
    if (args.length != 2) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.readTextFile(inputPath)
      .flatMap(_.toLowerCase.split("${symbol_escape}${symbol_escape}W+"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .writeAsCsv(outputPath)

    env.execute("${parentArtifactId}-flink")
  }

}
