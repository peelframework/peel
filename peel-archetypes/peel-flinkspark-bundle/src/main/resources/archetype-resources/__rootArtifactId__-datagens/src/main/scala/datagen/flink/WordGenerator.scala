#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.datagen.flink

import ${package}.datagen.util.RanHash
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.NumberSequenceIterator

import scala.io.{Codec, Source}

/** A simple word generator that samples words uniformly from a dictionary */
object WordGenerator {

  type DistributionSampler = (RanHash, Int) => Int

  val SEED = 1010

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      Console.err.println("Usage: <jar> dictionarySize numberOfTasks tuplesPerTask outputPath")
      System.exit(-1)
    }

    val dictionarySize = args(0).toInt
    val numberOfTasks = args(1).toInt
    val tuplesPerTask = args(2).toInt
    val outputPath = args(3)
    val numberOfWords = numberOfTasks * tuplesPerTask

    // load dictionary from the resources folder and partially apply the sampleWord function to it
    val dictionary = {
      val src = Source.fromInputStream(getClass.getResourceAsStream("/dictionary"), Codec.UTF8.toString())
      val res = src.getLines().toArray
      if (dictionarySize > 0 && dictionarySize < res.length) res.slice(0, dictionarySize) else res
    }

    // define sampling function that returns uniformly distributed integers in the [0, dictionary.length) range
    def sampleUniform(ran: RanHash): Int = {
      Math.floor(ran.next() * dictionary.length + 0.5).toInt % dictionary.length
    }

    val environment = ExecutionEnvironment.getExecutionEnvironment

    environment
      // create a sequence [1 .. N] to create N words
      .fromParallelCollection(new NumberSequenceIterator(1, numberOfWords))
      // set up workers
      .setParallelism(numberOfTasks)
      // map every n <- [1 .. N] to a random word sampled from a word list
      .map(i => dictionary(sampleUniform(new RanHash(SEED + i))))
      // write result to file
      .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE)

    environment.execute(s"WordGenerator[${symbol_dollar}numberOfWords]")
  }
}
