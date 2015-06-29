package eu.stratosphere.peel.core.results

import org.junit.{Ignore, Test}

import scala.util.Random

class SymbolsBenchmark {

  val SEED = 4535232132L
  val N = 1000

  @Ignore
  @Test
  def symbolsBenchmark(): Unit = {
    val random = new Random(SEED)

    val (strings, constructionTime) = time {
      val base = for (i <- 0 to N) yield random.nextString(63)

      Seq() ++
        base.filter(_ => random.nextDouble() < 0.1) ++
        base.filter(_ => random.nextDouble() < 0.2) ++
        base.filter(_ => random.nextDouble() < 0.3) ++
        base.filter(_ => random.nextDouble() < 0.4) ++
        base.filter(_ => random.nextDouble() < 0.5) ++
        base.filter(_ => random.nextDouble() < 0.6) ++
        base.filter(_ => random.nextDouble() < 0.7) ++
        base.filter(_ => random.nextDouble() < 0.8) ++
        base.filter(_ => random.nextDouble() < 0.9) ++
        base
    }
    println(s"Construction time is ${constructionTime / 1e6}ms")

    val (symbols, conversionTime) = time {
      for (s <- strings) yield Symbol(s)
    }
    println(s"Conversion time is ${conversionTime / 1e6}ms")

    println("---")

    val (_, strCompTime) = time {
      for (x <- strings; y <- strings; if x == y) yield 1
    }
    println(s"String comparison time is ${strCompTime / 1e6}ms")

    val (_, symCompTime) = time {
      for (x <- symbols; y <- symbols; if x == y) yield 1
    }
    println(s"Symbol comparison time is ${symCompTime / 1e6}ms")

    println("---")

    val (strHT, strBuildHashTableTime) = time {
      var ht = scala.collection.mutable.HashMap[String, List[(String, Int)]]()
      for (x <- strings) ht += x -> ((x, 1) :: ht.getOrElse(x, List.empty[(String, Int)]))
      ht
    }
    println(s"String hash table construction time is ${strBuildHashTableTime / 1e6}ms")

    val (symHT, symBuildHashTableTime) = time {
      var ht = scala.collection.mutable.HashMap[Symbol, List[(Symbol, Int)]]()
      for (x <- symbols) ht += x -> ((x, 1) :: ht.getOrElse(x, List.empty[(Symbol, Int)]))
      ht
    }
    println(s"Symbol hash table construction time is ${symBuildHashTableTime / 1e6}ms")

    println("---")

    val (_, strHTQueryTime) = time {
      for (x <- strings) strHT.get(x).size
    }
    println(s"String hash table lookup time is ${strHTQueryTime / 1e6}ms")

    val (_, symHTQueryTime) = time {
      for (x <- symbols) symHT.get(x).size
    }
    println(s"Symbol hash table lookup time is ${symHTQueryTime / 1e6}ms")
  }

  def time[A](f: => A) = {
    val s = System.nanoTime
    val r = f
    (r, System.nanoTime - s)
  }
}
