/* SimpleApp.scala */

package SeriesReduction {

  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.rdd.RDD

  object SeriesReduction {

    def state_change_creator(iter: Iterator[(Float, Int)]):
    (List[(Float, Int)], List[(Float, Int)], List[(Float, Int)]) = {
      var (results, left, right) = (List[(Float, Int)](), List[(Float, Int)](), List[(Float, Int)]())
      if (iter.isEmpty) return (results, left, right)
      val first = iter.next()
      left = first :: left
      var status = first._2
      var cur = first
      while (iter.hasNext) {
        cur = iter.next
        if (status != cur._2) {
          status = cur._2
          results = cur :: results
        }
      }
      right = cur :: right
      (results, left, right)
    }

    def reduceSerial[T, R] (data: RDD[T], stateChangeCreator: Iterator[T] => (List[R], List[T], List[T])): List[R] = {

      val ps = new Partializer[T, R](stateChangeCreator)
      val result = data.mapPartitionsWithIndex(ps.from_indexed_partition).reduce(ps.combine)
      result.parts.head.results
    }

    def main (args: Array[String] ) {
      val logFile = args (0) // Should be some file on your system
      val conf = new SparkConf ().setAppName ("State Machine Inspection")
      val sc = new SparkContext (conf)
      val logData = sc.textFile (logFile, 4)
      //println("Timestamps with state 0: %s, Timestamps with state 1 : %s".format(num0s, num1s))
      val prepared_rdd = logData.map (line => line.split (" ") )
                                .map (tup => (tup (0).dropRight (1).toFloat, tup (1).toInt) )

      val result = reduceSerial(prepared_rdd, state_change_creator)

      println ("\nLast 20 transitions:")
      //println(result.parts.head.results.sortBy(_._1).takeRight(20).mkString("\n"))
      println (result.sortBy (_._1).mkString ("\n") )
      println ("\nNumber of transitions found:")
      println (result.length)
    }
  }

}