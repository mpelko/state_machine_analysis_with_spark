/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object StateMachineInspection {
  def main(args: Array[String]) {
    val logFile = args(0) // Should be some file on your system
    val conf = new SparkConf().setAppName("State Machine Inspection")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 4)
    //println("Timestamps with state 0: %s, Timestamps with state 1 : %s".format(num0s, num1s))
    var partial_result = logData.map(line => line.split(" "))
                                .map(tup => (tup(0).dropRight(1).toFloat, tup(1).toInt))
                                .mapPartitions(map_relevant)
                                .collect()

    partial_result = partial_result.sortBy(_._1)
    println("\nLast 20 transitions:")
    println(clean_up(partial_result).takeRight(20).deep.mkString("\n"))
    println("\nNumber of transitions found:")
    println(clean_up(partial_result).length)
  }

  def map_relevant(iter: Iterator[(Float, Int)]) : Iterator[(Float, Int)] = {
    var res = List[(Float, Int)]()
    val first = iter.next()
    var status = first._2
    res = first :: res
    while (iter.hasNext)
    {
      val cur = iter.next;
      if ((status != cur._2) || !iter.hasNext) {
        status = cur._2
        res = cur :: res
      }
    }
    res.reverseIterator
  }

  def clean_up(input: Array[(Float, Int)]) : Array[(Float, Int)] = {
    var res = List[(Float, Int)]()
    var status = 0
    for (i <- input.indices) {
      if ((i==0) || (status != input(i)._2)) {
        res = input(i) :: res
        status = input(i)._2
      }
    }
    res.reverseIterator.toArray
  }

}