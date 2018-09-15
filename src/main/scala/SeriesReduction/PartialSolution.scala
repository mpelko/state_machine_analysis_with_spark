package SeriesReduction


/**
  * Created by mpelko on 29/12/16.
  */

@SerialVersionUID(101L)
class PartialSolution[T, R](val parts:List[Part[T, R]] = List[Part[T, R]]()) extends Serializable {
}

@SerialVersionUID(100L)
class Partializer[T, R](val creator:Iterator[T] => (List[R], List[T], List[T])) extends Serializable {

  def combine(first: PartialSolution[T, R], other: PartialSolution[T, R]): PartialSolution[T, R] = {
    // Here we can probably assume all the parts of the first have either a bigger start index or a lower start index
    // and that all the parts are sorted internally, so we can optimize the sortBy?
    val parts: List[Part[T, R]] = (first.parts ::: other.parts).sortBy(_.start_index)
    if (parts.isEmpty) {
      return first
    }
    var current_part = parts.head
    var merged_parts = List[Part[T, R]]()
    for (part <- parts.drop(1)) {
      if (part.start_index == current_part.end_index) {
        current_part = current_part.merge(part, this.creator)
      }
      else {
        merged_parts = current_part :: merged_parts
        current_part = part
      }
    }
    merged_parts = current_part :: merged_parts
    new PartialSolution(merged_parts)
  }

  def from_indexed_partition(idx: Int, partition: Iterator[T]): Iterator[PartialSolution[T, R]] = {

    val (results, start_unhandled, end_unhandled) = this.creator(partition)
    val fully_unhandled = start_unhandled == partition.toList
    val first_part = new Part(results, idx, idx + 1, start_unhandled, end_unhandled, fully_unhandled)
    val ps = new PartialSolution(List(first_part))
    List(ps).toIterator
  }
}