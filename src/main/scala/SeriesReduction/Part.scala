package SeriesReduction

/**
  * Created by mpelko on 29/12/16.
  *
  * The class used for the
  */


@SerialVersionUID(102L)
class Part[T, R](val results:List[R], val start_index: Int, val end_index:Int,
                 val start_unhandled:List[T], val end_unhandled:List[T],
                 val fully_unhandled:Boolean) extends Serializable {

  /* A reduction method to merge the exisitng part with another part, producing a new part containing reduced info
   * from both parts. The method assumes that the part in the argument is either a next or the previous part within
   * the series.
   */
  def merge(other:Part[T, R], creator:Iterator[T] => (List[R], List[T], List[T])): Part[T, R] = {
    val (first, second) = if (this.start_index < other.start_index) (this, other) else (other, this)
    val (new_results, start_unhandled, end_unhandled) = creator((first.end_unhandled++second.start_unhandled).toIterator)
    val new_start_unhandled = if(first.fully_unhandled) start_unhandled else first.start_unhandled
    val new_end_unhandled = if(second.fully_unhandled) end_unhandled  else second.end_unhandled
    // This could probably be handled better
    val new_fully_unhandled = (new_start_unhandled == (first.end_unhandled++second.start_unhandled) ) &
                              first.fully_unhandled &
                              second.fully_unhandled
    val merged_results = first.results++new_results++second.results
    new Part[T, R](merged_results,
                   first.start_index,
                   second.end_index,
                   new_start_unhandled,
                   new_end_unhandled,
                   new_fully_unhandled)
  }
}
