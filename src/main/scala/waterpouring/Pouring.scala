package waterpouring

import scala.util.Random

/**
 * Created by jiarui.yan on 2015-11-27.
 */
class Pouring(capacity: Vector[Int]) {

  type State = Vector[Int]

  val initialState = capacity map (x => 0)

  trait Move {
    def change(state: State): State
  }
  case class Empty(glass: Int) extends Move {
    def change(state: State) = state updated(glass, 0)
  }
  case class Fill(glass: Int) extends Move {
    def change(state: State) = state updated(glass, capacity(glass))
  }
  case class Pour(from: Int, to: Int) extends Move {
    def change(state: State) = {
      val amount = state(from) min (capacity(to) - state(to))
      state updated(from, state(from)-amount) updated (to, state(to) + amount)
    }
  }

  val glasses = 0 until capacity.length

  val moves =
    (for (g <- glasses) yield Empty(g)) ++
      (for (g <- glasses) yield Fill(g)) ++
      (for (from <- glasses; to <- glasses if from != to) yield Pour(from, to))


  class Path(history: List[Move], val endState: State) {
    def extend(move: Move) = new Path(move :: history, move change endState)
    override def toString = (history.reverse mkString " ") + "--> " + endState
  }

  val initialPath = new Path(Nil, initialState)

  def from(paths: Set[Path], explored: Set[State]): Stream[Set[Path]] =
    if (paths.isEmpty) Stream.empty
    else {
      val more = for {
        path <- paths
        next <- moves map path.extend
        if !(explored contains( next.endState))
      } yield next

      paths #:: from(more, explored ++ (more map (_.endState)))
    }

  val pathSets = from(Set(initialPath), Set(initialState))

  def solutions(target: Int): Stream[Path] =
    for {
      pathSet <- pathSets
      path <- pathSet
      if path.endState contains target
    } yield path


  def fromInt(combinations: Set[List[Int]]): Stream[Set[List[Int]]] =
    if (combinations.isEmpty) Stream.empty
    else {
      val more = for {
        current <- combinations
        newInt <- List.range(1, 13)
      } yield newInt :: current

      combinations #:: fromInt(more)
    }



  val intStream = fromInt(Set(Nil))

  def intSolution: Stream[List[Int]] =
    for {
      com <- intStream
      list <- com
      if list.length == 12
      a = list(0)
      b = list(1)
      c = list(2)
      d = list(3)
      e = list(4)
      f = list(5)
      g = list(6)
      h = list(7)
      i = list(8)
      j = list(9)
      k = list(10)
      l = list(11)
      s1 = a+l+e+i
      s2 = l+d+k+h
      s3 = g+k+c+j
      s4 = b+f+i+j
      s5 = a+b+c+d
      s6 = e+f+g+h
      if s1 == s2 == s3 == s4 == s5 == s6
    } yield list


  class Cube(list: List[Int]) {

    val a = list(0)
    val b = list(1)
    val c = list(2)
    val d = list(3)
    val e = list(4)
    val f = list(5)
    val g = list(6)
    val h = list(7)
    val i = list(8)
    val j = list(9)
    val k = list(10)
    val l = list(11)



    def isValid = {
      val s1 = a+l+e+i
      val s2 = l+d+k+h
      val s3 = g+k+c+j
      val s4 = b+f+i+j
      val s5 = a+b+c+d
      val s6 = e+f+g+h

      s1 == s2 && s1 == s3 && s1 == s4 && s1 == s5 && s1 == s6 &&
        s2 == s3 && s2 == s4 && s2 == s5 && s2 == s6 &&
        s3 == s4 && s3 == s5 && s3 == s6 &&
        s4 == s5 && s4 == s6  &&
        s5 == s6 && list.toSet.size == 12

    }

    override def toString = list mkString " "
  }

  def fromCube(): Stream[Cube] = {
    val next = List(Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1, Random.nextInt(12 - 1) + 1)
    val nextCube = new Cube(next)
    nextCube #:: fromCube()
  }

  val cubeStream = fromCube

  def cubeSolution: Stream[Cube] =
    for {
      c <- cubeStream
      if c.isValid
    } yield c

}

