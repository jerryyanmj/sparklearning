package test

/**
 * Created by jiarui.yan on 2015-11-26.
 */
class Test {

  def main(args: Array[String]) {
    val x = for {
      a<- List.range(1,13)
      b<- List.range(1,13)
      if (a + b < 50)
      if (a + b > 26)
    } yield (a,b)

    val y = for {
      a <- List.range(1, 13)
      b <- List.range(2, 13)
      c <- List.range(3, 13)//.dropWhile( x => x == a).dropWhile( x => x == b)
      d <- List.range(4, 13)//.dropWhile( x => x == a).dropWhile( x => x == b).dropWhile( x => x == c)
      e <- List.range(5, 13)//.dropWhile( x => x == a).dropWhile( x => x == b).dropWhile( x => x == c).dropWhile( x => x == d)
      f <- List.range(6, 13)//.dropWhile( x => x == a).dropWhile( x => x == b).dropWhile( x => x == c).dropWhile( x => x == d).dropWhile( x => x == e)
      g <- List.range(7, 13)//.dropWhile( x => x == a).dropWhile( x => x == b).dropWhile( x => x == c).dropWhile( x => x == d).dropWhile( x => x == e).dropWhile( x => x == f)
      h <- List.range(8, 13)//.dropWhile( x => x == a).dropWhile( x => x == b).dropWhile( x => x == c).dropWhile( x => x == d).dropWhile( x => x == e).dropWhile( x => x == f).dropWhile( x => x == g)
      i <- List.range(9, 13)//.dropWhile( x => x == a).dropWhile( x => x == b).dropWhile( x => x == c).dropWhile( x => x == d).dropWhile( x => x == e).dropWhile( x => x == f).dropWhile( x => x == g).dropWhile( x => x == h)
      //j <- List.range(1, 13)//.dropWhile( x => x == a).dropWhile( x => x == b).dropWhile( x => x == c).dropWhile( x => x == d).dropWhile( x => x == e).dropWhile( x => x == f).dropWhile( x => x == g).dropWhile( x => x == h).dropWhile( x => x == i)
      //k <- List.range(1, 13)//.dropWhile( x => x == a).dropWhile( x => x == b).dropWhile( x => x == c).dropWhile( x => x == d).dropWhile( x => x == e).dropWhile( x => x == f).dropWhile( x => x == g).dropWhile( x => x == h).dropWhile( x => x == i).dropWhile( x => x == j)
      //l <- List.range(1, 13)//.dropWhile( x => x == a).dropWhile( x => x == b).dropWhile( x => x == c).dropWhile( x => x == d).dropWhile( x => x == e).dropWhile( x => x == f).dropWhile( x => x == g).dropWhile( x => x == h).dropWhile( x => x == i).dropWhile( x => x == j).dropWhile( x => x == k)
      s1 = a + b + c + d
      s2 = c + e + f + g
//      s3 = d + g + h + i
//      s4 = a + k + j + i
//      s5 = l + k + b + e
//      s6 = h + f + l + j
//      if s1 == s2 == s3 == s4 == s5 == s6
      if (s1 == s2)
    } yield (a,b,c,d,e,f,g,h,i)//,j,k,l)
  }

}
