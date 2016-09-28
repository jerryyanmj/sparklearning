package xml

import scala.xml.XML

/**
 * Created by jiarui.yan on 2015-09-30.
 */
object VelocixFeedParser {

  def main (args: Array[String]): Unit = {


    val data = XML.load("/Users/jiarui.yan/Downloads/velocix_feed.xml")

    val r = for (
      etr <- data \ "entry";
      id = (etr \ "id").text;
      idArr = id.split(":");
      edge = idArr(1);
      t = idArr(2);
      if (t.indexOf("httpaccess") > 0 && t.indexOf("20151001023500") > 0)
    ) yield edge

    val s = r.toSet

    println(s.size)


  }

}
