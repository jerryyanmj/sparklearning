package model

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Created by jiarui.yan on 6/29/15.
 */
trait GeminiParserHelper {
  def parseRawValue(s: String) = Option(s).filter(x => !x.equals("-") && !x.equals("\"-\""))

  def parseIntRawValue(f: String => Int)(s: String) = {
    parseRawValue(s) match {
      case Some(x) => Some(f(x))
      case _ => None
    }
  }

  def parseLongRawValue(f: String => Long)(s: String) = {
    parseRawValue(s) match {
      case Some(x) => Some(f(x))
      case _ => None
    }
  }

  def parseDatePartRawValue(f: (String, String) => String)(s: String, part: String) = {
    parseRawValue(s) match {
      case Some(x) => Some(f(x, part))
      case _ => None
    }
  }

  def dateStrExtractor(dateStr: String) = dateStr.substring(1, dateStr.length-1)

  def datePartUtil(outFormat:String, dateStr: String) = {
    val intFmt = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")
    val inDateTime = DateTime.parse(dateStr, intFmt)
    DateTimeFormat.forPattern(outFormat).withZoneUTC().print(inDateTime)
  }

  def isTest(s: String) = s.toLowerCase.contains("hls-lin-test")

  def isGenres(s: String) = s.toLowerCase.contains("genres")

  def isCrossDomain(s: String) = s.toLowerCase.contains("crossdomain.xml")

  def isClientAccessPolicy(s: String) = s.toLowerCase.contains("clientaccesspolicy.xml")

  def isBadReq(s: String) = s.toLowerCase.contains("BAD_REQUEST")

  def isVideoChunk(s: String) = s.toLowerCase.contains(".ts") ||
    s.toLowerCase.contains("Fragments(video".toLowerCase) ||
    s.toLowerCase.contains("FragmentInfo(video".toLowerCase)

  def isAudioChunk(s: String) = s.toLowerCase.contains("Fragments(audio".toLowerCase) ||
    s.toLowerCase.contains("FragmentInfo(audio".toLowerCase)

  def isEngAudioChunk(s: String) = s.toLowerCase.contains("Fragments(301_eng".toLowerCase) ||
    s.toLowerCase.contains("FragmentInfo(301_eng".toLowerCase) ||
    s.toLowerCase.contains("Fragments(302_eng".toLowerCase) ||
    s.toLowerCase.contains("FragmentInfo(302_eng".toLowerCase)

  def isTextChunk(s: String) = s.toLowerCase.contains("Fragments(textstream".toLowerCase) ||
    s.toLowerCase.contains("FragmentInfo(textstream".toLowerCase)

  def isManifestChunk(s: String) = s.toLowerCase.contains(".isml".toLowerCase) ||
    s.toLowerCase.contains("isml/Manifest".toLowerCase) ||
    s.toLowerCase.contains("ism/Manifest".toLowerCase)

  def isManifest(s: String) = s.toLowerCase.contains(".m3u8")

  def isKey(s: String) = s.toLowerCase.contains(".key")

}
