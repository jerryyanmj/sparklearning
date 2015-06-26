package model

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.joda.time.DateTime
import org.joda.time.format._

/**
 * Created by jiarui.yan on 5/31/15.
 */

class GeminiRaw(args: Array[String]) {
  val Array(c_info, dynamic_ip, c_ip, date_string, cs_method, cs_referrer, sc_status, sc_stream_bytes, sc_bytes, cs_uri, cs_user_agent, x_duration, s_cache_status, event_type, unix_time, http_range, cs_cookie, download_speed) = args
}

class GeminiRawDecoder(props: VerifiableProperties = null) extends Decoder[GeminiRaw] {
  val encoding =
    if(props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")

  def fromBytes(bytes: Array[Byte]): GeminiRaw = {
    val logLine = new String(bytes, encoding)
    new GeminiRaw(logLine.split('\t'))
  }
}

class GeminiParsed(rawLog: GeminiRaw) {
  val raw = rawLog

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

  def datePartUtil(outFormat:String, dateStr: String) = {
    val intFmt = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")
    val inDateTime = DateTime.parse(dateStr, intFmt)
    DateTimeFormat.forPattern(outFormat).withZoneUTC().print(inDateTime)
  }

  def clientIp = parseRawValue(raw.c_ip)

  def dateTime = parseRawValue(raw.date_string)

  def eventType = {
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

    def chunkName(s: String) = s match {
      case csm if isAudioChunk(csm) => "AUDIO_CHUNK"
      case csm if isVideoChunk(csm) => "VIDEO_CHUNK"
      case csm if isManifestChunk(csm) => "MANIFEST_CHUNK"
      case csm if isTextChunk(csm) => "TEXT_CHUNK"
    }

    val csMethod = raw.cs_method
    if (csMethod.equals("-") || csMethod.equals("\"-\"")) {
      None
    } else {
      Option(
        raw.cs_method match {
          case csm if (isTest(csm)) => "TEST_REQUEST"
          case csm if (isGenres(csm)) => "GENRE_SERVICE_REQUEST"
          case csm if (isCrossDomain(csm)) => "CROSS_DOMAIN_XML"
          case csm if (isClientAccessPolicy(csm)) => "CLIENT_ACCESS_POLICY"
          case csm if (isBadReq(csm)) => "BAD_REQUEST"
          case csm if (isEngAudioChunk(csm)) => (chunkType, streamType) match {
            case (Some("Linear"), Some("SS")) => "LINEAR_SS_AUDIO_CHUNK_ENG"
            case _ => "CDN_UNKNOWN"
          }
          case csm if isManifest(csm) => (chunkType, streamType) match {
            case (Some(ct), Some("HLS")) => ct.toUpperCase + "_HLS_MANIFEST"
            case _ => "UNKNOWN_HLS_MANIFEST"
          }
          case csm if isKey(csm) => (chunkType, streamType) match {
            case (Some(ct), Some("HLS")) => ct.toUpperCase + "_HLS_KEY"
            case _ => "UNKNOWN_HLS_KEY"
          }
          case _ => (chunkType, streamType) match {
            case (Some(ct), Some(st)) => Array(ct.toUpperCase, st.toUpperCase, chunkName(raw.cs_method)).mkString("_")
            case (None, Some(st)) => Array("UNKNOWN", st.toUpperCase, chunkName(raw.cs_method)).mkString("_")
            case _ => "CDN_UNKNOWN"
          }
        }
      )
    }


  }

  def httpStatusCode = parseIntRawValue(x => x.toInt)(raw.sc_status)

  def userAgent = parseRawValue(raw.cs_user_agent)

  def contentSize = parseLongRawValue(x => x.toLong)(raw.sc_stream_bytes)

  def targetServer = parseRawValue(raw.event_type)

  def year = parseDatePartRawValue(datePartUtil)(raw.date_string.substring(1, raw.date_string.length-1), "yyyy")

  def month = parseDatePartRawValue(datePartUtil)(raw.date_string.substring(1, raw.date_string.length-1), "MM")

  def day = parseDatePartRawValue(datePartUtil)(raw.date_string.substring(1, raw.date_string.length-1), "dd")

  def hour = parseDatePartRawValue(datePartUtil)(raw.date_string.substring(1, raw.date_string.length-1), "HH")

  def minute = parseDatePartRawValue(datePartUtil)(raw.date_string.substring(1, raw.date_string.length-1), "mm")

  def second = parseDatePartRawValue(datePartUtil)(raw.date_string.substring(1, raw.date_string.length-1), "ss")

  def utc = parseLongRawValue(x => x.toLong)(raw.unix_time)

  def mappedUserAgent = ???

  def cacheDeliveryType = parseRawValue(raw.s_cache_status)

  def contentType = ???

  def videoAssetTime = ???

  def bitRate = ???

  def abrLevel = ???

  def chunkSeq = ???

  def sDns = parseRawValue(raw.c_info) match {
    case Some(s) => Some(s.substring(s.indexOf("nginx-access: ") + "nginx-access: ".length))
    case _ => None
  }

  def sIp = ???

  def csMethod = parseRawValue(raw.cs_method)

  def csVersion = ???

  def csReferrer = parseRawValue(raw.cs_referrer)

  def scBytes = parseLongRawValue(x=>x.toLong)(raw.sc_bytes)

  def csUri = parseRawValue(raw.cs_uri)

  def xDuration = parseIntRawValue(x=>x.toInt)(raw.x_duration)

  def dynamicLoadIp = parseRawValue(raw.dynamic_ip)

  def streamType = raw.cs_method match {
    case st if st.indexOf("/ss/") != -1 => Some("SS")
    case st if st.indexOf("/hls/") != -1 => Some("HLS")
    case _ => None
  }

  def chunkType = raw.event_type match {
    case ct if ct.contains("linear") => Some("Linear")
    case ct if ct.contains("video") != -1 => Some("Vod")
    case _ => None
  }

  def downloadTime = parseIntRawValue((x => x.toInt))(raw.download_speed)

  def sessionId = ???
  def logSessionId = ???
  def userGUID = ???
  def sVxRate = ???
  def sVxRateStatus = ???
  def xVxSerial = ???
  def rsBytes = ???
  def xProtohash = ???
  def csVxToken = ???
  def cPort = ???
  def cVxZone  = ???
  def csCookie  = ???
  def scVxDownloadRate = ???
  def requestId = ???
  def drmClassification = ???
  def cdnLocation = ???
  def serverName = ???


}
