package model

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.joda.time.DateTime
import org.joda.time.format._

/**
 * Created by jiarui.yan on 5/31/15.
 */

class GeminiRaw(args: Array[String]) extends Serializable {
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

class GeminiRawMiddleTier(args: Array[String]) extends Serializable {
  val Array(c_info, c_ip, date_string, cs_method, sc_status, sc_stream_bytes, cs_uri, cs_user_agent, x_duration, s_cache_status) = args
}

class GeminiParsedMiddleTier(rawLog: GeminiRawMiddleTier) extends Serializable {
  val raw = rawLog

  def csMethod = raw.cs_method
}

class GeminiParsed(rawLog: GeminiRaw) extends Serializable with GeminiParserHelper {
  val raw = rawLog

  def clientIp = parseRawValue(raw.c_ip)

  def dateTime = parseRawValue(raw.date_string)

  def eventType = {

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

  def year = parseDatePartRawValue(datePartUtil)(dateStrExtractor(raw.date_string), "yyyy")

  def month = parseDatePartRawValue(datePartUtil)(dateStrExtractor(raw.date_string), "MM")

  def day = parseDatePartRawValue(datePartUtil)(dateStrExtractor(raw.date_string), "dd")

  def hour = parseDatePartRawValue(datePartUtil)(dateStrExtractor(raw.date_string), "HH")

  def minute = parseDatePartRawValue(datePartUtil)(dateStrExtractor(raw.date_string), "mm")

  def second = parseDatePartRawValue(datePartUtil)(dateStrExtractor(raw.date_string), "ss")

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
