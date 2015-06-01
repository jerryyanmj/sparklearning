//Session ID
getSessionIDFromCSURI.outputSchema = "session_id:chararray";
function getSessionIDFromCSURI(cs_method)
{
	if(cs_method != null)
	{	
    	var session = getSessionIdField(cs_method);
		var url_tokens = session.split("&");
		if(url_tokens.length > 0)
		{
			return url_tokens[0];
		}
		else
		{
			return "";
       		 }
	}
	else
	{
		return "";
	}
}

//Get Stream Type 
getStreamTypeString.outputSchema = "stream_type:chararray";
function getStreamTypeString(event_string)
{
	var stream_type = "";
	if (event_string.indexOf("linear")>-1)
	{
		return "lin";
	}
	else if (event_string.indexOf("video")>-1)
	{
		return "video";
	}
	else 
	{
	 return "";
	}
}  


//Event Type Field
getEventTypeString.outputSchema = "event_type:chararray";
function getEventTypeString(cs_method)
{
	var event_type = "CDN_UNKNOWN";
	var stream_type = getStreamTypeString(event_type);
	
	if((stream_type != null) && (stream_type != null))
	{
	if (cs_method.indexOf("hls-lin-test")>-1)
	{
		event_type = "TEST_REQUEST";
	}
	else if(cs_method.indexOf(".ts") > -1) 
   	{
		if(((stream_type.indexOf("lin") > -1)||
		(cs_method.indexOf("hls") > 0))&&
			(cs_method.indexOf(".com/Assets")<0))
		{	
		    	event_type = "LINEAR_HLS_VIDEO_CHUNK";
		}
		else if((stream_type.indexOf("vod") >-1) || (cs_method.indexOf(".com/Assets" > -1)))
		{
			event_type = "VOD_HLS_VIDEO_CHUNK";
		}
		else
		{
			event_type = "UNKNOWN_HLS_VIDEO_CHUNK";
		}
	}
	else if((cs_method.indexOf("Fragments(video") >-1) || (cs_method.indexOf("FragmentInfo(video")>-1))
	{
		if((stream_type.indexOf("lin") > -1) || 
			 (cs_method.indexOf(".com/Assets")< 0))
		{	
		    	event_type = "LINEAR_SS_VIDEO_CHUNK";
		}
		else if((stream_type.indexOf("vod") >-1) || (cs_method.indexOf(".com/Assets" > -1)))
		{
			event_type = "VOD_SS_VIDEO_CHUNK";
		}
		else
		{
			event_type = "UNKNOWN_SS_VIDEO_CHUNK";
		}
	}
    	else if((cs_method.indexOf("Fragments(textstream") >-1) || (cs_method.indexOf("FragmentInfo(textstream")>-1))
        {
                if((stream_type.indexOf("lin") > -1)||
                        (cs_method.indexOf(".com/Assets")<0))
                {
                        event_type = "LINEAR_SS_TEXT_CHUNK";
                }
		 else if((stream_type.indexOf("vod") >-1) || (cs_method.indexOf(".com/Assets" > -1)))
                {
                        event_type = "VOD_SS_TEXT_CHUNK";
                }
                else
             {
                        event_type = "UNKNOWN_SS_TEXT_CHUNK";
		}

        }
	else if((cs_method.indexOf("isml/Manifest") >-1) || (cs_method.indexOf("ism/Manifest")>-1) || (cs_method.indexOf("isml/manifest") >-1) || (cs_method.indexOf("ism/manifest")>-1))
        {
                if((stream_type.indexOf("lin") > -1)||
                        (cs_method.indexOf(".com/Assets")<0))
                {
                        event_type = "LINEAR_SS_MANIFEST_CHUNK";
                }
                 else if((stream_type.indexOf("vod") >-1) || (cs_method.indexOf(".com/Assets" > -1)))
                {
                        event_type = "VOD_SS_MANIFEST_CHUNK";
                }
                else
                {
                        event_type = "UNKNOWN_SS_MANIFEST_CHUNK";
		}
        }
	else if((cs_method.indexOf("Fragments(audio") >-1) || (cs_method.indexOf("FragmentInfo(audio")>-1))
	{
		if((stream_type.indexOf("lin") > -1)||
			(cs_method.indexOf(".com/Assets")<0))
		{	
		    	event_type = "LINEAR_SS_AUDIO_CHUNK";
		}
		else if((stream_type.indexOf("vod") >-1) || (cs_method.indexOf(".com/Assets" > -1)))
		{
			event_type = "VOD_SS_AUDIO_CHUNK";
		}
		else
		{
			event_type = "UNKNOWN_SS_AUDIO_CHUNK";
		}
	}
	else if((cs_method.indexOf("FragmentInfo(301_eng") >-1) || (cs_method.indexOf("Fragments(301_eng") >-1) || (cs_method.indexOf("Fragments(302_eng") >-1) || (cs_method.indexOf("FragmentInfo(302_eng") >-1))
        {
                if(((stream_type.indexOf("lin") > -1)||
                        (cs_method.indexOf("ss-rsn") > -1)))
                {
                        event_type = "LINEAR_SS_AUDIO_CHUNK_ENG";
                }
                else
                {
                        event_type = "CDN_UNKNOWN";
                }
        }
     else if((cs_method.indexOf("FragmentInfo(301_eng") >-1) || (cs_method.indexOf("Fragments(301_eng") >-1) || (cs_method.indexOf("Fragments(302_eng") >-1)|| (cs_method.indexOf("FragmentInfo(302_eng") >-1))
        {
                if(((stream_type.indexOf("lin") > -1)||
                        (cs_method.indexOf("ss-rsn") > -1)))
                {
                        event_type = "LINEAR_SS_AUDIO_CHUNK_ENG";
                }
                else
                {
                        event_type = "CDN_UNKNOWN";
                }
        }
	else if(cs_method.indexOf(".m3u8")>-1)
	{
		if((stream_type.indexOf("lin") > -1) ||
			(cs_method.indexOf(".com/Assets")<0))
		{	
		    	event_type = "LINEAR_HLS_MANIFEST";
		}
		else if((stream_type.indexOf("vod") >-1) || (cs_method.indexOf(".com/Assets" > -1)))
		{
			event_type = "VOD_HLS_MANIFEST";
		}
		else
		{
			event_type = "UNKNOWN_HLS_MANIFEST";
		}
	}
	else if(cs_method.indexOf(".key")>-1)
	{
		if((stream_type.indexOf("lin") > -1)||
           (cs_method.indexOf(".com/Assets")<0))
                 {
                         event_type = "LINEAR_HLS_KEY";
                 }
                 else if((stream_type.indexOf("vod") >-1) || (cs_method.indexOf(".com/Assets" > -1)))
                 {
                         event_type = "VOD_HLS_KEY";
                 }
                 else
                 {
                         event_type = "UNKNOWN_HLS_KEY";
                 }
	}
        else if(cs_method.indexOf("BAD_REQUEST")>-1)
        {
                event_type = "BAD_REQUEST";
        }

  	else if(cs_method.indexOf("clientaccesspolicy.xml")>-1)
        {
                event_type = "CLIENT_ACCESS_POLICY";
        }
	else if(cs_method.indexOf("crossdomain.xml")>-1)
        {
                event_type = "CROSS_DOMAIN_XML";
        }
        else if(cs_method.indexOf("genres")>-1)
        {
                event_type = "GENRE_SERVICE_REQUEST";
        }
}
	return event_type;
}


//User Agent Field
//"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/600.4.10 (KHTML, like Gecko) Version/8.0.4 Safari/600.4.10" 
getUserAgentString.outputSchema = "user_agent:chararray";
function getUserAgentString(cs_user_agent)
{
	if(cs_user_agent != null)
	{	
       		var user_agent = cs_user_agent.split("\"");
        	return user_agent[1];
	}
	else
	{
		return "";
	}
}

//User Guid Field
//In the case we get the cookie we will update this
getUserGuidField.outputSchema = "user_guid:chararray";
function getUserGuidField(cs_vx_token)
{
	if (cs_vx_token != null)
{	
        if (cs_vx_token.indexOf("RequestID")>-1)
        {
                user_guid=""
                parameter="customer_guid"
                if (cs_vx_token.indexOf(parameter) != -1)
                {
                        for(i=cs_vx_token.indexOf(parameter)+parameter.length+1; i<cs_vx_token.length; i++)
                        {
                                if(cs_vx_token.charAt(i) != "A")
                                {
                                        user_guid+=cs_vx_token.charAt(i);
                                } else
                                {
                                        return user_guid;
                                }
                        }
                 }

        }
        else if (cs_vx_token.indexOf("customerGuid")>-1)
        {
                user_guid=""
                parameter="customerGuid"
                if (cs_vx_token.indexOf(parameter) != -1)
                {
                        for(i=cs_vx_token.indexOf(parameter)+parameter.length+1; i<cs_vx_token.length; i++)
                        {
                                if(cs_vx_token.charAt(i) != "&")
                                {
                                        user_guid+=cs_vx_token.charAt(i);
                                } else
                                {
                                        return user_guid;
                                }
                        }
                 }
        }
	else if (cs_vx_token.indexOf("CustomerGUID")>-1)
        {
                user_guid=""
                parameter="CustomerGUID"
                if (cs_vx_token.indexOf(parameter) != -1)
                {
                        for(i=cs_vx_token.indexOf(parameter)+parameter.length+1; i<cs_vx_token.length; i++)
                        {
                                if(cs_vx_token.charAt(i) != "&")
                                {
                                        user_guid+=cs_vx_token.charAt(i);
                                } else
                                {
                                        return user_guid;
                                }
                        }
                 }
        }
        else{
                var vx_token = cs_vx_token.split("customer_guid=");
                if(vx_token.length > 1)
                {
                return vx_token[1];
                }
        }
}
else
{
	return "";
}
}

//Target Server Field 
getTargetServerString.outputSchema = "target_server:chararray";
function getTargetServerString(cs_uri)
{
	if(cs_uri != null)
	{
        	var url_tokens = cs_uri.split("/");
        	return url_tokens[2];
	}
	else
	{
		return "";
	}
}
                
//Year Field
getYear.outputSchema = "year:chararray";
function getYear(date_string)
{
    if(date_string != null)
    {
            var year = date_string.split("/",4);
            if(year.length > 1)
            {
                return year[2].substr(0,4); 
            }
            else
            {
                   return "nothing";
            }
    }
    else
    {
        return "nothing";
    }
}
//Month Field
getMonth.outputSchema = "month:chararray";
function getMonth(date_string)
{
    if(date_string != null)
    {
            var month = date_string.split("/",4);
            if(month.length > 1)
            {
                return month[1].substr(0,3);
            }
            else
            {
                   return "nothing";
            }
    }
    else
    {
        return "nothing";
    }
}

//Day Field
getDay.outputSchema = "day:int";
function getDay(date_string)
{
	if(date_string != null)
	{
            var day = date_string.split("[",2);
            if(day.length > 1)
            {
                return day[1].substr(0,2);
            }
            else
            {
                   return "nothing";
            }
    }
    else
    {
        return "nothing";
    }
}

//Hour  Field
getHour.outputSchema = "hour:int";
function getHour(date_string)
{
	if(date_string != null)
	{
        var hour = date_string.split(":");
        if(hour.length > 1)
        {
        return hour[1].substr(0,2);
        }
        else
        {
        return "hour";
        } 
	}
	else
	{
		return "";
	}
}
                           
//Minute  Field
getMinute.outputSchema = "minute:int";
function getMinute(date_string)
{
	if(date_string != null)
	{
        var minute = date_string.split(":");
        if(minute.length > 1)
        {
        return minute[2].substr(0,2);
	}
        else
        {
        return "minute";
         }
	}
	else
	{
		return "";
	}
}

//Second Field
getSecond.outputSchema = "second:int";
function getSecond(date_string)
{
	if(date_string != null)
	{
        	var second = date_string.split(":");
        	if(second.length > 1)
       		 {
        return second[3].substr(0,2);
        }
        else
        {
        return "second";
        }
	}
	else
	{
		return "";
	}
}




//Video Asset Name Field 
getVideoAssetNameString.outputSchema = "video_asset_name:chararray";
function getVideoAssetNameString(cs_method)
{
	if(cs_method != null)
	{
		var url_tokens = cs_method.split("/");
		var thirdToken=url_tokens[5];
		if ( thirdToken == "DRM" ) {
		return url_tokens[5];
		} else {
		return url_tokens[5];
	}}
	else
	{
		return "";
	}
}


//Clean Video Asset Name
//getCleanVideoAssetNameString.outputSchema = "video_asset_name2:chararray";
//function getCleanVideoAssetNameString(cs_method)
//{
//      var video_asset_name = getVideoAssetNameString(cs_method);
//	  if(video_asset_name.indexOf(".isml")>1)
//      {
//      var video_asset = video_asset_name.split(".isml");
//      return video_asset[0];
//      }
//      else
//      {
//      return video_asset_name;
//}         
//} 


getVideoAssetNameString.outputSchema = "video_asset:chararray";
function getVideoAssetNameString(cs_method)
{
    if(cs_method != null)
    {
        var url_tokens = cs_method.split("/");
        var thirdToken=url_tokens[5];
        if(thirdToken != null)
    {
        thirdToken = thirdToken.replace(".isml","");
    }
        return thirdToken;
    }
}

getContentField.outputSchema = "content:int";
function getContentField(sc_stream_bytes)
{
        var content_size=sc_stream_bytes;
        return content_size;
}

                                                                  
//BitRate
getBitRate.outputSchema = "bit_rate:int";
function getBitRate(event_type, cs_method,sc_stream_bytes)
{
        bitrate='';
        
        var stream_type = getStreamTypeString(event_type);
        var content_size = getContentField(sc_stream_bytes);

    if(cs_method != null)
    {
     if(cs_method.indexOf(".ts") > 0)
        {
                if(((stream_type.indexOf("lin") > -1) &&
                        (cs_method.indexOf("hls") > -1))&&
                        (cs_method.indexOf(".com/Assets")<0))
                {
                        event_type = "LINEAR_HLS_VIDEO_CHUNK";

                        if (content_size >= '2200000')
                        {
                                bitrate = '3400000'
                                return bitrate;
                        }
                        if (content_size >= '1600000'  && content_size < '2200000')
                        {
                                bitrate = '2400000'
                                return bitrate;
                        }
                        if (content_size >= '900000'  && content_size < '1600000')
                        {
                                bitrate = '1400000'
                                return bitrate;
                        }
                        if (content_size >= '700000'  && content_size < '900000')
                        {
                                bitrate = '900000'
                                return bitrate;
                        }
                        if (content_size >= '0'  && content_size < '700000')
                        {
                                bitrate = '600000'
                                return bitrate;
                        }

                }

                else if((stream_type.indexOf("vod") >-1) || (cs_method.indexOf(".com/Assets" > -1)))
                {
                        event_type = "VOD_HLS_VIDEO_CHUNK";
                        if (content_size >= '2200000')
                        {
                                bitrate = '3400000'
                                return bitrate;
                        }
                        if (content_size >= '1600000'  && content_size < '2200000')
                        {
                                bitrate = '2400000'
                                return bitrate;
                        }
                        if (content_size >= '900000'  && content_size < '1600000')
                        {
                                bitrate = '1400000'
                                return bitrate;
                        }
                        if (content_size >= '700000'  && content_size < '900000')
                        {
                                bitrate = '900000'
                                return bitrate;
                        }
                        if (content_size >= '0'  && content_size < '700000')
                        {
                                bitrate = '600000'
                                return bitrate;
                        }
                }
        }
                else
        {
                parameter="QualityLevels"
                if (cs_method.indexOf(parameter) != -1)
                {
                        for(i=cs_method.indexOf(parameter)+parameter.length+1; i<cs_method.length; i++)
                        {
                                if(cs_method.charAt(i) != ")")
                                {
                                        bitrate+=cs_method.charAt(i);
                                }
                                else
                                {
                                        return bitrate;
                                }

                        }
                }
        }
    }
    else
    {
        return '0';
    }

}

//Request ID Field
getRequestIdField.outputSchema = "request_id:chararray";
function getRequestIdField(cs_vx_token)
{
	requestid=""
	parameter="RequestID"
	if(cs_vx_token != null)
	{
	if (cs_vx_token.indexOf(parameter) != -1) {
		for(i=cs_vx_token.indexOf(parameter)+parameter.length+1; i<cs_vx_token.length; i++) {
			if(cs_vx_token.charAt(i) != "&") {
				requestid+=cs_vx_token.charAt(i);
			} else {
				return requestid;
			}
		}
	} else {
		return "";
	}
}
else
{	
	return "";
}
		
}

//Session ID Field
getSessionIdField.outputSchema = "session_id:chararray";
function getSessionIdField(cs_method)
{
	if(cs_method != null)
	{
	if(cs_method.indexOf("SessionID") > -1)
        {
                var vx_token = cs_method.split("SessionID=");
                if(vx_token.length > 1)
                {
                        return vx_token[1];
                }
                else
                {
                        return "-1";
                }
        }
        else if (cs_method.indexOf("sessionId") > -1)
        {
                var vx_token = cs_method.split("sessionId=");
                if(vx_token.length > 1)
                {
                        return vx_token[1];
                }
                else
                {
                        return "1";
                }
        }
        else{
                return "";
        }
}
else
{
	return "";
}
}

//ABR Level Field
getABR.outputSchema = "abr_val:chararray";
function getABR(cs_method)
{
	if(cs_method != null)
	{	
                patternMatch = cs_method.match(/\d\d\d\d\d\d\d\dT\d\d\d\d\d\d-\d(\d)-\d\d\d\d\d\d/);

                if (patternMatch != null) {
                        return patternMatch[1];
                } 
		else 
		{
                        return "";
                }
                } 
		else 
		{
                        return "";
                }
}

//Chunk Sequence  Level Field
getChunkSequenceField.outputSchema = "chunk_sequence:chararray";
function getChunkSequenceField(cs_method)
{
	if(cs_method != null)
	{	
                patternMatch = cs_method.match(/\d\d\d\d\d\d\d\dT\d\d\d\d\d\d-\d\d-(\d\d\d\d\d\d)/);

                if (patternMatch != null) {
                        return patternMatch[1];
                } else {
                        return "";
                }
                } 
		else 
		{
                        return "";
                }
}

getGeminiNodeName.outputSchema = "node_name:chararray";
function getGeminiNodeName(c_info)
{
	    var gemini_node = "";
        var url_tokens = c_info.split("nginx-access: ");
        if(url_tokens.length > 0)
        {
            return url_tokens[1];
        }
        else
        {
            return "";
             }
}

getCSVersion.outputSchema = "cs_version:chararray";
function getCSVersion(cs_method)
{
        var url_tokens = cs_method.split("HTTP/");
        if(url_tokens.length > 0)
        {
            return url_tokens[1];
        }
        else
        {
            return "";
             }
}

getCsReferrerField.outputSchema = "cs_referrer:chararray"
function getCsReferrerField(cs_referrer)
{
	if(cs_referrer != null)
	{	
	cs_ref = cs_referrer.replace(/"/g, "");
	return cs_ref;
                } 
		else 
		{
                        return "";
                }
}

//Cookie Field
getCsCookieField.outputSchema = "cs_cookie:chararray"
function getCsCookieField(cs_cookie)
{
	if(cs_cookie != null)
	{	
        cs_cookies = cs_cookie.replace(/"/g, "");
        return cs_cookies;
                } 
		else 
		{
                        return "";
                }
}

getMappedUserAgentString.outputSchema = "mapped_user_agent:chararray";
function getMappedUserAgentString(user_agent)
{
	if(user_agent != null)
	{	
        var agent = user_agent.toLowerCase();

        if (agent.indexOf("ipad") > -1)
        {
                mapped_user_agent = "ipad";
        }
        else if(agent.indexOf("roku") > -1)
        {
                mapped_user_agent = "roku";
        }
        else if(agent.indexOf("ipod") > -1)
        {
                mapped_user_agent = "ipod";
        }
        else if(agent.indexOf("iphone") > -1)
        {
                 mapped_user_agent = "iphone";
        }
        else if(agent.indexOf("ios_other") > -1)
        {
                mapped_user_agent = "ios_other";
        }
        else if(agent.indexOf("android") > -1)
        {
                mapped_user_agent = "android";
        }
        else if(agent.indexOf("online_video_portal") > -1)
        {
                mapped_user_agent = "online_video_portal";
        }
        else if(agent.indexOf("twctv") > -1)
        {
                 mapped_user_agent = "twctv";
        }
        else if(agent.indexOf("xbox") > -1)
        {
                mapped_user_agent = "xbox";
        }
	else if(agent.indexOf("samsung smart tv") > -1)
        {
                mapped_user_agent = "samsung smart tv";
        }
        else if(agent.indexOf("samsunghas-agent") > -1)
        {
                mapped_user_agent = "samsung smart tv";
        }
        else if(agent.indexOf("applecoremedia") > -1)
        {
                mapped_user_agent = "ios_other";
        }
        else if(agent.indexOf("twcdeportes") > -1)
        {
                mapped_user_agent = "twcdeportes";
        }
        else if(agent.indexOf("twcsportsnet") > -1)
                {
                mapped_user_agent = "twcsportsnet";
        }
        else if(agent.indexOf("other") > -1)
        {
                mapped_user_agent = "other";
        }
        else if(agent.indexOf("ruby") > -1)
        {
                mapped_user_agent = "ruby";
        }
        else if(agent.indexOf("ios_rsn_espanol") > -1)
        {
                mapped_user_agent = "ios_rsn_espanol";
        }
        else if(agent.indexOf("ios_rsn_english") > -1)
        {
                mapped_user_agent = "ios_rsn_english";
        }
        else if(agent.indexOf("nativehost") > -1)
        {
                mapped_user_agent = "xbox";
        }
        else if(agent.indexOf("mozilla") > -1)
        {
                mapped_user_agent = "online_video_portal";
        }
        else if(agent.indexOf("htc streaming player") >-1)
        {
                mapped_user_agent = "android";
        }
        else
        {
                mapped_user_agent ="UNKNOWN";
        }
        return mapped_user_agent;
                } 
		else 
		{
                        return "";
                }
}

getCDNLocationString.outputSchema = "cdn_location:chararray";
function getCDNLocationString(cs_uri)
{       
        var target_server = getTargetServerString(cs_uri);
        var cdn_location = "";


        if ((target_server.indexOf("hls-lin")>-1) || (target_server.indexOf("ss-lin")>-1))
        {
        
        var n = /\d+/.exec(target_server)[0];
        if (n >= 1 && n <= 40)  
        {
                if (target_server.indexOf("hls-lin")>-1)
                {
                        cdn_location = "Peakview";
                }
                else if (target_server.indexOf("ss-lin")>-1)
                {
                        cdn_location = "Peakview";
                } 
                else
                {
                        cdn_location = "Unknown"
                }
        }
        else if (n >= 101 && n <= 140)
        {
                if (target_server.indexOf("hls-lin")>-1)
                {
                        cdn_location = "Peakview";
                }
                else if (target_server.indexOf("ss-lin")>-1)
                {
                        cdn_location = "Peakview";
                }
                else
                {
                        cdn_location = "Unknown"
                }
        }
        else if (n >= 201 && n <= 240)
        {
                if (target_server.indexOf("hls-lin")>-1)
                {
                        cdn_location = "Charlotte";
                }
                else if (target_server.indexOf("ss-lin")>-1)
                {
                        cdn_location = "Charlotte";
                }
                else
                {
                        cdn_location = "Unknown"
                }
        }
        else if (n >= 301 && n <= 340)
        {
                if (target_server.indexOf("hls-lin")>-1)
                {
                        cdn_location = "Charlotte";
                }
                else if (target_server.indexOf("ss-lin")>-1)
                {
                        cdn_location = "Charlotte";
                }
                else
                {
                        cdn_location = "Unknown"
                }
        }
        }
        else
        {
                cdn_location =""
        }
        return cdn_location;
}


getDRMClassificationString.outputSchema = "drm_classification:chararray";
function getDRMClassificationString(event_type,cs_method)
{
        var target_server = getStreamTypeString(event_type);
		var stream_type= "";
        var drm_classification = "";

        if ((target_server.indexOf("lin")>-1) || (cs_method.indexOf("ss")>-1))
        {
        var n = /\d+/.exec(target_server)[0];
        if ((n >= 1 && n <= 40)||(n >= 201 && n <= 240))
        {
                if ((target_server.indexOf("lin")>-1)  ||  (cs_method.indexOf("hls")>-1))
                {
                        drm_classification = "Non-DRM HLS";
                }
                else if ((target_server.indexOf("lin")>-1)  ||  (cs_method.indexOf("ss")>-1))
                {
                        drm_classification = "Non-DRM SS";
                }
                else
                {
                        drm_classification = "Unknown";
                }
        }
        else if ((n >= 101 && n <= 140)||(n >= 301 && n <= 340))
        {
                if  ((target_server.indexOf("lin")>-1)  ||  (cs_method.indexOf("hls")>-1))
                {
                        drm_classification = "DRM HLS";
                }
                else if ((target_server.indexOf("lin")>-1)  ||  (cs_method.indexOf("ss")>-1))
                {
                        drm_classification = "DRM SS";
                }
                else
                {
                        drm_classification = "Unknown";
                }
        }
    }
        else
        {
                drm_classification =""
        }
        return drm_classification;
}
