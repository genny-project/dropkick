package life.genny.utils;
//
//import com.google.common.collect.FluentIterable;
//import com.google.common.collect.Sets;
//import com.google.gson.reflect.TypeToken;
//
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//import io.quarkus.runtime.annotations.RegisterForReflection;
//import io.vertx.core.eventbus.MessageProducer;
//import life.genny.eventbus.EventBusInterface;
//import life.genny.eventbus.EventBusMock;
//import life.genny.models.GennyToken;
//import life.genny.qwanda.Answer;
//import life.genny.qwanda.Ask;
//import life.genny.qwanda.datatype.DataType;
//import life.genny.qwanda.attribute.Attribute;
//import life.genny.qwanda.attribute.EntityAttribute;
//import life.genny.qwanda.entity.BaseEntity;
//import life.genny.qwanda.entity.SearchEntity;
//import life.genny.qwanda.exception.BadDataException;
//import life.genny.qwanda.message.*;
//import life.genny.qwandautils.GennyCacheInterface;
//import life.genny.qwandautils.GennySettings;
//import life.genny.qwandautils.jsonb;
//import life.genny.qwandautils.QwandaUtils;
//import life.genny.qwandautils.ANSIColour;
//import life.genny.utils.BaseEntityUtils;
//import org.apache.commons.lang.ArrayUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.http.client.ClientProtocolException;
//import org.apache.logging.log4j.Logger;
//
//import javax.naming.NamingException;
//import java.io.IOException;
//import java.lang.invoke.MethodHandles;
//import java.lang.reflect.Type;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.function.Consumer;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//import java.util.stream.Collectors;
//
//import java.time.LocalDateTime;
//import java.time.LocalDate;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.naming.NamingException;

import org.acme.kafka.streams.aggregator.model.ApiQwandaService;
import org.acme.kafka.streams.aggregator.model.ApiService;
import org.acme.kafka.streams.aggregator.model.ApiUtilsService;
import org.acme.kafka.streams.aggregator.streams.TopologyProducer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.TypeLiteral;
import org.apache.http.client.ClientProtocolException;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.dropkick.live.data.InternalProducer;
import life.genny.models.GennyToken;
import life.genny.qwanda.Answer;
import life.genny.qwanda.Ask;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeText;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.datatype.DataType;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.SearchEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.message.MessageData;
import life.genny.qwanda.message.QBulkMessage;
import life.genny.qwanda.message.QCmdMessage;
import life.genny.qwanda.message.QDataAskMessage;
import life.genny.qwanda.message.QDataAttributeMessage;
import life.genny.qwanda.message.QDataBaseEntityMessage;
import life.genny.qwanda.message.QEventMessage;
import life.genny.qwandautils.GennySettings;

@RegisterForReflection
@ApplicationScoped
public class VertxUtils {

	private static final Logger log = Logger.getLogger(VertxUtils.class);
     public boolean cachedEnabled = false;

 
    static final String DEFAULT_TOKEN = "DUMMY";
    static final String[] DEFAULT_FILTER_ARRAY = { "PRI_FIRSTNAME", "PRI_LASTNAME", "PRI_MOBILE", "PRI_IMAGE_URL",
            "PRI_CODE", "PRI_NAME", "PRI_USERNAME" };

    public enum ESubscriptionType {
        DIRECT, TRIGGER;

    }

    static Jsonb jsonb = JsonbBuilder.create();
    
    @Inject
    InternalProducer producer;
    
	@Inject
	@RestClient
	ApiService apiService;

	@Inject
	@RestClient
	ApiQwandaService apiQwandaService;
	
	@Inject
	@RestClient
	ApiUtilsService apiUtilsService;


	static public Map<String,Map<String, Attribute>> realmAttributeMap = new ConcurrentHashMap<>();
	static public Map<String, Map<String, BaseEntity>> defs = new ConcurrentHashMap<>(); // realm and DEF lookup
	    static public QDataAttributeMessage attributesMsg = null;

    public List<Answer> answerBuffer = new ArrayList<Answer>();


   public void setRealmFilterArray(final String realm, final String[] filterArray) {
        putStringArray(realm, "FILTER", "PRIVACY", filterArray);
    }

    public String[] getRealmFilterArray(final String realm) {
        String[] result = getStringArray(realm, "FILTER", "PRIVACY");
        if (result == null) {
            return DEFAULT_FILTER_ARRAY;
        } else {
            return result;
        }
    }

    public <T> T getObject(final String realm, final String keyPrefix, final String key, final Class clazz) {
        return getObject(realm, keyPrefix, key, clazz, DEFAULT_TOKEN);
    }

    public <T> T getObject(final String realm, final String keyPrefix, final String key, final Class clazz,
                                  final String token) {
        T item = null;
        String prekey = (StringUtils.isBlank(keyPrefix)) ? "" : (keyPrefix + ":");
        JsonObject json = readCachedJson(realm, prekey + key, token);
        if (json.getString("status").equalsIgnoreCase("ok")) {
            String data = json.getString("value");
            try {
                item = (T) jsonb.fromJson(data, clazz);
            } catch (Exception e) {
                log.error("Bad jsonb " + realm + ":" + key + ":" + clazz.getTypeName());
            }
            return item;
        } else {
            return null;
        }

    }

    public <T> T getObject(final String realm, final String keyPrefix, final String key, final Type clazz) {
        return getObject(realm, keyPrefix, key, clazz, DEFAULT_TOKEN);
    }

    public <T> T getObject(final String realm, final String keyPrefix, final String key, final Type clazz,
                                  final String token) {
        T item = null;
        String prekey = (StringUtils.isBlank(keyPrefix)) ? "" : (keyPrefix + ":");
        JsonObject json = readCachedJson(realm, prekey + key, token);
        if (json.getString("status").equalsIgnoreCase("ok")) {
            String data = json.getString("value");
            try {
                item = (T) jsonb.fromJson(data, clazz);
            } catch (Exception e) {
                log.info("Bad jsonb " + realm + ":" + key + ":" + clazz.getTypeName());
            }
            return item;
        } else {
            return null;
        }
    }

    public void putObject(final String realm, final String keyPrefix, final String key, final Object obj) {
        putObject(realm, keyPrefix, key, obj, DEFAULT_TOKEN);
    }

    public void putObject(final String realm, final String keyPrefix, final String key, final Object obj,
                                 final String token) {
        String data = jsonb.toJson(obj);
        String prekey = (StringUtils.isBlank(keyPrefix)) ? "" : (keyPrefix + ":");

        writeCachedJson(realm, prekey + key, data, token);
    }



    public JsonObject readCachedJson(final String realm, final String key) {
        return readCachedJson(realm, key, DEFAULT_TOKEN);
    }

    public JsonObject readCachedJson(String realm, final String key, final String token) {
        JsonObject result = null;

        if (!GennySettings.forceCacheApi) {
            String ret = null;
            try {
                // log.info("VERTX READING DIRECTLY FROM CACHE! USING
                // "+(GennySettings.isCacheServer?" LOCAL DDT":"CLIENT "));
                if (key == null) {
                    log.error("The key needed for the cache to retrieve an entry is null");

                    return null;
                }
                ret = apiService.getDataFromCache(realm, key,"Bearer "+ token);
               // ret = (String) cacheInterface.readCache(realm, key, token);
            } catch (Exception e) {
                log.error("Cache is  null maybe the realm is not provided or was the wrong realm. The realm is a follows::: " + realm + " ::: if nothing appears within the colons the realm is a empty string");
                e.printStackTrace();
            }
            if (ret != null) {
                result =  javax.json.Json.createObjectBuilder().add("status", "ok").add("value", ret).build();
            } else {
                result = javax.json.Json.createObjectBuilder().add("status", "error").add("value", ret).build();
            }
        } else {
            String resultStr = null;
            //	log.info("VERTX READING FROM CACHE API!");
			if (cachedEnabled) {
			    if ("DUMMY".equals(token)) {
			        // leave realm as it
			    } else {
			        GennyToken temp = new GennyToken(token);
			        realm = temp.getRealm();
			    }

//                    resultStr = (String) localCache.get(realm + ":" + key);
//                    if ((resultStr != null) && (!"\"null\"".equals(resultStr))) {
//                        String resultStr6 = null;
//                        if (false) {
//                            // ugly way to fix json
//                            resultStr6  = VertxUtils.fixJson(resultStr);
//                        } else {
//                            resultStr6 = resultStr;
//                        }
//                        // JsonObject rs2 = new JsonObject(resultStr6);
//                        // String resultStr2 = resultStr.replaceAll("\\","");
//                        // .replace("\\\"","\"");
//                        JsonObject resultJson = javax.json.Json.createObjectBuilder().add("status", "ok").add("value", resultStr6).build();
//                        resultStr = resultJson.toString();
//                    } else {
//                        resultStr = null;
//                    }

			} else {
			    log.debug(" DDT URL:" + GennySettings.ddtUrl + ", realm:" + realm + "key:" + key + "token:" + token );
			    //resultStr = QwandaUtils.apiGet(GennySettings.ddtUrl + "/service/cache/read/" + realm + "/" + key, token);
			    int count=5;
			    boolean resultFound = false;
			    while (count > 0) {
			    	resultStr = apiService.getDataFromCache(realm, key,"Bearer "+ token);
			       // resultStr = QwandaUtils.apiGet(GennySettings.ddtUrl + "/service/cache/read/" + key, token);
			        if (resultStr == null
			        || ("<html><head><title>Error</title></head><body>Not Found</body></html>".equals(resultStr))
			        || ("<html><body><h1>Resource not found</h1></body></html>".equals(resultStr))) {
			            log.error("Count:" + count  + ", can't find key:" + key + " from cache, response:" + resultStr);
			            count--;
			        } else {
			            count = 0;
			            resultFound = true;
			        }
			    }
			    // result not found, set result to null for next if check
			    if(!resultFound)  resultStr = null;
//					if (resultStr==null) {
//						resultStr = QwandaUtils.apiGet(GennySettings.ddtUrl + "/read/" + realm + "/" + key, token);
//					}
//					resultStr =  readFromDDT(realm, key, token).toString();
			}
			if (resultStr != null) {
			    try {
			        result = jsonb.fromJson(resultStr,JsonObject.class);
			    } catch (Exception e) {
			        log.error("JsonDecode Error "+resultStr);
			    }
			} else {
			    result =  javax.json.Json.createObjectBuilder().add("status", "error").build();
			}

        }

        return result;
    }

    public JsonObject writeCachedJson(final String realm, final String key, final String value) {
        log.debug("The realm provided to writeCachedJson is :::" + realm);
        return writeCachedJson(realm, key, value, DEFAULT_TOKEN);
    }

    public JsonObject writeCachedJson(final String realm, final String key, final String value,
                                             final String token) {
        log.debug("The realm provided to writeCachedJson is :::" + realm);
        return writeCachedJson(realm, key, value, token, 0L);
    }

    public JsonObject writeCachedJson(String realm, final String key, String value, final String token,
                                             long ttl_seconds) {
        log.debug("The realm provided to writeCachedJson is :::" + realm);
        if (!GennySettings.forceCacheApi) {
           // cacheInterface.writeCache(realm, key, value, token, ttl_seconds);
            apiService.writeDataIntoCache(key, value, "Bearer "+token);
        } else {
            if (cachedEnabled) {
			    // force
			    if ("DUMMY".equals(token)) {

			    } else {
			        GennyToken temp = new GennyToken(token);
			        realm = temp.getRealm();
			        log.info("A temporal realm was provided :::" + realm + "::: realm is within the colons");
			    }
//                    if (value == null) {
//                        localCache.remove(realm + ":" + key);
//                    } else {
//                        localCache.put(realm + ":" + key, value);
//                    }
			} else {

			    log.debug("WRITING TO CACHE USING API! " + key);
			    JsonObject json =  javax.json.Json.createObjectBuilder().build();

//					json.put("key", key);
//					json.put("json", value);
//					json.put("ttl", ttl_seconds + "");
//					QwandaUtils.apiPostEntity(GennySettings.ddtUrl + "/service/cache/write/"+key, json.toString(), token);
			    apiService.writeDataIntoCache(key, value, "Bearer "+token);
			  //  QwandaUtils.apiPostEntity2(GennySettings.ddtUrl + "/service/cache/write/"+key, value, token,null);
			}
        }

        JsonObject ok =  javax.json.Json.createObjectBuilder().add("status", "ok").build();
        return ok;

    }

 

    public <T extends BaseEntity> T  readFromDDT(String realm, final String code, final boolean withAttributes,
                                                        final String token, Class clazz) {
        T be = null;

        JsonObject json = readCachedJson(realm, code, token);

        if ("ok".equals(json.getString("status"))) {
            be = (T) jsonb.fromJson(json.getString("value"), clazz);
            if (be != null)  {
                if ( be.getCode() == null) {
                    log.error("readFromDDT baseEntity for realm " +
                            realm + " has null code! json is [" + json.getString("value") + "]");
                }
                else {
                    be.setFromCache(true);
                }
            }
        } else {
            // fetch normally
            // log.info("Cache MISS for " + code + " with attributes in realm " +
            // realm);
            if (cachedEnabled) {
                log.debug("Local Cache being used.. this is NOT production");
                // force
                GennyToken temp = new GennyToken(token);
                realm = temp.getRealm();
                String ddtvalue = null;//localCache.get(realm + ":" + code);
                if (ddtvalue == null) {
                    return null;
                } else {
                    be = (T) jsonb.fromJson(ddtvalue, BaseEntity.class);
                }
            } else {

                try {
                    if (withAttributes) {
                    	apiQwandaService.getBaseEntity(code,"Bearer "+ token);
                       // be = QwandaUtils.getBaseEntityByCodeWithAttributes(code, token);
                    } 
//                    else {
//                        be = QwandaUtils.getBaseEntityByCode(code, token);
//                    }
                } catch (Exception e) {
                    // Okay, this is bad. Usually the code is not in the database but in keycloak
                    // So lets leave it to the rules to sort out... (new user)
                    log.error(
                            "BE " + code + " for realm " + realm + " is NOT IN CACHE OR DB " + e.getLocalizedMessage());
                    return null;

                }

                if (be != null)
                    writeCachedJson(realm, code, jsonb.toJson(be));
            }
            if (be!=null)
                be.setFromCache(false);
            else
                log.warn(String.format("BaseEntity: %s fetched is null", code));
        }
        return be;
    }


    public <T extends BaseEntity> T  readFromDDT(String realm, final String code, final boolean withAttributes,
                                                        final String token) {
        return readFromDDT(realm, code, withAttributes,token,BaseEntity.class);
    }

    static boolean cacheDisabled = GennySettings.noCache;

    public <T extends BaseEntity> T readFromDDT(final String realm, final String code, final String token) {
        // if ("PER_SHARONCROW66_AT_GMAILCOM".equals(code)) {
        // log.info("DEBUG");
        // }

        return readFromDDT(realm, code, true, token);

    }

    public void subscribeAdmin(final String realm, final String adminUserCode) {
        final String SUBADMIN = "SUBADMIN";
        // Subscribe to a code
        Set<String> adminSet = getSetString(realm, SUBADMIN, "ADMINS");
        adminSet.add(adminUserCode);
        putSetString(realm, SUBADMIN, "ADMINS", adminSet);
    }

    public void unsubscribeAdmin(final String realm, final String adminUserCode) {
        final String SUBADMIN = "SUBADMIN";
        // Subscribe to a code
        Set<String> adminSet = getSetString(realm, SUBADMIN, "ADMINS");
        adminSet.remove(adminUserCode);
        putSetString(realm, SUBADMIN, "ADMINS", adminSet);
    }

    public void subscribe(final String realm, final String subscriptionCode, final String userCode) {
        final String SUB = "SUB";
        // Subscribe to a code
        Set<String> subscriberSet = getSetString(realm, SUB, subscriptionCode);
        subscriberSet.add(userCode);
        putSetString(realm, SUB, subscriptionCode, subscriberSet);
    }

    public void subscribe(final String realm, final List<BaseEntity> watchList, final String userCode) {
        final String SUB = "SUB";
        // Subscribe to a code
        for (BaseEntity be : watchList) {
            Set<String> subscriberSet = getSetString(realm, SUB, be.getCode());
            subscriberSet.add(userCode);
            putSetString(realm, SUB, be.getCode(), subscriberSet);
        }
    }

    public void subscribe(final String realm, final BaseEntity be, final String userCode) {
        final String SUB = "SUB";
        // Subscribe to a code
        Set<String> subscriberSet = getSetString(realm, SUB, be.getCode());
        subscriberSet.add(userCode);
        putSetString(realm, SUB, be.getCode(), subscriberSet);

    }

    /*
     * Subscribe list of users to the be
     */
    public void subscribe(final String realm, final BaseEntity be, final String[] SubscribersCodeArray) {
        final String SUB = "SUB";
        // Subscribe to a code
        // Set<String> subscriberSet = getSetString(realm, SUB, be.getCode());
        // subscriberSet.add(userCode);
        Set<String> subscriberSet = new HashSet<String>(Arrays.asList(SubscribersCodeArray));
        putSetString(realm, SUB, be.getCode(), subscriberSet);

    }

    public void unsubscribe(final String realm, final String subscriptionCode, final Set<String> userSet) {
        final String SUB = "SUB";
        // Subscribe to a code
        Set<String> subscriberSet = getSetString(realm, SUB, subscriptionCode);
        subscriberSet.removeAll(userSet);

        putSetString(realm, SUB, subscriptionCode, subscriberSet);
    }

    public String[] getSubscribers(final String realm, final String subscriptionCode) {
        final String SUB = "SUB";
        // Subscribe to a code
        String[] resultArray = getObject(realm, SUB, subscriptionCode, String[].class);

        String[] resultAdmins = getObject(realm, "SUBADMIN", "ADMINS", String[].class);
        String[] result = (String[]) ArrayUtils.addAll(resultArray, resultAdmins);
        return result;

    }

    public void subscribeEvent(final String realm, final String subscriptionCode, final QEventMessage msg) {
        final String SUBEVT = "SUBEVT";
        // Subscribe to a code
        Set<String> subscriberSet = getSetString(realm, SUBEVT, subscriptionCode);
        subscriberSet.add(jsonb.toJson(msg));
        putSetString(realm, SUBEVT, subscriptionCode, subscriberSet);
    }

    public QEventMessage[] getSubscribedEvents(final String realm, final String subscriptionCode) {
        final String SUBEVT = "SUBEVT";
        // Subscribe to a code
        String[] resultArray = getObject(realm, SUBEVT, subscriptionCode, String[].class);
        QEventMessage[] msgs = new QEventMessage[resultArray.length];
        int i = 0;
        for (String result : resultArray) {
            msgs[i] = jsonb.fromJson(result, QEventMessage.class);
            i++;
        }
        return msgs;
    }

    public Set<String> getSetString(final String realm, final String keyPrefix, final String key) {
        String[] resultArray = getObject(realm, keyPrefix, key, String[].class);
        log.info("realm provided to getSetString is :::" + realm );
        if (resultArray == null) {
            return new HashSet<String>();
        }
        Set<String> ret = new HashSet<>();
        for (String str : resultArray) {
        	ret.add(str);
        }
        return ret;
    }

    public void putSetString(final String realm, final String keyPrefix, final String key, final Set<String> set) {
        String[] strArray = set.toArray(new String[0]);
        putObject(realm, keyPrefix, key, strArray);
    }

    public void putStringArray(final String realm, final String keyPrefix, final String key,
                                      final String[] string) {
        putObject(realm, keyPrefix, key, string);
    }

    public String[] getStringArray(final String realm, final String keyPrefix, final String key) {
        String[] resultArray = getObject(realm, keyPrefix, key, String[].class);
        if (resultArray == null) {
            return null;
        }

        return resultArray;
    }

    public void putMap(final String realm, final String keyPrefix, final String key,
                              final Map<String, String> map) {
        putObject(realm, keyPrefix, key, map);
    }

    public Map<String, String> getMap(final String realm, final String keyPrefix, final String key) {
//        Type type = new TypeToken<Map<String, String>>() {
//        }.getType();
//        
        
        String mapStr = getObject(realm, keyPrefix, key, String.class);
        Map<String, String> myMap = jsonb.fromJson(
        	    mapStr,
        	    Types.MAP_STRING_ELEMENT.getType());
//        Map<String, String> myMap = getObject(realm, keyPrefix, key, type);
        return myMap;
    }

//    public static void putMessageProducer(String sessionState, MessageProducer<JsonObject> toSessionChannel) {
//        localMessageProducerCache.put(sessionState, toSessionChannel);
//
//    }
//
//    public static MessageProducer<JsonObject> getMessageProducer(String sessionState) {
//
//        return localMessageProducerCache.get(sessionState);
//
//    }

    public void publish(BaseEntity user, String channel, Object payload) {

        publish(user, channel, payload, DEFAULT_FILTER_ARRAY);
    }

    public JsonObject publish(BaseEntity user, String channel, Object payload, final String[] filterAttributes) {

 //        eb.publish(user, channel, payload, filterAttributes);

        JsonObject ok = javax.json.Json.createObjectBuilder().add("status", "ok").build();
        return ok;

    }

    public void publish(BaseEntity user, String channel, BaseEntity be, String aliasCode, String token) {

        QDataBaseEntityMessage msg = new QDataBaseEntityMessage(be, aliasCode);
        msg.setToken(token);
  //      eb.publish(user, channel, msg, null);
    }

    public JsonObject writeMsg(String channel, Object payload) {
        JsonObject result = null;
        Set<String> rxList = new HashSet<String>();
        String token = null;

        if ((payload instanceof String)||(payload == null)) {
            if ("null".equals((String)payload)) {
                return javax.json.Json.createObjectBuilder().add("status", "error").build();
            }
        }

        if ("webdata".equals(channel) || "webcmds".equals(channel)|| "events".equals(channel)|| "data".equals(channel)) {
            // This is a standard session only
        } else {
            // This looks like we are sending data to a subscription channel

            if (payload instanceof String) {
            	 JsonObject msg = jsonb.fromJson((String)payload,JsonObject.class);
               // JsonObject msg = (JsonObject) new JsonObject((String)payload);
                log.info(msg.getValue("event_type"));
                
                JsonArray jsonArray = msg.getJsonArray("recipientCodeArray");

                jsonArray.forEach(object -> {
                	JsonValue val = (JsonValue)object;
                	rxList.add(val.toString());
//                    if (object instanceof JsonObject) {
//                        JsonObject jsonObject = (JsonObject) object;
//                        rxList.add(jsonObject.toString());
//                    } else 	 if ( instanceof String) {
//                        rxList.add((JsonValueobject.toString());
//                    }

                });

                rxList.add(channel);
                JsonArrayBuilder finalArrayBuilder = javax.json.Json.createArrayBuilder();

                for (String ch : rxList) {
                    finalArrayBuilder.add(ch);
                }
                JsonArray finalArray = finalArrayBuilder.build();
                token = msg.getString("token");
                msg.put("recipientCodeArray", finalArray);
                log.info("Writing to channels "+finalArray);
                payload = msg.toString();
                channel = "webdata";

            }
            else if  (payload instanceof QDataBaseEntityMessage) {
                QDataBaseEntityMessage msg = (QDataBaseEntityMessage) payload;
                rxList.add(channel);
                String[] rx = msg.getRecipientCodeArray();
                if (rx != null) {
                    Set<String> rx2 = Arrays.stream(rx).collect(Collectors.toSet());
                    rxList.addAll(rx2);
                }
                rx = rxList.toArray(new String[0]);
                msg.setRecipientCodeArray(rx);
                log.info("Writing to channels "+rx);
                token = msg.getToken();
                channel = "webdata";
            } else if (payload instanceof QBulkMessage) {
                QBulkMessage msg = (QBulkMessage) payload;
                rxList.add(channel);
                String[] rx = msg.getRecipientCodeArray();
                if (rx != null) {
                    Set<String> rx2 = Arrays.stream(rx).collect(Collectors.toSet());
                    rxList.addAll(rx2);
                }
                rx = rxList.toArray(new String[0]);
                msg.setRecipientCodeArray(rx);
                log.info("Writing to channels "+rx);
                token = msg.getToken();
                channel = "webdata";
                producer.getToWebData().send(((JsonObject)payload).toString());
            } else if (payload instanceof JsonObject) {
                JsonObject msg = (JsonObject) payload;
                log.info(msg.getValue("code"));
                JsonArray jsonArray = msg.getJsonArray("recipientCodeArray");

                jsonArray.forEach(object -> {
                   	JsonValue val = (JsonValue)object;
                	rxList.add(val.toString());

//                    if (object instanceof JsonObject) {
//                        JsonObject jsonObject = (JsonObject) object;
//                        rxList.add(jsonObject.toString());
//                    } else 	 if (object instanceof String) {
//                        rxList.add(object.toString());
//                    }

                });

                rxList.add(channel);
                JsonArrayBuilder finalArrayBuilder = javax.json.Json.createArrayBuilder();

                for (String ch : rxList) {
                    finalArrayBuilder.add(ch);
                }
                JsonArray finalArray = finalArrayBuilder.build();

                msg.put("recipientCodeArray", finalArray);
                log.info("Writing to channels "+finalArray);
                token = msg.getString("token");
                payload = msg;
                channel = "webdata";
                producer.getToWebData().send(((JsonObject)payload).toString());
            }

        }

        //  eb.writeMsg(channel, payload);
		if (!rxList.isEmpty()) {
		    // send ends
		    writeMsgEnd(new GennyToken(token),rxList);
		}

        result = javax.json.Json.createObjectBuilder().add("status", "ok").build();
        return result;

    }


	/*
	public JsonObject writeMsgScheduled(String channel, final String jsonMessage, BaseEntity source, String cron, final GennyToken userToken) throws IOException {

		QScheduleMessage scheduleMessage = new QScheduleMessage(jsonMessage, source.getCode(), channel, cron, userToken.getRealm());

		QwandaUtils.apiPostEntity(GennySettings.scheduleServiceUrl , jsonMessage, userToken.getToken());


	}
	 */



    public JsonObject writeMsg(String channel, BaseEntity baseentity, String aliasCode) {
        QDataBaseEntityMessage msg = new QDataBaseEntityMessage(baseentity, aliasCode);
        return writeMsg(channel, msg);
    }


    public void writeMsgEnd(GennyToken userToken) {
        QCmdMessage msgend = new QCmdMessage("END_PROCESS", "END_PROCESS");
        msgend.setToken(userToken.getToken());
        msgend.setSend(true);
        writeMsg("webcmds", msgend);
    }

    public void writeMsgEnd(GennyToken userToken,Set<String> rxSet) {
        QCmdMessage msgend = new QCmdMessage("END_PROCESS", "END_PROCESS");
        msgend.setToken(userToken.getToken());
        msgend.setSend(true);
        String[] rxArray = rxSet.toArray(new String[0]);
        msgend.setRecipientCodeArray(rxArray);
        writeMsg("project", msgend);
    }

    public QEventMessage sendEvent(final String code)
    {
        QEventMessage msg = new QEventMessage("UPDATE",code);
        writeMsg("events",msg);
        return msg;
    }


    public QEventMessage sendEvent(final String code, final String source, final String target, final GennyToken serviceToken)
    {
        QEventMessage msg = new QEventMessage("UPDATE",code);
        MessageData data = new MessageData(code);
        data.setParentCode(source);
        data.setTargetCode(target);
        msg.setData(data);
        msg.setToken(serviceToken.getToken());
        writeMsg("events",msg);
        return msg;
    }

	public static List<String> getSearchColumnFilterArray(SearchEntity searchBE)
	{
		List<String> attributeFilter = new ArrayList<String>();
		List<String> assocAttributeFilter = new ArrayList<String>();

		for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {
			String attributeCode = ea.getAttributeCode();
			if (attributeCode.startsWith("COL_") || attributeCode.startsWith("CAL_")) {
				if (attributeCode.equals("COL_PRI_ADDRESS_FULL")) {
					attributeFilter.add("PRI_ADDRESS_LATITUDE");
					attributeFilter.add("PRI_ADDRESS_LONGITUDE");
				}
				if (attributeCode.startsWith("COL__")) {
					String[] splitCode = attributeCode.substring("COL__".length()).split("__");
					assocAttributeFilter.add(splitCode[0]);
				} else {
				attributeFilter.add(attributeCode.substring("COL_".length()));
				}
			}
		}
		attributeFilter.addAll(assocAttributeFilter);
		return attributeFilter;
	}

    public Object privacyFilter(BaseEntity user, Object payload, final String[] filterAttributes) {
        if (payload instanceof QDataBaseEntityMessage) {
            return jsonb.toJson(privacyFilter(user, (QDataBaseEntityMessage) payload,
                    new HashMap<String, BaseEntity>(), filterAttributes));
        } else if (payload instanceof QBulkMessage) {
            return jsonb.toJson(privacyFilter(user, (QBulkMessage) payload, filterAttributes));
        } else
            return payload;
    }

    public QDataBaseEntityMessage privacyFilter(BaseEntity user, QDataBaseEntityMessage msg,
                                                       Map<String, BaseEntity> uniquePeople, final String[] filterAttributes) {
        ArrayList<BaseEntity> bes = new ArrayList<BaseEntity>();
        for (BaseEntity be : msg.getItems()) {
            if (uniquePeople != null && be.getCode() != null && !uniquePeople.containsKey(be.getCode())) {

                be = privacyFilter(user, be, filterAttributes);
                uniquePeople.put(be.getCode(), be);
                bes.add(be);
            } else {
                /*
                 * Avoid sending the attributes again for the same BaseEntity, so sending
                 * without attributes
                 */
                BaseEntity slimBaseEntity = new BaseEntity(be.getCode(), be.getName());
                /*
                 * Setting the links again but Adam don't want it to be send as it increasing
                 * the size of BE. Frontend should create links based on the parentCode of
                 * baseEntity not the links. This requires work in the frontend. But currently
                 * the GRP_NEW_ITEMS are being sent without any links so it doesn't show any
                 * internships.
                 */
                slimBaseEntity.setLinks(be.getLinks());
                bes.add(slimBaseEntity);
            }
        }
        msg.setItems(bes.toArray(new BaseEntity[bes.size()]));
        return msg;
    }

    public QBulkMessage privacyFilter(BaseEntity user, QBulkMessage msg, final String[] filterAttributes) {
        Map<String, BaseEntity> uniqueBes = new HashMap<String, BaseEntity>();
        for (QDataBaseEntityMessage beMsg : msg.getMessages()) {
            beMsg = privacyFilter(user, beMsg, uniqueBes, filterAttributes);
        }
        return msg;
    }

    public BaseEntity privacyFilter(BaseEntity user, BaseEntity be) {
        final String[] filterStrArray = { "PRI_FIRSTNAME", "PRI_LASTNAME", "PRI_MOBILE", "PRI_EMAIL", "PRI_PHONE",
                "PRI_IMAGE_URL", "PRI_CODE", "PRI_NAME", "PRI_USERNAME" };

        return privacyFilter(user, be, filterStrArray);
    }

    public <T extends BaseEntity> T  privacyFilter(BaseEntity user, T be, final String[] filterAttributes) {
        Set<EntityAttribute> allowedAttributes = new HashSet<EntityAttribute>();
        for (EntityAttribute entityAttribute : be.getBaseEntityAttributes()) {
            // log.info("ATTRIBUTE:"+entityAttribute.getAttributeCode()+(entityAttribute.getPrivacyFlag()?"PRIVACYFLAG=TRUE":"PRIVACYFLAG=FALSE"));
            if (( (be.getCode().startsWith("PER_")) || be.getCode().startsWith("PRJ_"))   && (!be.getCode().equals(user.getCode()))) {
                String attributeCode = entityAttribute.getAttributeCode();

                if (Arrays.stream(filterAttributes).anyMatch(x -> x.equals(attributeCode))) {

                    allowedAttributes.add(entityAttribute);
                } else {
                    if (attributeCode.startsWith("PRI_IS_")) {
                        allowedAttributes.add(entityAttribute);// allow all roles

                    } else if (attributeCode.startsWith("LNK_")) {
                        allowedAttributes.add(entityAttribute);// allow attributes that starts with "LNK_"
                    }
                }
            } else {
                if (!entityAttribute.getPrivacyFlag()) { // don't allow privacy flag attributes to get through
                    allowedAttributes.add(entityAttribute);
                }
            }
            if (entityAttribute.getAttributeCode().equals("PRI_INTERVIEW_URL")) {
                log.info("My Interview");
            }
        }
        // Handle Created and Updated attributes
        if (Arrays.asList(filterAttributes).contains("PRI_CREATED")) {
            // log.info("filterAttributes contains PRI_CREATED");
            Attribute createdAttr = new Attribute("PRI_CREATED", "Created", new DataType(LocalDateTime.class));
            EntityAttribute created = new EntityAttribute(be, createdAttr, 1.0);
            created.setValueDateTime(be.getCreated());
            allowedAttributes.add(created);// allow attributes that starts with "LNK_"
        }
        if (Arrays.asList(filterAttributes).contains("PRI_CREATED_DATE")) {
            // log.info("filterAttributes contains PRI_CREATED_DATE");
            Attribute createdAttr = new Attribute("PRI_CREATED_DATE", "Created", new DataType(LocalDate.class));
            EntityAttribute created = new EntityAttribute(be, createdAttr, 1.0);
            created.setValueDate(be.getCreated().toLocalDate());
            allowedAttributes.add(created);// allow attributes that starts with "LNK_"
        }
        if (Arrays.asList(filterAttributes).contains("PRI_UPDATED")) {
            // log.info("filterAttributes contains PRI_UPDATED");
            Attribute updatedAttr = new Attribute("PRI_UPDATED", "Updated", new DataType(LocalDateTime.class));
            EntityAttribute updated = new EntityAttribute(be, updatedAttr, 1.0);
            updated.setValueDateTime(be.getUpdated());
            allowedAttributes.add(updated);// allow attributes that starts with "LNK_"
        }
        if (Arrays.asList(filterAttributes).contains("PRI_UPDATED_DATE")) {
            // log.info("filterAttributes contains PRI_UPDATED_DATE");
            Attribute updatedAttr = new Attribute("PRI_UPDATED_DATE", "Updated", new DataType(LocalDate.class));
            EntityAttribute updated = new EntityAttribute(be, updatedAttr, 1.0);
            updated.setValueDate(be.getUpdated().toLocalDate());
            allowedAttributes.add(updated);// allow attributes that starts with "LNK_"
        }
        be.setBaseEntityAttributes(allowedAttributes);

        return be;
    }

    public BaseEntity privacyFilter(BaseEntity be, final String[] filterAttributes) {
        Set<EntityAttribute> allowedAttributes = new HashSet<EntityAttribute>();
        for (EntityAttribute entityAttribute : be.getBaseEntityAttributes()) {
            String attributeCode = entityAttribute.getAttributeCode();
            if (Arrays.stream(filterAttributes).anyMatch(x -> x.equals(attributeCode))) {
                allowedAttributes.add(entityAttribute);
            }
        }
        // Handle Created and Updated attributes
        if (Arrays.asList(filterAttributes).contains("PRI_CREATED")) {
            // log.info("filterAttributes contains PRI_CREATED");
            Attribute createdAttr = new Attribute("PRI_CREATED", "Created", new DataType(LocalDateTime.class));
            EntityAttribute created = new EntityAttribute(be, createdAttr, 1.0);
            created.setValueDateTime(be.getCreated());
            allowedAttributes.add(created);// allow attributes that starts with "LNK_"
        }
        if (Arrays.asList(filterAttributes).contains("PRI_CREATED_DATE")) {
            // log.info("filterAttributes contains PRI_CREATED_DATE");
            Attribute createdAttr = new Attribute("PRI_CREATED_DATE", "Created", new DataType(LocalDate.class));
            EntityAttribute created = new EntityAttribute(be, createdAttr, 1.0);
            created.setValueDate(be.getCreated().toLocalDate());
            allowedAttributes.add(created);// allow attributes that starts with "LNK_"
        }
        if (Arrays.asList(filterAttributes).contains("PRI_UPDATED")) {
            // log.info("filterAttributes contains PRI_UPDATED");
            Attribute updatedAttr = new Attribute("PRI_UPDATED", "Updated", new DataType(LocalDateTime.class));
            EntityAttribute updated = new EntityAttribute(be, updatedAttr, 1.0);
            updated.setValueDateTime(be.getUpdated());
            allowedAttributes.add(updated);// allow attributes that starts with "LNK_"
        }
        if (Arrays.asList(filterAttributes).contains("PRI_UPDATED_DATE")) {
            // log.info("filterAttributes contains PRI_UPDATED_DATE");
            Attribute updatedAttr = new Attribute("PRI_UPDATED_DATE", "Updated", new DataType(LocalDate.class));
            EntityAttribute updated = new EntityAttribute(be, updatedAttr, 1.0);
            updated.setValueDate(be.getUpdated().toLocalDate());
            allowedAttributes.add(updated);// allow attributes that starts with "LNK_"
        }
        be.setBaseEntityAttributes(allowedAttributes);

        return be;
    }

    public static Boolean checkIfAttributeValueContainsString(BaseEntity baseentity, String attributeCode,
                                                              String checkIfPresentStr) {

        Boolean isContainsValue = false;

        if (baseentity != null && attributeCode != null && checkIfPresentStr != null) {
            String attributeValue = baseentity.getValue(attributeCode, null);

            if (attributeValue != null && attributeValue.toLowerCase().contains(checkIfPresentStr.toLowerCase())) {
                return true;
            }
        }

        return isContainsValue;
    }

    public static String apiPostEntity(final String postUrl, final String entityString, final String authToken,
                                       final Consumer<String> callback) throws IOException {
        {
            String responseString = "ok";

            return responseString;
        }
    }

    public static String apiPostEntity(final String postUrl, final String entityString, final String authToken)
            throws IOException {
        {
            return apiPostEntity(postUrl, entityString, authToken, null);
        }
    }

    public Set<String> fetchRealmsFromApi() {
        List<String> activeRealms = new ArrayList<String>();
      //  JsonObject ar = jsonb.fromJson(apiUtilsService.getRealms(), "Bearer "+serviceToken.getToken()),JsonObject.class);
      JsonObject ar = readCachedJson(GennySettings.GENNY_REALM, "REALMS");
        String ars = ar.getString("value");

        if (ars == null) {
            ars = apiUtilsService.getRealms("NOT REQUIRED");
            // ars = QwandaUtils.apiGet(GennySettings.qwandaServiceUrl + "/utils/realms", "NOTREQUIRED");
        }

//        Type listType = new TypeToken<List<String>>() {
//        }.getType();
        ars = ars.replaceAll("\\\"", "\"");
        
        activeRealms = jsonb.fromJson(
        	    ars,
        	    Types.LIST_ELEMENT.getType());

//        activeRealms = jsonb.fromJson(ars, listType);
        Set<String> realms = new HashSet<>(activeRealms);
        return realms;
    }

    public String fixJson(String resultStr)
    {
        String resultStr2 = resultStr.replaceAll(Pattern.quote("\\\""),
                Matcher.quoteReplacement("\""));
        String resultStr3 = resultStr2.replaceAll(Pattern.quote("\\n"),
                Matcher.quoteReplacement("\n"));
        String resultStr4 = resultStr3.replaceAll(Pattern.quote("\\\n"),
                Matcher.quoteReplacement("\n"));
//		String resultStr5 = resultStr4.replaceAll(Pattern.quote("\"{"),
//				Matcher.quoteReplacement("{"));
//		String resultStr6 = resultStr5.replaceAll(Pattern.quote("\"["),
//				Matcher.quoteReplacement("["));
//		String resultStr7 = resultStr6.replaceAll(Pattern.quote("]\""),
//				Matcher.quoteReplacement("]"));
//		String resultStr8 = resultStr5.replaceAll(Pattern.quote("}\""), Matcher.quoteReplacement("}"));
        String ret = resultStr4.replaceAll(Pattern.quote("\\\""
                        + ""),
                Matcher.quoteReplacement("\""));
        return ret;

    }


    public void sendFeedbackError(GennyToken userToken, Answer answer, String message)
    {
        sendFeedback(userToken,answer,"ERROR",message);
    }

    public void sendFeedbackWarning(GennyToken userToken, Answer answer, String message)
    {
        sendFeedback(userToken,answer,"WARN",message);
    }
    public void sendFeedbackSuspicious(GennyToken userToken, Answer answer, String message)
    {
        sendFeedback(userToken,answer,"SUSPICIOUS",message);
    }
    public void sendFeedbackHint(GennyToken userToken, Answer answer, String message)
    {
        sendFeedback(userToken,answer,"HINT",message);
    }




    public void sendFeedback(GennyToken userToken, Answer answer, String prefix, String message)
    {
        // find the baseentity
        BaseEntity be = getObject(userToken.getRealm(), "", answer.getTargetCode(), BaseEntity.class,
                userToken.getToken());

        BaseEntity sendBe = new BaseEntity(be.getCode(),be.getName());
        sendBe.setRealm(userToken.getRealm());
        try {
            Attribute att = getAttribute(answer.getAttributeCode(), userToken.getToken());
            sendBe.addAttribute(att);
            sendBe.setValue(att, answer.getValue());
            Optional<EntityAttribute> ea =sendBe.findEntityAttribute(answer.getAttributeCode());
            if (ea.isPresent()) {
                ea.get().setFeedback(prefix+":"+message);
                QDataBaseEntityMessage msg = new QDataBaseEntityMessage(sendBe);
                msg.setReplace(true);
                msg.setToken(userToken.getToken());
                writeMsg("webcmds", msg);
            }
        } catch (BadDataException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void sendToFrontEnd(GennyToken userToken, Answer... answers) {
        if ((answers.length > 0)) {
            // find the baseentity
            BaseEntity be = getObject(userToken.getRealm(), "", answers[0].getTargetCode(), BaseEntity.class,
                    userToken.getToken());
            if (be != null) {

                BaseEntity newBe = new BaseEntity(be.getCode(), be.getName());
                newBe.setRealm(userToken.getRealm());

                for (Answer answer : answers) {

                    try {
                        Attribute att = getAttribute(answer.getAttributeCode(), userToken.getToken());
                        newBe.addAttribute(att);
                        newBe.setValue(att, answer.getValue());
                    } catch (BadDataException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                QDataBaseEntityMessage msg = new QDataBaseEntityMessage(newBe);
                msg.setReplace(true);
                msg.setToken(userToken.getToken());
                writeMsg("webcmds", msg);
            }
        }

    }

	public void sendCmdMsg(BaseEntityUtils beUtils, String msgType, String code) 
	{
		sendCmdMsg(beUtils, msgType, code, null, null);
	}

	public void sendCmdMsg(BaseEntityUtils beUtils, String msgType, String code, List<String> targetCodes) 
	{
		sendCmdMsg(beUtils, msgType, code, null, targetCodes);
	}

	public void sendCmdMsg(BaseEntityUtils beUtils, String msgType, String code, String message) 
	{
		sendCmdMsg(beUtils, msgType, code, message, null);
	}

	public void sendCmdMsg(BaseEntityUtils beUtils, String msgType, String code, String message, List<String> targetCodes) 
	{
		QCmdMessage msg = new QCmdMessage(msgType, code);
		msg.setToken(beUtils.getGennyToken().getToken());
		msg.setSend(true);  		
		if (message != null) {
			msg.setMessage(message);
		}
		if (targetCodes != null) {
			msg.setTargetCodes(targetCodes);
		}
		writeMsg("webcmds",msg);
	}

	public void sendAskMsg(BaseEntityUtils beUtils, Ask ask) 
	{
		QDataAskMessage msg = new QDataAskMessage(ask);
		msg.setToken(beUtils.getGennyToken().getToken());
		msg.setReplace(true);
		writeMsg("webcmds", jsonb.toJson(msg));
	}

	public void sendBaseEntityMsg(BaseEntityUtils beUtils, BaseEntity be) 
	{
		QDataBaseEntityMessage msg = new QDataBaseEntityMessage(be);
		msg.setToken(beUtils.getGennyToken().getToken());
		msg.setReplace(true);
		writeMsg("webcmds", jsonb.toJson(msg));
	}

	public void sendBaseEntityMsg(BaseEntityUtils beUtils, BaseEntity[] beArray) 
	{
		QDataBaseEntityMessage msg = new QDataBaseEntityMessage(beArray);
		msg.setToken(beUtils.getGennyToken().getToken());
		msg.setReplace(true);
		writeMsg("webcmds", jsonb.toJson(msg));
	}

	public JsonObject fetchDataFromCache(final String code, final GennyToken token) {
		String data = null;
		String value = null;
		try {
			data = apiService.getDataFromCache(token.getRealm(), code, "Bearer " + token.getToken());
			JsonObject json = jsonb.fromJson(data, JsonObject.class);
			if ("ok".equalsIgnoreCase(json.getString("status"))) {
				value = json.getString("value");
				// log.info(value);
			}
		} catch (Exception e) {
			log.error("Failed to read cache for data" + code + ", exception:" + e.getMessage());
			e.printStackTrace();
		}
		return jsonb.fromJson(value, JsonObject.class);
	}

	public BaseEntity fetchBaseEntityFromCache(final String code, final GennyToken token) {
		String data = null;
		String value = null;

		data = apiService.getDataFromCache(token.getRealm(), code, "Bearer " + token.getToken());
		JsonObject json = jsonb.fromJson(data, JsonObject.class);
		if ("ok".equalsIgnoreCase(json.getString("status"))) {
			value = json.getString("value");
			// log.info(value);
			return jsonb.fromJson(value, BaseEntity.class);
		}
		return null;
	}
	
    public Attribute getAttribute(final String attributeCode, final String token) {
    	GennyToken gennyToken = new GennyToken(token);
    	return getAttribute(attributeCode, gennyToken);
    }
    
    public Attribute getAttribute(final String attributeCode, final GennyToken gennyToken) {
    	String realm = gennyToken.getRealm();
    	if (!realmAttributeMap.containsKey(realm)) {
    		loadAllAttributesIntoCache(gennyToken);
    	}
        Attribute ret = realmAttributeMap.get(gennyToken.getRealm()).get(attributeCode);
        if (ret == null) {
            if (attributeCode.startsWith("SRT_") || attributeCode.startsWith("RAW_")) {
                ret = new AttributeText(attributeCode, attributeCode);
            } else {
                loadAllAttributesIntoCache(gennyToken);
                ret = realmAttributeMap.get(gennyToken.getRealm()).get(attributeCode);
                if (ret == null) {
                    log.error("Attribute NOT FOUND :"+realm+":"+attributeCode);
                }
            }
        }
        return ret;
    }
    
    public QDataAttributeMessage loadAllAttributesIntoCache(final GennyToken token) {
        try {
            boolean cacheWorked = false;
            String realm = token.getRealm();
            log.info("All the attributes about to become loaded ... for realm "+realm);
                 log.info("LOADING ATTRIBUTES FROM API");
                String jsonString = apiQwandaService.getAttributes("Bearer " + token.getToken());
                if (!StringUtils.isBlank(jsonString)) {
 
                    attributesMsg = jsonb.fromJson(jsonString, QDataAttributeMessage.class);
                    Attribute[] attributeArray = attributesMsg.getItems();

                    if (!realmAttributeMap.containsKey(realm)) {
                    	realmAttributeMap.put(realm, new ConcurrentHashMap<String,Attribute>());
                    }
                    Map<String,Attribute> attributeMap = realmAttributeMap.get(realm);
      
                    for (Attribute attribute : attributeArray) {
                        attributeMap.put(attribute.getCode(), attribute);
                    }
                   // realmAttributeMap.put(realm, attributeMap);
                    
                    log.info("All the attributes have been loaded from api in " + attributeMap.size() + " attributes");
                } else {
                    log.error("NO ATTRIBUTES LOADED FROM API");
                }


            return attributesMsg;
        } catch (Exception e) {
            log.error("Attributes API not available, exception:" + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }
    public QDataAttributeMessage loadAllAttributesIntoCache(final String token) {
        return loadAllAttributesIntoCache(new GennyToken(token));
    }
}
