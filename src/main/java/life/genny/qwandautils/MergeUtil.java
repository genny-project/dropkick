package life.genny.qwandautils;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import org.acme.kafka.streams.aggregator.model.ApiQwandaService;
import org.acme.kafka.streams.aggregator.model.ApiService;
import org.acme.kafka.streams.aggregator.streams.TopologyProducer;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import life.genny.models.GennyToken;
import life.genny.qwanda.Link;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.utils.BaseEntityUtils;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@RegisterForReflection
@ApplicationScoped
public class MergeUtil {
	
	private static final Logger log = Logger.getLogger(MergeUtil.class);

	public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_RED = "\u001B[31m";
	
    /* [[VARIABLENAME.ATTRIBUTE]] pattern */
    /* Used for baseentity-attribute merging */
	public static final String REGEX_START = "[[";
	public static final String REGEX_END = "]]";
	public static final String REGEX_START_PATTERN = Pattern.quote(REGEX_START);
    public static final String REGEX_END_PATTERN = Pattern.quote(REGEX_END);
    public static final Pattern PATTERN = Pattern.compile(REGEX_START_PATTERN + "(?s)(.*?)" + REGEX_END_PATTERN);
    public static final String DEFAULT = "";
    public static final String PATTERN_BASEENTITY = REGEX_START_PATTERN + "(?s)(.*?)" + REGEX_END_PATTERN;
    public static final Pattern PATTERN_MATCHER = Pattern.compile(PATTERN_BASEENTITY);
    
    /* {{VARIABLE}} pattern */
    /* this is for direct merging */
    public static final String VARIABLE_REGEX_START = "{{";
    public static final String VARIABLE_REGEX_END = "}}";
    public static final Pattern PATTERN_VARIABLE = Pattern.compile(Pattern.quote(VARIABLE_REGEX_START) + "(?s)(.*?)" + Pattern.quote(VARIABLE_REGEX_END));  
    
    /* ((FORMAT)) pattern */
    /* this is for formatting data such as dates or strings */
    public static final String FORMAT_VARIABLE_REGEX_START = "((";
    public static final String FORMAT_VARIABLE_REGEX_END = "))";
    public static final Pattern FORMAT_PATTERN_VARIABLE = Pattern.compile(Pattern.quote(FORMAT_VARIABLE_REGEX_START) + "(.*)" + Pattern.quote(FORMAT_VARIABLE_REGEX_END));  
    
	static Jsonb jsonb = JsonbBuilder.create();
	
	@Inject
	@RestClient
	ApiService apiService;

	
	@Inject
	@RestClient
	ApiQwandaService apiQwandaService;

    
	public String merge(String mergeStr, Map<String, Object> templateEntityMap) { 
		
		/* matching [OBJECT.ATTRIBUTE] patterns */
		if (mergeStr != null) {

			Matcher match = PATTERN_MATCHER.matcher(mergeStr);
			Matcher matchVariables = PATTERN_VARIABLE.matcher(mergeStr);
		
			if (templateEntityMap != null && templateEntityMap.size() > 0) {
				
				while (match.find()) {	
					
					Object mergedObject = wordMerge(match.group(1), templateEntityMap);
					if (mergedObject != null) {
						log.info("mergedObject = " + mergedObject);
						mergeStr = mergeStr.replace(REGEX_START + match.group(1) + REGEX_END, mergedObject.toString());
					} else {
						mergeStr = mergeStr.replace(REGEX_START + match.group(1) + REGEX_END, "");
					}			
				}
				
				/* duplicating this for now. ideally wordMerge should be a bit more flexible and allows all kind of data to be passed */
				while (matchVariables.find()) {
					
					Object mergedText = templateEntityMap.get(matchVariables.group(1));
					if (mergedText != null) {
						mergeStr = mergeStr.replace(VARIABLE_REGEX_START + matchVariables.group(1) + VARIABLE_REGEX_END, mergedText.toString());
					} else {
						mergeStr = mergeStr.replace(VARIABLE_REGEX_START + matchVariables.group(1) + VARIABLE_REGEX_END, "");
					}	
				}
				
			}

		} else {
			log.warn("mergeStr is NULL");
		}
	
		return mergeStr;
	}
	
	@SuppressWarnings("unused")
	public Object wordMerge(String mergeText, Map<String, Object> entitymap) {
		
		if(mergeText != null && !mergeText.isEmpty()) {
			
			try {
				
				/* we split the text to merge into 2 components: BE.PRI... becomes [BE, PRI...] */
				String[] entityArr = mergeText.split("\\.");
				String keyCode = entityArr[0];
				log.debug("looking for key in map: " + keyCode);
				
				if((entityArr.length == 0))
					return DEFAULT;
				
				if(entitymap.containsKey(keyCode)) {
					
					Object value = entitymap.get(keyCode);

					if (value == null) {
						log.info("value is NULL for key " + keyCode);
					} else {
						
						if (value.getClass().equals(BaseEntity.class)) {
							
							BaseEntity be = (BaseEntity)value;
							String attributeCode = entityArr[1];

							Object attributeValue = be.getValue(attributeCode, null);
							log.info("context: " + keyCode + ", attr: " + attributeCode + ", value: " + attributeValue);


							Matcher matchFormat = null;
							if (entityArr != null && entityArr.length > 2) {
								matchFormat = FORMAT_PATTERN_VARIABLE.matcher(entityArr[2]);
							}
							
							if (attributeValue instanceof java.time.LocalDateTime) {
								/* If the date-related mergeString needs to formatter to a particultar format -> we split the date-time related merge text to merge into 3 components: BE.PRI.TimeDateformat... becomes [BE, PRI...] */
								/* 1st component -> BaseEntity code ; 2nd component -> attribute code ; 3rd component -> (date-Format) */
								if (matchFormat != null && matchFormat.find()) {
									log.info("This datetime attribute code ::"+attributeCode+ " needs to be formatted and the format is ::"+entityArr[2]);
										return getFormattedDateTimeString((LocalDateTime) attributeValue, matchFormat.group(1));
								} else {
									log.info("This DateTime attribute code ::"+attributeCode+ " needs no formatting");
									return (LocalDateTime) attributeValue;
								}

							} else if (attributeValue instanceof java.time.LocalDate) {

								if (matchFormat != null && matchFormat.find()) {
									log.info("This date attribute code ::"+attributeCode+ " needs to be formatted and the format is ::"+entityArr[2]);
									return getFormattedDateString((LocalDate) attributeValue, matchFormat.group(1));
								} else {
									log.info("This Date attribute code ::"+attributeCode+ " needs no formatting");
									return (LocalDate) attributeValue;
								}

							} else if (attributeValue instanceof java.lang.String){
								String result = null;
								if (matchFormat != null && matchFormat.find()) {
									result  =  getFormattedString((String) attributeValue, matchFormat.group(1));
									log.info("This String attribute code ::" + attributeCode
									+ " needs to be formatted " + "and the format is ::" + entityArr[2]
									+ ", result is:" + result);
								} else {
									result =  getBaseEntityAttrValueAsString(be, attributeCode);
									log.info("This String attribute code ::" + attributeCode
									+ " needs no formatting, result is:" + result);
								}
								return result;
							} else if (attributeValue instanceof java.lang.Boolean) {
								return (Boolean) attributeValue;
							} else if (attributeValue instanceof java.lang.Integer) {
								return (Integer) attributeValue;
							} else if (attributeValue instanceof java.lang.Long) {
								return (Long) attributeValue;
							} else if (attributeValue instanceof java.lang.Double) {
								return (Double) attributeValue;
							} else {
								return getBaseEntityAttrValueAsString(be, attributeCode);
							}
							
						} else if (value.getClass().equals(String.class)) {
							return (String) value;
						}
					}
				}

			} catch (Exception e) {
				log.error("ERROR",e);
			}
		
		}
		
		return DEFAULT;	
	}


	/**
	 * Check to see if all contexts are present
	 */
	public Boolean contextsArePresent(String mergeStr, Map<String, Object> templateEntityMap) { 
		
		/* matching [OBJECT.ATTRIBUTE] patterns */
		if (mergeStr != null) {

			Matcher match = PATTERN_MATCHER.matcher(mergeStr);
			Matcher matchVariables = PATTERN_VARIABLE.matcher(mergeStr);
		
			if(templateEntityMap != null && templateEntityMap.size() > 0) {
				
				while (match.find()) {
					
					Object mergedObject = wordMerge(match.group(1), templateEntityMap);
					if (mergedObject == null || mergedObject.toString().isEmpty()) {
						return false;
					}			
				}
				
				while(matchVariables.find()) {
					
					Object mergedText = templateEntityMap.get(matchVariables.group(1));
					if (mergedText == null) {
						return false;
					}	
				}
				
			}

		} else {
			log.warn("mergeStr is NULL");
		}
		return true;
	}

	public Boolean requiresMerging(String mergeStr) {

		if (mergeStr == null) {
			log.warn("mergeStr is NULL");
			return null;
		}

		Matcher match = PATTERN_MATCHER.matcher(mergeStr);
		Matcher matchVariables = PATTERN_VARIABLE.matcher(mergeStr);

		return (match.find() || matchVariables.find());
	}

	
	/**
	 * 
	 * @param baseEntAttributeCode
	 * @param token
	 * @return Deserialized BaseEntity model object with values for a BaseEntity code that is passed
	 */
	public BaseEntity getBaseEntityForAttr(String baseEntAttributeCode, String token) {
		
		String qwandaServiceUrl = System.getenv("REACT_APP_QWANDA_API_URL");
		String attributeString = null;
		BaseEntity be = null;
		apiQwandaService.getBaseEntity(baseEntAttributeCode,"Bearer "+ token);
//			attributeString = QwandaUtils
//					.apiGet(qwandaServiceUrl + "/qwanda/baseentitys/" +baseEntAttributeCode, token);
		be = jsonb.fromJson(attributeString, BaseEntity.class);
		
		
		return be;
	}
	
	/**
	 * 
	 * @param BaseEntity object
	 * @param attributeCode
	 * @return The attribute value for the BaseEntity attribute code passed
	 */
	public String getBaseEntityAttrValueAsString(BaseEntity be, String attributeCode) {
		
		String attributeVal = null;
		for(EntityAttribute ea : be.getBaseEntityAttributes()) {
			if(ea.getAttributeCode().equals(attributeCode)) {
				attributeVal = ea.getObjectAsString();
			}
		}
		
		return attributeVal;
	}
	
	
	/**
	 * 
	 * @param baseEntityCode
	 * @param attributeCode
	 * @param token
	 * @return attribute value
	 */
	public String getAttrValue(String baseEntityCode, String attributeCode, String token) {
		
		String attrValue = null;
		
		if(baseEntityCode != null && token != null) {
			
			BaseEntity be = getBaseEntityForAttr(baseEntityCode, token);
			attrValue = getBaseEntityAttrValueAsString(be, attributeCode);			
		}
		
		return attrValue;
	}
	
	public String getFullName(String baseEntityCode, String token) {
		
		String fullName = null;

		if(baseEntityCode != null && token != null) {

			String firstName = getAttrValue(baseEntityCode, "PRI_FIRSTNAME", token);
			String lastName = getAttrValue(baseEntityCode, "PRI_LASTNAME", token);
			fullName = firstName + " " + lastName;
			log.info("PRI_FULLNAME   ::   "+ fullName);		
		}
		
		return fullName;
	}

	public boolean createBaseEntity(String sourceCode, String linkCode, String targetCode, String name, Long id, String token) {
		
		BaseEntity be = new BaseEntity(targetCode, name);
		String qwandaServiceUrl = System.getenv("REACT_APP_QWANDA_API_URL");
		
	      
        String jsonBE = jsonb.toJson(be);
        try {
        	String output = apiQwandaService.createBaseEntity(jsonBE, token);
//            String output= apiPostEntity2(qwandaServiceUrl + "/qwanda/baseentitys", jsonBE, token,null);
            
        	String ee = apiQwandaService.createEntityEntity(jsonb.toJson(new Link(sourceCode, targetCode, linkCode)), token);
 //           apiPostEntity2(qwandaServiceUrl + "/qwanda/entityentitys", jsonb.toJson(new Link(sourceCode, targetCode, linkCode)),token,null);
            log.info("this is the output :: "+ output);
            
        }catch (Exception e) {
            log.error("ERROR",e);
        }
		
		return true;
		
	}
	
	public static Object getBaseEntityAttrObjectValue(BaseEntity be, String attributeCode) {

		Object attributeVal = null;
		for (EntityAttribute ea : be.getBaseEntityAttributes()) {
			if (ea.getAttributeCode().equals(attributeCode)) {
				attributeVal = ea.getObject();
			}
		}

		return attributeVal;

	}
	
	public static String getFormattedDateTimeString(LocalDateTime dateToBeFormatted, String format) {
		if(dateToBeFormatted != null && format != null) {
			DateTimeFormatter dateformat = DateTimeFormatter.ofPattern(format);
			return dateToBeFormatted.format(dateformat);
		}
		return null;
	}

	public static String getFormattedDateString(LocalDate dateToBeFormatted, String format) {
		if(dateToBeFormatted != null && format != null) {
			DateTimeFormatter dateformat = DateTimeFormatter.ofPattern(format);
			return dateToBeFormatted.format(dateformat);
		}
		return null;
	}

	/**
	 * This is a utility used to format strings during merging.
	 * Feel free to add some cool little string formatting tools below.
	 *
	 * Author - Jasper Robison (27/07/21)
	 *
	 * @param	stringToBeFormatted		The string we want to format
	 * @param	format					how it should be formatted (can be dot seperated string for multiple)
	 *
	 * @return	The formatted string.
	 */
	public static String getFormattedString(String stringToBeFormatted, String format) {

		if (stringToBeFormatted != null && format != null) {
			String[] formatCommands = format.split("\\.");

			for (String cmd : formatCommands) {
				// A nice little clean up command for attribute values
				if (cmd.equals("CLEAN")) {
					stringToBeFormatted = stringToBeFormatted.replace("\"", "").replace("[", "").replace("]", "").replace(" ", "");
				}
				// A nice little substring command
				if (cmd.startsWith("SUBSTRING(")) {
					String[] subStringField = cmd.replace("SUBSTRING", "").replace("(", "").replace(")", "").replace(" ", "").split(",");
					Integer begin = Integer.valueOf(subStringField[0]);
					Integer end = subStringField.length > 1 ? Integer.valueOf(subStringField[1]) : null;
					if (end != null) {
						stringToBeFormatted = stringToBeFormatted.substring(begin, end);
					} else {
						stringToBeFormatted = stringToBeFormatted.substring(begin);
					}
				}
			}
			return stringToBeFormatted;
		}
		return null;
	}

	/**
	 * This method is used to find any associated contexts. 
	 * This allows us to provide a default set of context associations
	 * that the system can fetch for us in order to reduce code in rules 
	 * and other areas.
	 *
	 * Author - Jasper Robison (30/09/2021)
	 *
	 * @param	beUtils					Standard genny utility
	 * @param	ctxMap					the context map to add to, and fetch associations from.
	 * @param	contextAssociationJson	the json instructions for fetching associations.
	 */
//	public void addAssociatedContexts(BaseEntityUtils beUtils,HashMap<String, Object> ctxMap, String contextAssociationJson) {
//
//		// Enter Try-Catch for better error logging
//		try {
//
//			// TODO: Make sure dependant contexts are handled in correct order automatically
//
//			// convert to JsonObject for easier processing
//			JsonObject ctxAssocJson = new JsonObject(contextAssociationJson);
//			JsonArray ctxAssocArray = ctxAssocJson.getJsonArray("associations");
//
//			for (Object obj : ctxAssocArray) {
//
//				JsonObject ctxAssoc = (JsonObject) obj;
//
//				// These are the required params in the json
//				String code = ctxAssoc.getString("code");
//				String parent = ctxAssoc.getString("parent");
//				String attributeCode = ctxAssoc.getString("attributeCode");
//
//				// Grab Parent from map so we can fetch associated entity
//				BaseEntity parentBE = (BaseEntity) ctxMap.get(parent);
//				BaseEntity assocBE = beUtils.getBaseEntityFromLNKAttr(parentBE, attributeCode);
//
//				// Add the context map if associated be is found
//				if (assocBE != null) {
//					ctxMap.put(code, assocBE);
//				} else {
//					log.error(ANSIColour.RED+"Associated BE not found for " + parent + "." + attributeCode+ANSIColour.RESET);
//				}
//
//			}
//		} catch (Exception e) {
//			log.error(ANSIColour.RED+"Something is wrong with the context association JSON!!!!!"+ANSIColour.RESET);
//		}
//	}

	private static final ExecutorService executorService = Executors.newFixedThreadPool(20);

	private static HttpClient httpClient = HttpClient.newBuilder().executor(executorService)
			.version(HttpClient.Version.HTTP_2).connectTimeout(Duration.ofSeconds(20)).build();

//	public static String apiPostEntity2(final String postUrl, final String entityString, final String authToken,
//			final Consumer<String> callback) throws IOException {
//
//		Integer httpTimeout = 7; // 7 seconds
//		
//		log.info("fetching from "+postUrl);
//
//		if (StringUtils.isBlank(postUrl)) {
//			log.error("Blank url in apiPostEntity");
//		}
//
//		BodyPublisher requestBody = BodyPublishers.ofString(entityString);
//
//		HttpRequest.Builder requestBuilder = HttpRequest.newBuilder().POST(requestBody).uri(URI.create(postUrl))
//				.setHeader("Content-Type", "application/json").setHeader("Authorization", "Bearer " + authToken);
//
//		if (postUrl.contains("genny.life")) { // Hack for local server not having http2
//			requestBuilder = requestBuilder.version(HttpClient.Version.HTTP_1_1);
//		}
//
//		HttpRequest request = requestBuilder.build();
//
//		String result = null;
//		Boolean done = false;
//		int count = 5;
//		while ((!done) && (count > 0)) {
////			httpClient = HttpClient.newBuilder().executor(executorService).version(HttpClient.Version.HTTP_2)
////					.connectTimeout(Duration.ofSeconds(httpTimeout)).build();
//			CompletableFuture<java.net.http.HttpResponse<String>> response = httpClient.sendAsync(request,
//					java.net.http.HttpResponse.BodyHandlers.ofString());
//
//			try {
//				result = response.thenApply(java.net.http.HttpResponse::body).get(httpTimeout, TimeUnit.SECONDS);
//				done = true;
//			} catch (InterruptedException | ExecutionException | TimeoutException e) {
//				// TODO Auto-generated catch block
//				log.error("Count:" + count + ", Exception occurred when post to URL: " + postUrl
//						+ ",Body is entityString:" + entityString + ", Exception details:" + e.getMessage());
//				// try renewing the httpclient
//				if (count <= 0) {
//					done = true;
//				}
//			}
//			count--;
//		}
//
//
//		return result;
//
//
//	}
	
	
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

}
