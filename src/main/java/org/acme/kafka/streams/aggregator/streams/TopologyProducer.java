package org.acme.kafka.streams.aggregator.streams;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.net.ssl.HttpsURLConnection;

import org.acme.kafka.streams.aggregator.model.ApiBridgeService;
import org.acme.kafka.streams.aggregator.model.ApiQwandaService;
import org.acme.kafka.streams.aggregator.model.ApiService;
import org.acme.kafka.streams.aggregator.model.Attribute2;
import org.acme.kafka.streams.aggregator.model.KeycloakService;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import life.genny.models.GennyToken;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeText;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.SearchEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.message.QDataBaseEntityMessage;
import life.genny.qwandautils.MergeUtil;
import life.genny.utils.DefUtils;

@ApplicationScoped
public class TopologyProducer {

	private static final Logger log = Logger.getLogger(TopologyProducer.class);

	@Inject
	InternalProducer producer;

	@Inject
	MergeUtil mergeUtil;

	@Inject
	DefUtils defUtils;

	@Inject
	@RestClient
	ApiService apiService;

	@Inject
	@RestClient
	ApiQwandaService apiQwandaService;

	@Inject
	@RestClient
	ApiBridgeService apiBridgeService;

	@Inject
	@RestClient
	KeycloakService keycloakService;

	@ConfigProperty(name = "genny.show.values", defaultValue = "false")
	Boolean showValues;

	@ConfigProperty(name = "genny.keycloak.url", defaultValue = "https://keycloak.gada.io")
	String baseKeycloakUrl;

	@ConfigProperty(name = "genny.keycloak.realm", defaultValue = "genny")
	String keycloakRealm;

	@ConfigProperty(name = "genny.service.username", defaultValue = "service")
	String serviceUsername;

	@ConfigProperty(name = "genny.service.password", defaultValue = "password")
	String servicePassword;

	@ConfigProperty(name = "genny.oidc.auth-server-url", defaultValue = "https://keycloak.genny.life/auth/realms/genny")
	String keycloakUrl;

	@ConfigProperty(name = "genny.oidc.client-id", defaultValue = "backend")
	String clientId;

	@ConfigProperty(name = "genny.oidc.credentials.secret", defaultValue = "secret")
	String secret;

	@ConfigProperty(name = "genny.api.url", defaultValue = "http://alyson.genny.life:8280")
	String apiUrl;

	@ConfigProperty(name = "genny.default.dropdown.size", defaultValue = "25")
	Integer defaultDropDownSize;

	GennyToken serviceToken;

	Jsonb jsonb = JsonbBuilder.create();

	final Serde<String> stringSerde = Serdes.String();
	final Serde<byte[]> byteArraySerde = Serdes.ByteArray();

	static final String WEATHER_STATIONS_STORE = "weather-stations-store";
	static final String ATTRIBUTES_STORE = "attributes-store";

	static final String DATA_TOPIC = "data";
	static final String EVENTS_TOPIC = "events";
	static final String WEBCMDS_TOPIC = "webcmds";
	static final String WEBDATA_TOPIC = "webdata";
	static final String BLACKLIST_TOPIC = "blacklist";

	public Boolean checkDropDown(String data) {
		Boolean valid = false;

		JsonObject json = jsonb.fromJson(data, JsonObject.class);
		if (json.containsKey("event_type")) {
			String eventType = json.getString("event_type");
			if ("DD".equals(eventType)) {
				JsonObject dataJson = json.getJsonObject("data");
				String sourceCode = dataJson.getString("sourceCode");
				String attributeCode = json.getString("attributeCode");
				String token = json.getString("token");
				BaseEntity sourceBe = fetchBaseEntityFromCache(sourceCode, serviceToken);
				if (sourceBe != null) {
					// Check Target exist
					String targetCode = dataJson.getString("targetCode");
					BaseEntity targetBe = fetchBaseEntityFromCache(targetCode, serviceToken);
					if (targetBe != null) {
						BaseEntity defBe = defUtils.getDEF(targetBe, serviceToken);
						Boolean defDropdownExists = false;
						/*
						 * Determine whether there is a DEF attribute and target type that has a new DEF
						 * search for this combination
						 */
						try {
							defDropdownExists = hasDropdown(attributeCode, defBe);
							if (defDropdownExists) {
								log.info("Dropdown for " + attributeCode + " exists");
								String searchText = dataJson.getString("value");
								String parentCode = dataJson.getString("parentCode");
								String questionCode = dataJson.getString("questionCode");
								log.info(attributeCode + ":" + parentCode + ":[" + searchText + "]");
								// QDataBaseEntityMessage msg = getDropdownData(beUtils,message,dropdownSize);
								QDataBaseEntityMessage msg = getDropdownData(token, defBe, sourceBe, targetBe,
										attributeCode, parentCode, questionCode, searchText, 15);
								String jsonStr = null;
								
								try {
									jsonStr = jsonb.toJson(msg);
									producer.getToWebCmds().send(jsonStr);
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							
							}
						} catch (Exception e) {

						}

					}
				}

			}
		}
//		BaseEntity targetBe = beUtils.getBaseEntityByCode(message.getData().getTargetCode());
//		BaseEntity defBe = beUtils.getDEF(targetBe);
//		if (defBe != null) {
//			System.out.println(ruleDetails+" "+drools.getRule().getName()+" Target DEF identified as "+defBe.getCode()+"!! "+defBe.getName());
//		}

		return valid;
	}

	@Produces
	public Topology buildTopology() {

		if (showValues) {
			log.info("service username :" + serviceUsername);
			log.info("service password :" + servicePassword);
			log.info("keycloakUrl      :" + keycloakUrl);
			log.info("BasekeycloakUrl  :" + baseKeycloakUrl);
			log.info("keycloak clientId:" + clientId);
			log.info("keycloak secret  :" + secret);
			log.info("keycloak realm   :" + keycloakRealm);
			log.info("api Url          :" + apiUrl);
			log.info("Def Dropdown Size:" + defaultDropDownSize);
		}

		try {
			serviceToken = getToken(serviceUsername, servicePassword);
			setUpDefs(serviceToken);
			defUtils.loadAllAttributesIntoCache(serviceToken);
		} catch (IOException e) {
			log.error("Cannot obtain Service Token for " + keycloakUrl + " and " + keycloakRealm);
		} catch (BadDataException e) {
			log.error("Cannot set up DEFs for " + keycloakUrl + " and " + keycloakRealm);
		}

//		ObjectMapperSerde<Attribute> qdatamessageSerde = new ObjectMapperSerde<>(Attribute.class);
//
		Attribute2 attribute = new Attribute2();
		StreamsBuilder builder = new StreamsBuilder();

		// Read the input Kafka topic into a KStream instance.

		builder.stream("events", Consumed.with(Serdes.String(), Serdes.String())).filter((k, v) -> checkDropDown(v))
				// .mapValues(attribute::tidy)
				// .peek((k, v) -> log.info("K[" + k + "] " + v))
				.to("webcmds", Produced.with(Serdes.String(), Serdes.String()));

		return builder.build();
	}

//    public QDataAttributeMessage loadAllAttributesIntoCache(final GennyToken token) {
//        try {
//            boolean cacheWorked = false;
//            String realm = token.getRealm();
//            log.info("All the attributes about to become loaded ... for realm "+realm);
//                 log.info("LOADING ATTRIBUTES FROM API");
//                String jsonString = apiQwandaService.getAttributes("Bearer " + token.getToken());
//                if (!StringUtils.isBlank(jsonString)) {
// 
//                    attributesMsg = jsonb.fromJson(jsonString, QDataAttributeMessage.class);
//                    Attribute[] attributeArray = attributesMsg.getItems();
//
//                    if (!realmAttributeMap.containsKey(realm)) {
//                    	realmAttributeMap.put(realm, new ConcurrentHashMap<String,Attribute>());
//                    }
//                    Map<String,Attribute> attributeMap = realmAttributeMap.get(realm);
//      
//                    for (Attribute attribute : attributeArray) {
//                        attributeMap.put(attribute.getCode(), attribute);
//                    }
//                   // realmAttributeMap.put(realm, attributeMap);
//                    
//                    log.info("All the attributes have been loaded from api in " + attributeMap.size() + " attributes");
//                } else {
//                    log.error("NO ATTRIBUTES LOADED FROM API");
//                }
//
//
//            return attributesMsg;
//        } catch (Exception e) {
//            log.error("Attributes API not available");
//        }
//        return null;
//    }
//    public QDataAttributeMessage loadAllAttributesIntoCache(final String token) {
//        return loadAllAttributesIntoCache(new GennyToken(token));
//    }

	public void setUpDefs(GennyToken userToken) throws BadDataException {

		SearchEntity searchBE = new SearchEntity("SBE_DEF", "DEF check")
				.addSort("PRI_NAME", "Created", SearchEntity.Sort.ASC)
				.addFilter("PRI_CODE", SearchEntity.StringFilter.LIKE, "DEF_%").addColumn("PRI_CODE", "Name");

		searchBE.setRealm(userToken.getRealm());
		searchBE.setPageStart(0);
		searchBE.setPageSize(1000);

		List<BaseEntity> items = getBaseEntitys(searchBE, userToken);
		// Load up RuleUtils.defs

		defUtils.defs.put(userToken.getRealm(), new ConcurrentHashMap<String, BaseEntity>());

		for (BaseEntity item : items) {
//            if the item is a def appointment, then add a default datetime for the start (Mandatory)
			if (item.getCode().equals("DEF_APPOINTMENT")) {
				Attribute attribute = new AttributeText("DFT_PRI_START_DATETIME", "Default Start Time");
				attribute.setRealm(userToken.getRealm());
				EntityAttribute newEA = new EntityAttribute(item, attribute, 1.0, "2021-07-28 00:00:00");
				item.addAttribute(newEA);

				Optional<EntityAttribute> ea = item.findEntityAttribute("ATT_PRI_START_DATETIME");
				if (ea.isPresent()) {
					ea.get().setValue(true);
				}
			}

//            Save the BaseEntity created
			item.setFastAttributes(true); // make fast
			defUtils.defs.get(userToken.getRealm()).put(item.getCode(), item);
			log.info("Saving (" + userToken.getRealm() + ") DEF " + item.getCode());
		}
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

	public JsonObject fetchSearchResults(final String searchBE, final GennyToken token) {
		String data = null;
		String value = null;
		try {
			data = apiQwandaService.getSearchResults(searchBE, "Bearer " + token.getToken());
			JsonObject json = jsonb.fromJson(data, JsonObject.class);
			if ("ok".equalsIgnoreCase(json.getString("status"))) {
				value = json.getString("value");
				// log.info(value);
			}
		} catch (Exception e) {
			log.error("Failed to get Results for search " + e.getMessage());
			e.printStackTrace();
		}
		return jsonb.fromJson(value, JsonObject.class);
	}

	public JsonObject getDecodedToken(final String bearerToken) {
		final String[] chunks = bearerToken.split("\\.");
		Base64.Decoder decoder = Base64.getDecoder();
//			String header = new String(decoder.decode(chunks[0]));
		String payload = new String(decoder.decode(chunks[1]));
		JsonObject json = jsonb.fromJson(payload, JsonObject.class);
		return json;
	}

	/**
	 * @param searchBE
	 * @return
	 */
	public List<BaseEntity> getBaseEntitys(final SearchEntity searchBE, GennyToken serviceToken) {
		List<BaseEntity> results = new ArrayList<BaseEntity>();

		try {
			log.info("creating searchJson for " + searchBE.getCode());
			String searchJson = jsonb.toJson(searchBE);
			log.info("Fetching baseentitys for " + searchBE.getCode());
			String resultJsonStr = apiQwandaService.getSearchResults(searchJson, "Bearer " + serviceToken.getToken());

			JsonObject resultJson = null;

			try {
				resultJson = jsonb.fromJson(resultJsonStr, JsonObject.class);
				JsonArray result = resultJson.getJsonArray("codes");
				log.info("Fetched baseentitys for " + searchBE.getCode() + ":" + resultJson);
				int size = result.size();
				for (int i = 0; i < size; i++) {
					String code = result.getString(i);
					BaseEntity be = fetchBaseEntityFromCache(code, serviceToken);
//					System.out.println("code:" + code + ",index:" + (i+1) + "/" + size);

					be.setIndex(i);
					results.add(be);
				}

			} catch (Exception e1) {
				log.error("Bad Json -> " + resultJsonStr);
			}

		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return results;
	}

	private GennyToken getToken(final String username, final String password) throws IOException {

		JsonObject keycloakResponseJson = getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, username, password,
				null);
		String accessToken = keycloakResponseJson.getString("access_token");
		GennyToken token = new GennyToken(accessToken);
		return token;
	}

	public JsonObject getToken(String keycloakUrl, String realm, String clientId, String secret, String username,
			String password, String refreshToken) throws IOException {

		HashMap<String, String> postDataParams = new HashMap<>();
		postDataParams.put("Content-Type", "application/x-www-form-urlencoded");
		/* if we have a refresh token */
//		if(refreshToken != null) {
//
//			/* we decode it */
//			JsonObject decodedServiceToken = KeycloakUtils.getDecodedToken(refreshToken);
//
//			/* we get the expiry timestamp */
//			long expiryTime = decodedServiceToken.getLong("exp");
//
//			/* we get the current time */
//			long nowTime = LocalDateTime.now().atZone(TimeZone.getDefault().toZoneId()).toEpochSecond();
//
//			/* we calculate the differencr */ 
//			long duration = expiryTime - nowTime;
//
//			/* if the difference is negative it means the expiry time is less than the nowTime 
//				if the difference < 180000, it means the token will expire in 3 hours
//			*/
//			if(duration <= GennySettings.ACCESS_TOKEN_EXPIRY_LIMIT_SECONDS) {
//
//				/* if the refresh token is about to expire, we must re-generate a new one */
//				refreshToken = null;
//			}
//		}
		/*
		 * if we don't have a refresh token, we generate a new token using username and
		 * password
		 */
		if (refreshToken == null) {
			postDataParams.put("username", username);
			postDataParams.put("password", password);
			if (showValues) {
				log.info("using username " + username);
				log.info("using password " + password);
				log.info("using client_id " + clientId);
				log.info("using client_secret " + secret);
			}
			postDataParams.put("grant_type", "password");
		} else {
			postDataParams.put("refresh_token", refreshToken);
			postDataParams.put("grant_type", "refresh_token");
			if (showValues) {
				log.info("using refresh token");
				log.info(refreshToken);
			}
		}

		postDataParams.put("client_id", clientId);
		if (!StringUtils.isBlank(secret)) {
			postDataParams.put("client_secret", secret);
		}

		String requestURL = keycloakUrl + "/auth/realms/" + realm + "/protocol/openid-connect/token";

		String postDataStr = getPostDataString(postDataParams);
		// String str =keycloakService.getAccessToken(realm, postDataStr);
		String str = performPostCall(requestURL, postDataParams);

		if (showValues) {
			log.info("keycloak auth url = " + requestURL);
			log.info(username + " token= " + str);
		}

		JsonObject json = jsonb.fromJson(str, JsonObject.class);
		return json;

	}

	public String performPostCall(String requestURL, HashMap<String, String> postDataParams) {

		URL url;
		String response = "";
		try {
			url = new URL(requestURL);

			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setReadTimeout(15000);
			conn.setConnectTimeout(15000);
			conn.setRequestMethod("POST");
			conn.setDoInput(true);
			conn.setDoOutput(true);

			OutputStream os = conn.getOutputStream();
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			writer.write(getPostDataString(postDataParams));

			writer.flush();
			writer.close();
			os.close();
			int responseCode = conn.getResponseCode();

			if (responseCode == HttpsURLConnection.HTTP_OK) {
				String line;
				BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				while ((line = br.readLine()) != null) {
					response += line;
				}
			} else {
				response = "";

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return response;
	}

	private String getPostDataString(HashMap<String, String> params) throws UnsupportedEncodingException {
		StringBuilder result = new StringBuilder();
		boolean first = true;
		for (Map.Entry<String, String> entry : params.entrySet()) {
			if (first)
				first = false;
			else
				result.append("&");

			result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
			result.append("=");
			result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
		}

		return result.toString();
	}

	// custom executor
//	private static final ExecutorService executorService = Executors.newFixedThreadPool(20);
//
//	private static HttpClient httpClient = HttpClient.newBuilder().executor(executorService)
//			.version(HttpClient.Version.HTTP_2).connectTimeout(Duration.ofSeconds(20)).build();
//
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

	public Boolean hasDropdown(final String attributeCode, final BaseEntity defBe) throws Exception {

		// Check if attribute code exists as a SER
		Optional<EntityAttribute> searchAtt = defBe.findEntityAttribute("SER_" + attributeCode); // SER_
		if (searchAtt.isPresent()) {
			// temporary enable check
			String serValue = searchAtt.get().getValueString();
			log.info("Attribute exists in " + defBe.getCode() + " for SER_" + attributeCode + " --> " + serValue);
			JsonObject serJson = jsonb.fromJson(serValue, JsonObject.class);
			if (serJson.containsKey("enabled")) {
				Boolean isEnabled = serJson.getBoolean("enabled");
				return isEnabled;
			} else {
				log.info("Attribute exists in " + defBe.getCode() + " for SER_" + attributeCode
						+ " --> but NOT enabled!");
				return true;
			}
		} else {
			log.info("No attribute exists in " + defBe.getCode() + " for SER_" + attributeCode);
		}
		return false;

	}

	public QDataBaseEntityMessage getDropdownData(String token, BaseEntity defBe, BaseEntity sourceBe,
			BaseEntity targetBe, final String attributeCode, final String parentCode, final String questionCode,
			final String searchText, Integer dropdownSize) {

		log.info("DROPDOWN :identified Dropdown Target Baseentity as " + defBe.getCode() + " : " + defBe.getName());
		log.info("DROPDOWN :identified Dropdown Attribute as " + attributeCode);

		/*
		 * Now check if this attribute is ok if
		 * (!internBe.containsEntityAttribute("ATT_"+qEventDropdownMessage.
		 * getAttributeCode())) {
		 * System.out.println("Error - Attribute "+qEventDropdownMessage.
		 * getAttributeCode()+" is not allowed for this target"); }
		 */

		/*
		 * because it is a drop down event we will search the DEF for the search
		 * attribute
		 */
		Optional<EntityAttribute> searchAtt = defBe.findEntityAttribute("SER_" + attributeCode); // SER_LNK_EDU_PROVIDER
		// String serValue =
		// "{\"search\":\"SBE_DROPDOWN\",\"parms\":[{\"attributeCode\":\"PRI_IS_EDU_PROVIDER\",\"value\":\"true\"}]}";
		String serValue = "{\"search\":\"SBE_DROPDOWN\",\"parms\":[{\"attributeCode\":\"PRI_IS_INTERN\",\"value\":\"true\"}]}";
		if (searchAtt.isPresent()) {
			serValue = searchAtt.get().getValueString();
			log.info("DROPDOWN :Search Attribute Value = " + serValue);
		} else {
			// return new QDataBaseEntityMessage();
		}

		return defUtils.getDropdownDataMessage(serviceToken, attributeCode, parentCode, questionCode, serValue,
				sourceBe, targetBe, searchText, token);

	}

}
