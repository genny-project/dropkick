package life.genny.dropkick.streams;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbException;
import javax.persistence.EntityManager;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.quarkus.runtime.StartupEvent;
import life.genny.dropkick.intf.KafkaBean;
import life.genny.dropkick.live.data.InternalProducer;
import life.genny.qwandaq.models.ANSIColour;
import life.genny.qwandaq.models.GennySettings;
import life.genny.qwandaq.models.GennyToken;
import life.genny.qwandaq.attribute.Attribute;
import life.genny.qwandaq.attribute.EntityAttribute;
import life.genny.qwandaq.data.BridgeSwitch;
import life.genny.qwandaq.data.GennyCache;
import life.genny.qwandaq.datatype.DataType;
import life.genny.qwandaq.entity.BaseEntity;
import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.message.QDataBaseEntityMessage;
import life.genny.qwandaq.utils.MergeUtils;
import life.genny.qwandaq.utils.BaseEntityUtils;
import life.genny.qwandaq.utils.CacheUtils;
import life.genny.qwandaq.utils.CapabilityUtils;
import life.genny.qwandaq.utils.DatabaseUtils;
import life.genny.qwandaq.utils.QwandaUtils;
import life.genny.qwandaq.utils.DefUtils;
import life.genny.qwandaq.utils.KafkaUtils;
import life.genny.qwandaq.utils.KeycloakUtils;

@ApplicationScoped
public class TopologyProducer {

	private static final Logger log = Logger.getLogger(TopologyProducer.class);

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

	@ConfigProperty(name = "quarkus.oidc.auth-server-url", defaultValue = "https://keycloak.genny.life/auth/realms/genny")
	String keycloakUrl;

	@ConfigProperty(name = "genny.oidc.client-id", defaultValue = "backend")
	String clientId;

	@ConfigProperty(name = "genny.oidc.credentials.secret", defaultValue = "secret")
	String secret;

	@ConfigProperty(name = "genny.default.dropdown.size", defaultValue = "25")
	Integer defaultDropDownSize;

	@Inject
	EntityManager entityManager;

	@Inject
	InternalProducer producer;

	GennyToken serviceToken;

	BaseEntityUtils beUtils;

	@Inject
	GennyCache cache;

	@Inject
	KafkaBean kafkaBean;

	Jsonb jsonb = JsonbBuilder.create();

    void onStart(@Observes StartupEvent ev) {

		if (showValues) {
			log.info("service username  : " + serviceUsername);
			log.info("service password  : " + servicePassword);
			log.info("keycloakUrl       : " + keycloakUrl);
			log.info("BasekeycloakUrl   : " + baseKeycloakUrl);
			log.info("keycloak clientId : " + clientId);
			log.info("keycloak secret   : " + secret);
			log.info("keycloak realm    : " + keycloakRealm);
			log.info("Def Dropdown Size : " + defaultDropDownSize);
		}

		// Fetch our service token
		serviceToken = KeycloakUtils.getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, serviceUsername, servicePassword);

		// Init Utility Objects
		beUtils = new BaseEntityUtils(serviceToken);

		// Establish connection to DB and cache, and init utilities
		DatabaseUtils.init(entityManager);
		CacheUtils.init(cache);
		QwandaUtils.init(serviceToken);
		DefUtils.init(beUtils);
		KafkaUtils.init(kafkaBean);

		log.info("[*] Finished Startup!");
    }

	@Produces
	public Topology buildTopology() {

		// Read the input Kafka topic into a KStream instance.
		StreamsBuilder builder = new StreamsBuilder();
		builder
			.stream("events", Consumed.with(Serdes.String(), Serdes.String()))
			.peek((k, v) -> log.debug("Consumed message: " + v))

			.filter((k, v) -> isValidDropdownMessage(v))
			.peek((k, v) -> log.debug("Processing valid message: " + v))

			// .peek((k, v) -> updateBridgeSwitch(v))
			.mapValues(v -> fetchDropdownResults(v))

			.filter((k, v) -> v != null)
			.peek((k, v) -> log.debug("Sending results: " + v))
			.foreach((k, v) -> KafkaUtils.writeMsg("webcmds", v));

		return builder.build();
	}

	/**
	* Check if dropdown message is valid and has all necessary fields.
	*
	* @param data	Message to check
	* @return		Boolean value determining validity
	 */
	public Boolean isValidDropdownMessage(String data) {


		JsonObject json = jsonb.fromJson(data, JsonObject.class);

		// Check to make sure it has an event type
		if (!json.containsKey("event_type")) {
			return false;
		}

		String eventType = getJsonStringSafe(json, "event_type");

		// Check the event type is a dropdown event
		if (!eventType.equals("DD")) {
			return false;
		}

		// Check if it has a token
		if (!json.containsKey("token")) {
			return false;
		}

		String token = getJsonStringSafe(json, "token");

		// Check if token is valid
		try {
			GennyToken userToken = new GennyToken(token);
		} catch (Exception e) {
			log.error("Bad Token sent in dropdown message!");
			return false;
		}

		JsonObject dataJson = json.getJsonObject("data");

		// Check attribute code exists
		if (!json.containsKey("attributeCode")) {
			log.error("No Attribute code in message "+data);
			return false;
		}

		// Check Source exists
		if (!dataJson.containsKey("sourceCode")) {
			log.error("Missing sourceCode in Dropdown Message ["+dataJson.toString()+"]");
			return false;
		}

		// Check Target exists
		if (!dataJson.containsKey("targetCode")) {
			log.error("Missing targetCode in Dropdown Message ["+dataJson.toString()+"]");
			return false;
		}

		// Grab info required to find the DEF
		String attributeCode = getJsonStringSafe(json, "attributeCode");
		String targetCode = getJsonStringSafe(dataJson, "targetCode");
		BaseEntity target = this.beUtils.getBaseEntityByCode(targetCode);

		if (target == null) {
			return false;
		}

		// Find the DEF
		BaseEntity defBE = DefUtils.getDEF(target);

		// Check if attribute code exists as a SER for the DEF
		Optional<EntityAttribute> searchAttribute = defBE.findEntityAttribute("SER_" + attributeCode);

		if (!searchAttribute.isPresent()) {
			log.info("No attribute exists in " + defBE.getCode() + " for SER_" + attributeCode);
			return false;
		}

		// Parse search json to object
		String searchValue = searchAttribute.get().getValueString();
		JsonObject searchJson = jsonb.fromJson(searchValue, JsonObject.class);
		log.info("Attribute exists in " + defBE.getCode() + " for SER_" + attributeCode + " --> " + searchValue);

		if (searchJson.containsKey("enabled")) {

			Boolean isEnabled = searchJson.getBoolean("enabled");
			log.info("Search Json Enabled = " + isEnabled);

			return isEnabled;
		}

		return true;
	}


	private String getJsonStringSafe(JsonObject jsonObject, String code) {
		try {
			if (!jsonObject.containsKey(code)) {
				return null;
			}
			if(jsonObject.getValueType() != JsonValue.ValueType.STRING)
				log.warn("Value: " + jsonObject.getString(code) + " is not a string!");
			return jsonObject.getString(code);
		} catch(NullPointerException e) {
			log.error(ANSIColour.RED + "===============================" + ANSIColour.RESET);
			log.error(ANSIColour.RED + "Failed to find value: " + code + ANSIColour.RESET);
			log.error(ANSIColour.RED + "Data: " + jsonObject.toString() + ANSIColour.RESET);
			e.printStackTrace();
			return null;
		}
	}

	/**
	* Fetch and return the results for this dropdown. Will return null
	* if items can not be fetched for this message. This null must 
	* be filtered by streams builder.
	*
	* @param data
	* @return
	 */
	public String fetchDropdownResults(String data) {

		JsonObject jsonStr = jsonb.fromJson(data, JsonObject.class);

		// create usertoken and use it to update beUtils
		String token = getJsonStringSafe(jsonStr, "token");
		GennyToken userToken = new GennyToken(token);
		beUtils = new BaseEntityUtils(serviceToken, userToken);

		JsonObject dataJson = jsonStr.getJsonObject("data");
		String searchText = null;
		String attrCode = getJsonStringSafe(dataJson, "attributeCode");
		String sourceCode = getJsonStringSafe(dataJson, "sourceCode");
		String targetCode = getJsonStringSafe(dataJson, "targetCode");
		searchText = getJsonStringSafe(dataJson, "value");
		String parentCode = getJsonStringSafe(dataJson, "parentCode");
		String questionCode = getJsonStringSafe(dataJson, "questionCode");


		log.info(attrCode + ":" + parentCode + ":[" + searchText + "]");

		BaseEntity source = this.beUtils.getBaseEntityByCode(sourceCode);

		if (source == null) {
			log.error("Source Entity is NULL!");
			return null;
		}

		BaseEntity target = this.beUtils.getBaseEntityByCode(targetCode);

		if (target == null) {
			log.error("Target Entity is NULL!");
			return null;
		}

		BaseEntity defBE = DefUtils.getDEF(target);

		log.info("Target DEF is " + defBE.getCode() + " : " + defBE.getName());
		log.info("Attribute is " + attrCode);

		CapabilityUtils capabilityUtils = new CapabilityUtils(beUtils);

		// Because it is a drop down event we will search the DEF for the search attribute
		Optional<EntityAttribute> searchAttribute = defBE.findEntityAttribute("SER_" + attrCode);

		if (!searchAttribute.isPresent()) {
			log.error("No present search attribute for " + defBE.getCode());
			return null;
		}

		String searchValue = searchAttribute.get().getValueString();
		log.info("Search Attribute Value = " + searchValue);

		JsonObject searchValueJson = null; 
		try {
			searchValueJson = jsonb.fromJson(searchValue,JsonObject.class);
		} catch (JsonbException e1) {
			e1.printStackTrace();
		}

		Integer pageStart = 0;
		Integer pageSize = searchValueJson.containsKey("dropdownSize") ? searchValueJson.getInt("dropdownSize") : GennySettings.defaultDropDownPageSize;
		Boolean searchingOnLinks = false;

		SearchEntity searchBE = new SearchEntity("SBE_DROPDOWN", " Search")
						.addColumn("PRI_CODE", "Code")
						.addColumn("PRI_NAME", "Name");

		Map<String, Object> ctxMap = new ConcurrentHashMap<>();

		if (source != null) {
			ctxMap.put("SOURCE", source);
		}
		if (target != null) {
			ctxMap.put("TARGET", target);
		}
		
		JsonArray jsonParms = searchValueJson.getJsonArray("parms");
		int size = jsonParms.size();

		for (int i = 0; i < size; i++) {

			JsonObject json = null;

			try {

				json = jsonParms.getJsonObject(i);

				// conditionals
				Boolean conditionsAreMet = true;
				if (json.containsKey("conditions")) {
					JsonArray conditions = json.getJsonArray("conditions");
					for (Object cond : conditions) {
						if (!capabilityUtils.conditionMet(cond.toString().replaceAll("\"", ""))) {
							conditionsAreMet = false;
						}
					}
				}

				if (conditionsAreMet) {

					String attributeCode = getJsonStringSafe(json, "attributeCode");

					// Filters
					if (attributeCode != null) {

						Attribute att = QwandaUtils.getAttribute(attributeCode);

						String val = getJsonStringSafe(json, "value");

						String logic = null;
						if (json.containsKey("logic")) {
							logic = json.getString("logic");
						}

						String filterStr = null;
						if (val.contains(":")) {
							String[] valSplit = val.split(":");
							filterStr = valSplit[0];
							val = valSplit[1];
						}

						DataType dataType = att.getDataType();

						if (dataType.getClassName().equals("life.genny.qwanda.entity.BaseEntity")) {

							// These represent EntityEntity
							if (attributeCode.equals("LNK_CORE") || attributeCode.equals("LNK_IND")) {

								log.info("Adding CORE/IND DTT filter");
								// This is used for the sort defaults
								searchingOnLinks = true;

								// For using the search source and target and merge any data
								String paramSourceCode = null;
								if (json.containsKey("sourceCode")) {
									paramSourceCode = getJsonStringSafe(json, "sourceCode");

									// These will return True by default if source or target are null
									if (!MergeUtils.contextsArePresent(paramSourceCode, ctxMap)) {
										log.error(ANSIColour.RED+"A Parent value is missing for " + paramSourceCode + ", Not sending dropdown results"+ANSIColour.RESET);
										return null;
									}

									paramSourceCode = MergeUtils.merge(paramSourceCode, ctxMap);
								}

								String paramTargetCode = null;
								if (json.containsKey("targetCode")) {
									paramTargetCode = json.getString("targetCode");

									if (!MergeUtils.contextsArePresent(paramTargetCode, ctxMap)) {
										log.error(ANSIColour.RED+"A Parent value is missing for " + paramTargetCode + ", Not sending dropdown results"+ANSIColour.RESET);
										return null;
									}

									paramTargetCode = MergeUtils.merge(paramTargetCode, ctxMap);
								}

								log.info("attributeCode = " + getJsonStringSafe(json, "attributeCode"));
								log.info("val = " + val);
								log.info("link paramSourceCode = " + paramSourceCode);
								log.info("link paramTargetCode = " + paramTargetCode);

								// Set Source and Target if found it parameter
								if (paramSourceCode != null) {
									searchBE.setSourceCode(paramSourceCode);
								}
								if (paramTargetCode != null) {
									searchBE.setTargetCode(paramTargetCode);
								}

								// Set LinkCode and LinkValue
								searchBE.setLinkCode(att.getCode());
								searchBE.setLinkValue(val);
							} else {
								// This is a DTT_LINK style that has class = baseentity --> Baseentity_Attribute
								// TODO equals?
								SearchEntity.StringFilter stringFilter = SearchEntity.StringFilter.LIKE;
								if (filterStr != null) {
									stringFilter = SearchEntity.convertOperatorToStringFilter(filterStr);
								}
								log.info("Adding BE DTT filter");

								if (logic != null && logic.equals("AND")) {
									searchBE.addAnd(attributeCode, stringFilter, val);
								} else if (logic != null && logic.equals("OR")) {
									searchBE.addOr(attributeCode, stringFilter, val);
								} else {
									searchBE.addFilter(attributeCode, stringFilter, val);
								}

							}

						} else if (dataType.getClassName().equals("java.lang.String")) {
							SearchEntity.StringFilter stringFilter = SearchEntity.StringFilter.LIKE;
							if (filterStr != null) {
								stringFilter = SearchEntity.convertOperatorToStringFilter(filterStr);
							}
							log.info("Adding string DTT filter");

							if (logic != null && logic.equals("AND")) {
								searchBE.addAnd(attributeCode, stringFilter, val);
							} else if (logic != null && logic.equals("OR")) {
								searchBE.addOr(attributeCode, stringFilter, val);
							} else {
								searchBE.addFilter(attributeCode, stringFilter, val);
							}
						} else {
							SearchEntity.Filter filter = SearchEntity.Filter.EQUALS;
							if (filterStr != null) {
								filter = SearchEntity.convertOperatorToFilter(filterStr);
							}
							log.info("Adding Other DTT filter");
							searchBE.addFilterAsString(attributeCode, filter, val);
						}
					}
				}

				// sorts
				String sortBy = null;
				if (json.containsKey("sortBy")) {
					sortBy = json.getString("sortBy");
				}
				if (sortBy != null) {
					String order = getJsonStringSafe(json, "order");
					SearchEntity.Sort sortOrder = order.equals("DESC") ? SearchEntity.Sort.DESC : SearchEntity.Sort.ASC;
					searchBE.addSort(sortBy, sortBy, sortOrder);
				}

			} catch (Exception e) {
				log.error(e);
				log.error("DROPDOWN :Bad Json Value ---> " + json.toString());
				continue;
			}
		}

		// default to sorting by name if no sorts were specified and if not searching for EntityEntitys
		Boolean hasSort = searchBE.getBaseEntityAttributes().stream().anyMatch(item -> item.getAttributeCode().startsWith("SRT_"));
		if (!hasSort && !searchingOnLinks) {
			searchBE.addSort("PRI_NAME", "Name", SearchEntity.Sort.ASC);
		}
		
		// Filter by name wildcard provided by user
		searchBE.addFilter("PRI_NAME", SearchEntity.StringFilter.LIKE,searchText+"%")
		.addOr("PRI_NAME", SearchEntity.StringFilter.LIKE, "% "+searchText+"%");

		searchBE.setRealm(serviceToken.getRealm());
		searchBE.setPageStart(pageStart);
		searchBE.setPageSize(pageSize);

		// Capability Based Conditional Filters
		// searchBE = SearchUtils.evaluateConditionalFilters(beUtils, searchBE);

		// Merge required attribute values
		// NOTE: This should correct any wrong datatypes too
		searchBE = DefUtils.mergeFilterValueVariables(searchBE, ctxMap);

		if (searchBE == null) {
			log.error(ANSIColour.RED + "Cannot Perform Search!!!" + ANSIColour.RESET);
			return null;
		}

		// Perform search and evaluate columns
		List<BaseEntity> results = this.beUtils.getBaseEntitys(searchBE);
		QDataBaseEntityMessage msg = new QDataBaseEntityMessage();
		
		if (results == null) {

			log.error(ANSIColour.RED + "Dropdown search returned NULL!" + ANSIColour.RESET);
			return null;

		} else if (results.size() > 0) {

			msg = new QDataBaseEntityMessage(results);
			log.info("DROPDOWN :Loaded " + msg.getItems().size() + " baseentitys");

			for (BaseEntity item : msg.getItems()) {

				if (item.getValueAsString("PRI_NAME") == null ) {
					log.warn("DROPDOWN : item: " + item.getCode() + " ===== " + item.getValueAsString("PRI_NAME"));
				} else {
					log.info("DROPDOWN : item: " + item.getCode() + " ===== " + item.getValueAsString("PRI_NAME"));
				}
			}
		} else {
			log.info("DROPDOWN :Loaded NO baseentitys");
		}

		// Set all required message fields and return msg
		msg.setParentCode(parentCode);
		msg.setQuestionCode(questionCode); 
		msg.setToken(token);
		msg.setLinkCode("LNK_CORE");
		msg.setLinkValue("ITEMS");
		msg.setReplace(true);
		msg.setShouldDeleteLinkedBaseEntities(false);

		return jsonb.toJson(msg);
	}

	/**
	* Update the Dropkick BridgeSwitch records using payload data.
	*
	* @param data
	 */
	public void updateBridgeSwitch(String data) {
		
		// deserialise msg into JsonObject
		JsonObject payload = jsonb.fromJson(data, JsonObject.class);
		String token = getJsonStringSafe(payload, "token");

		// grab userToken from message
		GennyToken userToken = new GennyToken(token);
		String jti = userToken.getUniqueId();
		String bridgeId = "webcmds"; //payload.getString(jti);

		// update bridge switch
		BridgeSwitch.mappings.put(jti, bridgeId);
	}

}
