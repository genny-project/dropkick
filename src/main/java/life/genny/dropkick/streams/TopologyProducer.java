package life.genny.dropkick.streams;

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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbException;
import javax.net.ssl.HttpsURLConnection;

import life.genny.dropkick.client.ApiBridgeService;
import life.genny.dropkick.client.ApiQwandaService;
import life.genny.dropkick.client.ApiService;
import life.genny.dropkick.client.KeycloakService;
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

import io.quarkus.runtime.StartupEvent;
import life.genny.dropkick.live.data.InternalProducer;
import life.genny.qwandaq.models.ANSIColour;
import life.genny.qwandaq.models.GennySettings;
import life.genny.qwandaq.models.GennyToken;
import life.genny.qwandaq.attribute.Attribute;
import life.genny.qwandaq.attribute.EntityAttribute;
import life.genny.qwandaq.datatype.DataType;
import life.genny.qwandaq.entity.BaseEntity;
import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.exception.BadDataException;
import life.genny.qwandaq.exception.DebugException;
import life.genny.qwandaq.message.QDataBaseEntityMessage;
import life.genny.qwandaq.utils.MergeUtils;
import life.genny.qwandaq.utils.BaseEntityUtils;
import life.genny.qwandaq.utils.QwandaUtils;
import life.genny.qwandaq.utils.DefUtils;
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

	@ConfigProperty(name = "genny.api.url", defaultValue = "http://alyson.genny.life:8280")
	String apiUrl;

	@ConfigProperty(name = "genny.default.dropdown.size", defaultValue = "25")
	Integer defaultDropDownSize;

	@Inject
	InternalProducer producer;

	@Inject
	@RestClient
	KeycloakService keycloakService;

	BaseEntityUtils beUtils;

	QwandaUtils qwandaUtils;

	DefUtils defUtils;

	GennyToken serviceToken;

	Jsonb jsonb = JsonbBuilder.create();

    void onStart(@Observes StartupEvent ev) {

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

		serviceToken = new KeycloakUtils().getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, serviceUsername, servicePassword, null);

		// Init Utility Objects
		beUtils = new BaseEntityUtils(serviceToken);

		qwandaUtils = new QwandaUtils(serviceToken);
		qwandaUtils.loadAllAttributes();

		defUtils = new DefUtils(beUtils, qwandaUtils);
		defUtils.initializeDefs();


		log.info("[*] Finished Startup!");
    }

	@Produces
	public Topology buildTopology() {


		// Read the input Kafka topic into a KStream instance.
		StreamsBuilder builder = new StreamsBuilder();
		builder
			.stream("events", Consumed.with(Serdes.String(), Serdes.String()))
			.filter((k, v) -> checkDropDown(v))
			.to("webcmds", Produced.with(Serdes.String(), Serdes.String()));

		return builder.build();
	}

	public Boolean checkDropDown(String data) {

		Boolean valid = false;
		JsonObject json = jsonb.fromJson(data, JsonObject.class);

		if (!json.containsKey("event_type")) {
			return false;
		}

		String eventType = json.getString("event_type");

		if (!eventType.equals("DD")) {
			return false;
		}

		// TODO: validate token here
		String token = json.getString("token");

		JsonObject dataJson = json.getJsonObject("data");
		String attributeCode = null;

		if (json.containsKey("attributeCode")) {
			attributeCode = json.getString("attributeCode");
		} else {
			log.error("No Attribute code in message "+data);
		}

		// Check Source exists
		if (!dataJson.containsKey("sourceCode")) {
			log.error("Missing sourceCode in Dropdown Message ["+dataJson.toString()+"]");
			return false;
		}

		String sourceCode = dataJson.getString("sourceCode");
		BaseEntity sourceBe = this.beUtils.getBaseEntityByCode(sourceCode);

		if (sourceBe == null) {
			return false;
		}

		// Check Target exists
		if (!dataJson.containsKey("targetCode")) {
			log.error("Missing targetCode in Dropdown Message ["+dataJson.toString()+"]");
			return false;
		}

		String targetCode = dataJson.getString("targetCode");
		BaseEntity targetBe = this.beUtils.getBaseEntityByCode(targetCode);

		if (targetBe == null) {
			return false;
		}

		BaseEntity defBe = this.defUtils.getDEF(targetBe);
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

				QDataBaseEntityMessage msg = getDropdownData(token, defBe, sourceBe, targetBe,
						attributeCode, parentCode, questionCode, searchText, 15);

				String jsonStr = null;

				try {
					jsonStr = jsonb.toJson(msg);
					producer.getToWebCmds().send(jsonStr);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return valid;
	}

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
			BaseEntity targetBe, final String attrCode, final String parentCode, final String questionCode,
			final String searchText, Integer dropdownSize) {

		log.info("DROPDOWN :identified Dropdown Target Baseentity as " + defBe.getCode() + " : " + defBe.getName());
		log.info("DROPDOWN :identified Dropdown Attribute as " + attrCode);

		// Because it is a drop down event we will search the DEF for the search attribute
		Optional<EntityAttribute> searchAtt = defBe.findEntityAttribute("SER_" + attrCode);

		String serValue = "{\"search\":\"SBE_DROPDOWN\",\"parms\":[{\"attributeCode\":\"PRI_IS_INTERN\",\"value\":\"true\"}]}";
		if (searchAtt.isPresent()) {
			serValue = searchAtt.get().getValueString();
			log.info("DROPDOWN :Search Attribute Value = " + serValue);
		}

		JsonObject searchValueJson = null; 
		try {
			searchValueJson = jsonb.fromJson(serValue,JsonObject.class);
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

		if (sourceBe!=null) {
			ctxMap.put("SOURCE", sourceBe);
		}
		if (targetBe!=null) {
			ctxMap.put("TARGET", targetBe);
		}
		
		JsonArray jsonParms = searchValueJson.getJsonArray("parms");
		int size = jsonParms.size();
		log.info("json size = " + size);

		for (int i = 0; i < size; i++) {

			JsonObject json = null;

			try {

				json = jsonParms.getJsonObject(i);
				String attributeCode = json.getString("attributeCode");

				// Filters
				if (attributeCode != null) {

					Attribute att = this.qwandaUtils.getAttribute(attributeCode);

					String val = json.getString("value");

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
					log.info("dataType = " + dataType);

					if (dataType.getClassName().equals("life.genny.qwanda.entity.BaseEntity")) {
						if (attributeCode.equals("LNK_CORE") || attributeCode.equals("LNK_IND")) {  // These represent EntityEntity
							log.info("Adding CORE/IND DTT filter");
						// This is used for the sort defaults
						searchingOnLinks = true;

						// For using the search source and target
						String sourceCode = null;
						if (json.containsKey("sourceCode")) {
							sourceCode = json.getString("sourceCode");
						}
						String targetCode = null;
						if (json.containsKey("targetCode")) {
							targetCode = json.getString("targetCode");
						}

						// These will return True by default if source or target are null
						if (!MergeUtils.contextsArePresent(sourceCode, ctxMap)) {
							log.error(ANSIColour.RED+"A Parent value is missing for " + sourceCode + ", Not sending dropdown results"+ANSIColour.RESET);
							return null;
						}
						if (!MergeUtils.contextsArePresent(targetCode, ctxMap)) {
							log.error(ANSIColour.RED+"A Parent value is missing for " + targetCode + ", Not sending dropdown results"+ANSIColour.RESET);
							return null;
						}

						// Merge any data for source and target
						sourceCode = MergeUtils.merge(sourceCode, ctxMap);
						targetCode = MergeUtils.merge(targetCode, ctxMap);

						log.info("attributeCode = " + json.getString("attributeCode"));
						log.info("val = " + val);
						log.info("link sourceCode = " + sourceCode);
						log.info("link targetCode = " + targetCode);

						// Set Source and Target if found it parameter
						if (sourceCode != null) {
							searchBE.setSourceCode(sourceCode);
						}
						if (targetCode != null) {
							searchBE.setTargetCode(targetCode);
						}

						// Set LinkCode and LinkValue
						searchBE.setLinkCode(att.getCode());
						searchBE.setLinkValue(val);
						} else {
							// This is a DTT_LINK style that has class = baseentity --> Baseentity_Attribute
							SearchEntity.StringFilter stringFilter = SearchEntity.StringFilter.LIKE;  // TODO equals?
							if (filterStr != null) {
								stringFilter = SearchEntity.convertOperatorToStringFilter(filterStr);
							}
							// searchBE.addFilter(attributeCode, stringFilter, val);
							log.info("Adding BE DTT filter");

							if (logic != null && logic.equals("AND")) {
								log.info("Adding AND filter for " + attributeCode);
								searchBE.addAnd(attributeCode, stringFilter, val);
							} else if (logic != null && logic.equals("OR")) {
								log.info("Adding OR filter for " + attributeCode);
								searchBE.addOr(attributeCode, stringFilter, val);
							} else {
								log.info("Adding REGULAR filter for " + attributeCode);
								searchBE.addFilter(attributeCode, stringFilter, val);
							}
							
						}
						
					} else if (dataType.getClassName().equals("java.lang.String")) {
						SearchEntity.StringFilter stringFilter = SearchEntity.StringFilter.LIKE;
						if (filterStr != null) {
							stringFilter = SearchEntity.convertOperatorToStringFilter(filterStr);
						}
						log.info("Adding string DTT filter");
						// searchBE.addFilter(attributeCode, stringFilter, val);					

						if (logic != null && logic.equals("AND")) {
							log.info("Adding AND filter for " + attributeCode);
							searchBE.addAnd(attributeCode, stringFilter, val);
						} else if (logic != null && logic.equals("OR")) {
							log.info("Adding OR filter for " + attributeCode);
							searchBE.addOr(attributeCode, stringFilter, val);
						} else {
							log.info("Adding REGULAR filter for " + attributeCode);
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

				// Sorts
				String sortBy = null;
				if (json.containsKey("sortBy")) {
					sortBy = json.getString("sortBy");
				}
				if (sortBy != null) {
					String order = json.getString("order");
					SearchEntity.Sort sortOrder = order.equals("DESC") ? SearchEntity.Sort.DESC : SearchEntity.Sort.ASC;
					searchBE.addSort(sortBy, sortBy, sortOrder);
				}

				// Conditionals
				if (json.containsKey("conditions")) {
					JsonArray conditions = json.getJsonArray("conditions");
					for (Object cond : conditions) {
						searchBE.addConditional(attributeCode, cond.toString());
					}
				}
			} catch (Exception e) {
				log.error(e.getStackTrace());
				// TODO Auto-generated catch block
				log.error("DROPDOWN :Bad Json Value ---> " + json.toString());
				continue;
			}
		}

		// Default to sorting by name if no sorts were specified and if not searching for EntityEntitys
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
		pageStart += pageSize;

		// Capability Based Conditional Filters
		// searchBE = SearchUtils.evaluateConditionalFilters(beUtils, searchBE);

		// Merge required attribute values
		// NOTE: This should correct any wrong datatypes too
		searchBE = mergeFilterValueVariables(searchBE, ctxMap);
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

			log.info("DROPDOWN :Loaded " + msg.getItems().length + " baseentitys");
			msg = new QDataBaseEntityMessage(results);

			for (BaseEntity item : msg.getItems()) {
				if ( item.getValueAsString("PRI_NAME") == null ) {
					log.warn("DROPDOWN : item: " + item.getCode() + " ===== " + item.getValueAsString("PRI_NAME"));
				} else {
					log.info("DROPDOWN : item: " + item.getCode() + " ===== " + item.getValueAsString("PRI_NAME"));
				}
			}
		} else {
			log.info("DROPDOWN :Loaded NO baseentitys");
		}

		msg.setParentCode(parentCode);
		msg.setQuestionCode(questionCode); 
		msg.setToken(token);
		msg.setLinkCode("LNK_CORE");
		msg.setLinkValue("ITEMS");
		msg.setReplace(true);
		msg.setShouldDeleteLinkedBaseEntities(false);

		return msg;
	}
	
	public SearchEntity mergeFilterValueVariables(SearchEntity searchBE, Map<String, Object> ctxMap) {

		for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {
			// Iterate all Filters
			if (ea.getAttributeCode().startsWith("PRI_") || ea.getAttributeCode().startsWith("LNK_")) {

				// Grab the Attribute for this Code, using array in case this is an associated filter
				String[] attributeCodeArray = ea.getAttributeCode().split("\\.");
				String attributeCode = attributeCodeArray[attributeCodeArray.length-1];
				// Fetch the corresponding attribute
				Attribute att = this.qwandaUtils.getAttribute(attributeCode);
				DataType dataType = att.getDataType();

				Object attributeFilterValue = ea.getValue();
				if (attributeFilterValue != null) {
					// Ensure EntityAttribute Dataype is Correct for Filter
					Attribute searchAtt = new Attribute(ea.getAttributeCode(), ea.getAttributeName(), dataType);
					ea.setAttribute(searchAtt);
					String attrValStr = attributeFilterValue.toString();

					// First Check if Merge is required
					Boolean requiresMerging = MergeUtils.requiresMerging(attrValStr);

					if (requiresMerging != null && requiresMerging) {
						// update Map with latest baseentity
						ctxMap.keySet().forEach(key -> {
							Object value = ctxMap.get(key);
							if (value.getClass().equals(BaseEntity.class)) {
								BaseEntity baseEntity = (BaseEntity) value;
								ctxMap.put(key, this.beUtils.getBaseEntityByCode(baseEntity.getCode()));
							}
						});
						// Check if contexts are present
						if (MergeUtils.contextsArePresent(attrValStr, ctxMap)) {
							// NOTE: HACK, MergeUtils should be taking care of this bracket replacement - Jasper (6/08/2021)
							Object mergedObj = MergeUtils.wordMerge(attrValStr.replace("[[", "").replace("]]", ""), ctxMap);
							// Ensure Dataype is Correct, then set Value
							ea.setValue(mergedObj);
						} else {
							log.error(ANSIColour.RED + "Not all contexts are present for " + attrValStr + ANSIColour.RESET);
							return null;
						}
					} else {
						// This should filter out any values of incorrect datatype
						ea.setValue(attributeFilterValue);
					}
				} else {
					log.error(ANSIColour.RED + "Value is NULL for entity attribute " + attributeCode + ANSIColour.RESET);
					return null;
				}
			}
		}

		return searchBE;
	}
		
	public BaseEntity getDEF(final BaseEntity be, final GennyToken userToken) {
		if (be == null) {
			log.error("be param is NULL");
			try {
				throw new DebugException("BaseEntityUtils: getDEF: The passed BaseEntity is NULL, supplying trace");
			} catch (DebugException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}

		if (be.getCode().startsWith("DEF_")) {
			return be;
		}
		// Some quick ones
		if (be.getCode().startsWith("PRJ_")) {
			BaseEntity defBe = this.defUtils.getDefMap(userToken).get("DEF_PROJECT");
			return defBe;
		}

		List<EntityAttribute> isAs = be.findPrefixEntityAttributes("PRI_IS_");

		// remove the non DEF ones
		/*
		 * PRI_IS_DELETED PRI_IS_EXPANDABLE PRI_IS_FULL PRI_IS_INHERITABLE PRI_IS_PHONE
		 * (?) PRI_IS_SKILLS
		 */
		Iterator<EntityAttribute> i = isAs.iterator();
		while (i.hasNext()) {
			EntityAttribute ea = i.next();

			if (ea.getAttributeCode().startsWith("PRI_IS_APPLIED_")) {

				i.remove();
			} else {
				switch (ea.getAttributeCode()) {
					case "PRI_IS_DELETED":
					case "PRI_IS_EXPANDABLE":
					case "PRI_IS_FULL":
					case "PRI_IS_INHERITABLE":
					case "PRI_IS_PHONE":
					case "PRI_IS_AGENT_PROFILE_GRP":
					case "PRI_IS_BUYER_PROFILE_GRP":
					case "PRI_IS_EDU_PROVIDER_STAFF_PROFILE_GRP":
					case "PRI_IS_REFERRER_PROFILE_GRP":
					case "PRI_IS_SELLER_PROFILE_GRP":
					case "PRI_IS SKILLS":
						log.warn("getDEF -> detected non DEFy attributeCode " + ea.getAttributeCode());
						i.remove();
						break;
					case "PRI_IS_DISABLED":
						log.warn("getDEF -> detected non DEFy attributeCode " + ea.getAttributeCode());
						// don't remove until we work it out...
						try {
							throw new DebugException("Bad DEF " + ea.getAttributeCode());
						} catch (DebugException e) {
							e.printStackTrace();
						}
						break;
					case "PRI_IS_LOGBOOK":
						log.debug("getDEF -> detected non DEFy attributeCode " + ea.getAttributeCode());
						i.remove();

					default:

				}
			}
		}

		if (isAs.size() == 1) {
			// Easy
			Map<String, BaseEntity> beMapping = this.defUtils.getDefMap(userToken);
			String attrCode = isAs.get(0).getAttributeCode();

			String trimedAttrCode = attrCode.substring("PRI_IS_".length());

			BaseEntity defBe = beMapping.get("DEF_" + trimedAttrCode);

			//				BaseEntity defBe = RulesUtils.defs.get(be.getRealm())
			//						.get("DEF_" + isAs.get(0).getAttributeCode().substring("PRI_IS_".length()));
			if (defBe == null) {
				log.error(
						"No such DEF called " + "DEF_" + isAs.get(0).getAttributeCode().substring("PRI_IS_".length()));
			}
			return defBe;
		} else if (isAs.isEmpty()) {
			// THIS HANDLES CURRENT BAD BEs
			// loop through the defs looking for matching prefix
			for (BaseEntity defBe : this.defUtils.getDefMap(userToken).values()) {
				String prefix = defBe.getValue("PRI_PREFIX", null);
				if (prefix == null) {
					continue;
				}
				// LITTLE HACK FOR OHS DOCS, SORRY!
				if (prefix.equals("DOC") && be.getCode().startsWith("DOC_OHS_")) {
					continue;
				}
				if (be.getCode().startsWith(prefix + "_")) {
					return defBe;
				}
			}

			log.error("NO DEF ASSOCIATED WITH be " + be.getCode());
			return new BaseEntity("ERR_DEF", "No DEF");
		} else {
			// Create sorted merge code
			String mergedCode = "DEF_" + isAs.stream().sorted(Comparator.comparing(EntityAttribute::getAttributeCode))
				.map(ea -> ea.getAttributeCode()).collect(Collectors.joining("_"));
			mergedCode = mergedCode.replaceAll("_PRI_IS_DELETED", "");
			BaseEntity mergedBe = this.defUtils.getDefMap(userToken).get(mergedCode);
			if (mergedBe == null) {
				log.info("Detected NEW Combination DEF - " + mergedCode);
				// Get primary PRI_IS
				Optional<EntityAttribute> topDog = be.getHighestEA("PRI_IS_");
				if (topDog.isPresent()) {
					String topCode = topDog.get().getAttributeCode().substring("PRI_IS_".length());
					BaseEntity defTopDog = this.defUtils.getDefMap(userToken).get("DEF_" + topCode);
					mergedBe = new BaseEntity(mergedCode, mergedCode); // So this combination DEF inherits top dogs name
					// now copy all the combined DEF eas.
					for (EntityAttribute isea : isAs) {
						BaseEntity defEa = this.defUtils.getDefMap(userToken)
							.get("DEF_" + isea.getAttributeCode().substring("PRI_IS_".length()));
						if (defEa != null) {
							for (EntityAttribute ea : defEa.getBaseEntityAttributes()) {
								try {
									mergedBe.addAttribute(ea);
								} catch (BadDataException e) {
									log.error("Bad data in getDEF ea merge " + mergedCode);
								}
							}
						} else {
							log.info(
									"No DEF code -> " + "DEF_" + isea.getAttributeCode().substring("PRI_IS_".length()));
							return null;
						}
					}
					this.defUtils.getDefMap(userToken).put(mergedCode, mergedBe);
					return mergedBe;

				} else {
					log.error("NO DEF EXISTS FOR " + be.getCode());
					return null;
				}
			} else {
				return mergedBe; // return 'merged' composite
			}
		}

	}

}
