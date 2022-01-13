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
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbException;

import life.genny.dropkick.client.KeycloakService;
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

	GennyToken serviceToken;

	BaseEntityUtils beUtils;

	QwandaUtils qwandaUtils;

	DefUtils defUtils;

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

		log.info("INCOMING MESSAGE - " + data);

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

					if (dataType.getClassName().equals("life.genny.qwanda.entity.BaseEntity")) {

						// These represent EntityEntity
						if (attributeCode.equals("LNK_CORE") || attributeCode.equals("LNK_IND")) {

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
							// TODO equals?
							SearchEntity.StringFilter stringFilter = SearchEntity.StringFilter.LIKE;
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
		searchBE = this.defUtils.mergeFilterValueVariables(searchBE, ctxMap);
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
	
}
