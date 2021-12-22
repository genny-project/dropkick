package life.genny.utils;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbException;

import org.acme.kafka.streams.aggregator.model.ApiBridgeService;
import org.acme.kafka.streams.aggregator.model.ApiQwandaService;
import org.acme.kafka.streams.aggregator.model.ApiService;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.dropkick.live.data.InternalProducer;
import life.genny.models.GennyToken;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeText;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.datatype.DataType;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.SearchEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.exception.DebugException;
import life.genny.qwanda.message.QDataAttributeMessage;
import life.genny.qwanda.message.QDataBaseEntityMessage;
import life.genny.qwandautils.ANSIColour;
import life.genny.qwandautils.GennySettings;
import life.genny.qwandautils.MergeUtil;

@RegisterForReflection
@ApplicationScoped
public class DefUtils {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger log = Logger.getLogger(DefUtils.class);
	
	public static Map<String, Map<String, BaseEntity>> defs = new ConcurrentHashMap<>(); // realm and DEF lookup
	   public static Map<String,Map<String, Attribute>> realmAttributeMap = new ConcurrentHashMap<>();
	   public static QDataAttributeMessage attributesMsg = null;

	
	   @Inject
	    InternalProducer producer;
				
	    @Inject
	    MergeUtil mergeUtil;
		
		@Inject
		@RestClient
		ApiService apiService;

		@Inject
		@RestClient
		ApiQwandaService apiQwandaService;

		@Inject
		@RestClient
		ApiBridgeService apiBridgeService;

		
		Jsonb jsonb = JsonbBuilder.create();
		
		GennyToken serviceToken;


		public void setUpDefs(GennyToken serviceToken) throws BadDataException {

			SearchEntity searchBE = new SearchEntity("SBE_DEF", "DEF check")
					.addSort("PRI_NAME", "Created", SearchEntity.Sort.ASC)
					.addFilter("PRI_CODE", SearchEntity.StringFilter.LIKE, "DEF_%").addColumn("PRI_CODE", "Name");

			searchBE.setRealm(serviceToken.getRealm());
			searchBE.setPageStart(0);
			searchBE.setPageSize(1000);

			List<BaseEntity> items = getBaseEntitys(searchBE, serviceToken);
			// Load up RuleUtils.defs

			this.serviceToken = serviceToken;
			defs.put(serviceToken.getRealm(), new ConcurrentHashMap<String, BaseEntity>());

			for (BaseEntity item : items) {
//	            if the item is a def appointment, then add a default datetime for the start (Mandatory)
				if (item.getCode().equals("DEF_APPOINTMENT")) {
					Attribute attribute = new AttributeText("DFT_PRI_START_DATETIME", "Default Start Time");
					attribute.setRealm(serviceToken.getRealm());
					EntityAttribute newEA = new EntityAttribute(item, attribute, 1.0, "2021-07-28 00:00:00");
					item.addAttribute(newEA);

					Optional<EntityAttribute> ea = item.findEntityAttribute("ATT_PRI_START_DATETIME");
					if (ea.isPresent()) {
						ea.get().setValue(true);
					}
				}

//	            Save the BaseEntity created
				item.setFastAttributes(true); // make fast
				defs.get(serviceToken.getRealm()).put(item.getCode(), item);
				log.info("Saving ("+serviceToken.getRealm()+") DEF "+item.getCode());
			}
		}
	 
	
	public QDataBaseEntityMessage getDropdownDataMessage(GennyToken serviceToken, final String dropdownCode, final String parentCode, final String questionCode,final String serValue, final BaseEntity sourceBe, final BaseEntity targetBe, final String searchText, final String token)
	{
		this.serviceToken = serviceToken;
		JsonObject searchValueJson = null; 
		try {
			searchValueJson = jsonb.fromJson(serValue,JsonObject.class);
		} catch (JsonbException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Integer pageStart = 0;
		Integer pageSize = searchValueJson.containsKey("dropdownSize")?searchValueJson.getInt("dropdownSize"):GennySettings.defaultDropDownPageSize;
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
//		for (Object parmValue : jsonParms) {
			JsonObject json=null;
			try {
				 json = jsonParms.getJsonObject(i);
				//JsonObject json = (JsonObject) parmValue;
				 log.info("DEBUG0: attributeCode null = " + json.isNull("attributeCode"));
				String attributeCode = json.getString("attributeCode");

				// Filters
				if (attributeCode != null) {

					Attribute att = getAttribute(attributeCode, serviceToken);
					log.info("DEBUG0: value null = " + json.isNull("value"));

					String val = json.getString("value");

					log.info("DEBUG0: logic null = " + json.isNull("logic"));

					String logic = null;
					if (!json.isNull("logic")) {
						logic = json.getString("logic");
					}
					log.info("DEBUG1");

					String filterStr = null;
					if (val.contains(":")) {
						String[] valSplit = val.split(":");
						filterStr = valSplit[0];
						val = valSplit[1];
					}
					log.info("DEBUG2");

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
						if (!mergeUtil.contextsArePresent(sourceCode, ctxMap)) {
							log.error(ANSIColour.RED+"A Parent value is missing for " + sourceCode + ", Not sending dropdown results"+ANSIColour.RESET);
							return null;
						}
						if (!mergeUtil.contextsArePresent(targetCode, ctxMap)) {
							log.error(ANSIColour.RED+"A Parent value is missing for " + targetCode + ", Not sending dropdown results"+ANSIColour.RESET);
							return null;
						}

						// Merge any data for source and target
						sourceCode = mergeUtil.merge(sourceCode, ctxMap);
						targetCode = mergeUtil.merge(targetCode, ctxMap);

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
//		QDataBaseEntityMessage msg = null;
		
		String jsonSearch =jsonb.toJson(searchBE);
		QDataBaseEntityMessage msg = fetchSearchResultsBE(jsonSearch, serviceToken);
//		msg = jsonb.fromJson(searchResults.toString(), QDataBaseEntityMessage.class);
		
		if (msg == null) {
			log.error(ANSIColour.RED + "Dropdown search returned NULL!" + ANSIColour.RESET);
			return null;

		} else if (msg.getItems().length > 0) {
			log.info("DROPDOWN :Loaded " + msg.getItems().length + " baseentitys");

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

		// BaseEntity[] arrayItems = items.toArray(new BaseEntity[0]);
		// QDataBaseEntityMessage msg =  new QDataBaseEntityMessage(arrayItems, message.getData().getParentCode(), "LINK", Long.decode(items.size()+""));
//		log.info("DROPDOWN :code = "+dropdownCode+" with "+Long.decode(msg.getItems().length+"")+" Items");
//		log.info("DROPDOWN :parentCode = "+message.getData().getParentCode());
		msg.setParentCode(parentCode);
		msg.setQuestionCode(questionCode); 
		msg.setToken(token);
		msg.setLinkCode("LNK_CORE");
		msg.setLinkValue("ITEMS");
		msg.setReplace(true);
		msg.setShouldDeleteLinkedBaseEntities(false);

		/* Linking child baseEntity to the parent baseEntity */
		// QDataBaseEntityMessage beMessage = setDynamicLinksToParentBe(msg, message.getData().getParentCode(), "LNK_CORE", "DROPDOWNITEMS", beUtils.getGennyToken(),
		// 		false);
		return msg;
		// return beMessage;
	}
	
	/**
	 * @param searchBE
	 * @return
	 */
	public List<BaseEntity> getBaseEntitys(final SearchEntity searchBE, GennyToken serviceToken) {
		List<BaseEntity> results = new ArrayList<BaseEntity>();

		try {
			log.info("creating searchJson for "+searchBE.getCode());
			String searchJson = jsonb.toJson(searchBE);
			log.info("Fetching baseentitys for "+searchBE.getCode());
			String resultJsonStr = apiQwandaService.getSearchResults(searchJson, "Bearer " + serviceToken.getToken());
			
JsonObject resultJson = null;

			try {
				resultJson = jsonb.fromJson(resultJsonStr, JsonObject.class);
				JsonArray result = resultJson.getJsonArray("codes");
				log.info("Fetched baseentitys for "+searchBE.getCode()+":"+resultJson);
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
	    
		public SearchEntity mergeFilterValueVariables(SearchEntity searchBE, Map<String, Object> ctxMap) {

			for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {
				// Iterate all Filters
				if (ea.getAttributeCode().startsWith("PRI_") || ea.getAttributeCode().startsWith("LNK_")) {

					// Grab the Attribute for this Code, using array in case this is an associated filter
					String[] attributeCodeArray = ea.getAttributeCode().split("\\.");
					String attributeCode = attributeCodeArray[attributeCodeArray.length-1];
					// Fetch the corresponding attribute
					Attribute att = getAttribute(attributeCode, serviceToken);
					DataType dataType = att.getDataType();

					Object attributeFilterValue = ea.getValue();
					if (attributeFilterValue != null) {
						// Ensure EntityAttribute Dataype is Correct for Filter
						Attribute searchAtt = new Attribute(ea.getAttributeCode(), ea.getAttributeName(), dataType);
						ea.setAttribute(searchAtt);
						String attrValStr = attributeFilterValue.toString();

						// First Check if Merge is required
						Boolean requiresMerging = mergeUtil.requiresMerging(attrValStr);

						if (requiresMerging != null && requiresMerging) {
						// update Map with latest baseentity
							ctxMap.keySet().forEach(key -> {
								Object value = ctxMap.get(key);
								if (value.getClass().equals(BaseEntity.class)) {
									BaseEntity baseEntity = (BaseEntity) value;
									ctxMap.put(key, fetchBaseEntityFromCache(baseEntity.getCode(),serviceToken));
								}
							});
							// Check if contexts are present
							if (mergeUtil.contextsArePresent(attrValStr, ctxMap)) {
								// NOTE: HACK, mergeUtil should be taking care of this bracket replacement - Jasper (6/08/2021)
								Object mergedObj = mergeUtil.wordMerge(attrValStr.replace("[[", "").replace("]]", ""), ctxMap);
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
		
		
		public QDataBaseEntityMessage fetchSearchResultsBE(final String searchBE, final GennyToken token) {
			String data = null;
			String value = null;
			Integer total = -1;
			QDataBaseEntityMessage msg = null;
			List<BaseEntity> results = new ArrayList<>();
			try {
				data = apiQwandaService.getSearchResults(searchBE, "Bearer " + token.getToken());
				JsonObject json = null;
				try {
					json = jsonb.fromJson(data, JsonObject.class);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (json.containsKey("codes")) {
					Attribute nameAtt = this.getAttribute("PRI_NAME", token);
						JsonArray codes = null;
						try {
							codes  = json.getJsonArray("codes");
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						total = json.getInt("total");
						int size = codes.size();
						for (int i = 0; i < size; i++) {
							String code = codes.getString(i);
							BaseEntity be = fetchBaseEntityFromCache(code, serviceToken);
							if (be == null) {
								log.error("Baseentity "+code+" does not exist in Cache!");
								continue;
							}
							Set<EntityAttribute> nameSet = new HashSet<>();
							for (EntityAttribute ea : be.getBaseEntityAttributes()) {
								if ("PRI_NAME".equals(ea.getAttributeCode())) {
									nameSet.add(ea);
									break;
								}
							}
							be.setBaseEntityAttributes(nameSet);
//							System.out.println("code:" + code + ",index:" + (i+1) + "/" + size);
//							BaseEntity beItem = new BaseEntity(be.getCode(),be.getName());
//							beItem.setRealm(token.getRealm());
//							EntityAttribute ea = be.findEntityAttribute(nameAtt);
//							if (ea!=null) {
//								beItem.addAttribute(ea);
//							}
							
							be.setIndex(i);
							results.add(be);
						}

				} 
			} catch (Exception e) {
				log.error("Failed to get Results for search " + e.getMessage());
				e.printStackTrace();
			}
			msg = new QDataBaseEntityMessage(results);
			msg.setTotal(Long.getLong(results.size()+""));
			return msg;
		}
		
		public JsonObject fetchSearchResults(final String searchBE, final GennyToken token) {
			String data = null;
			String value = null;
			try {
				data = apiQwandaService.getSearchResults(searchBE, "Bearer " + token.getToken());
				JsonObject json = jsonb.fromJson(data, JsonObject.class);
				if (json.containsKey("status")) {
					if ("ok".equalsIgnoreCase(json.getString("status"))) {
						value = json.getString("value");
						// log.info(value);
					}
				} 
			} catch (Exception e) {
				log.error("Failed to get Results for search " + e.getMessage());
				e.printStackTrace();
			}
			return jsonb.fromJson(value, JsonObject.class);
		}
		
		public Map<String, BaseEntity> getDefMap(final GennyToken userToken) {
			if ((defs == null) || (defs.isEmpty())) {
				// Load in Defs
				try {
					setUpDefs(userToken);
					return defs.get(userToken.getRealm());
				} catch (BadDataException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return defs.get(userToken.getRealm());
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
				BaseEntity defBe = defs.get(userToken.getRealm()).get("DEF_PROJECT");
				return defBe;
			}

			Set<EntityAttribute> newMerge = new HashSet<>();
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
							// TODO Auto-generated catch block
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
				Map<String, BaseEntity> beMapping = getDefMap(userToken);
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
				for (BaseEntity defBe : defs.get(userToken.getRealm()).values()) {
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
				BaseEntity mergedBe = defs.get(userToken.getRealm()).get(mergedCode);
				if (mergedBe == null) {
					log.info("Detected NEW Combination DEF - " + mergedCode);
					// Get primary PRI_IS
					Optional<EntityAttribute> topDog = be.getHighestEA("PRI_IS_");
					if (topDog.isPresent()) {
						String topCode = topDog.get().getAttributeCode().substring("PRI_IS_".length());
						BaseEntity defTopDog = defs.get(userToken.getRealm()).get("DEF_" + topCode);
						mergedBe = new BaseEntity(mergedCode, mergedCode); // So this combination DEF inherits top dogs name
						// now copy all the combined DEF eas.
						for (EntityAttribute isea : isAs) {
							BaseEntity defEa = defs.get(userToken.getRealm())
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
						defs.get(userToken.getRealm()).put(mergedCode, mergedBe);
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
