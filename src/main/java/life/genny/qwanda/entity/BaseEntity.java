/*
 * (C) Copyright 2017 GADA Technology (http://www.outcome-hub.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * Contributors: Adam Crow Byron Aguirre
 */

package life.genny.qwanda.entity;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.json.bind.annotation.JsonbTransient;

import org.jboss.logging.Logger;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.AnswerLink;
import life.genny.qwanda.CodedEntity;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.exception.BadDataException;


/**
 * BaseEntity represents a base entity that contains many attributes. It is the
 * base parent for many Qwanda classes and serves to establish Hibernate
 * compatibility and datetime stamping. BaseEntity objects may be scored against
 * each other. BaseEntity objects may not have a deterministic code Examples of
 * derivative entities may be Person, Company, Event, Product, TradeService.
 * This attribute information includes:
 * <ul>
 * <li>The List of attributes
 * </ul>
 *
 * 
 * 
 * @author Adam Crow
 * @author Byron Aguirre
 * @version %I%, %G%
 * @since 1.0
 */

@RegisterForReflection
public class BaseEntity extends CodedEntity{

	/**
	 * 
	 */

	private static final long serialVersionUID = 1L;

	/**
	 * Stores logger object.
	 */
	public static final Logger log = Logger.getLogger(BaseEntity.class);

	private static final String DEFAULT_CODE_PREFIX = "BAS_";
	
	public static final String PRI_NAME = "PRI_NAME";
	public static final String PRI_IMAGE_URL = "PRI_IMAGE_URL";
	public static final String PRI_PHONE = "PRI_PHONE";
	public static final String PRI_ADDRESS_FULL = "PRI_ADDRESS_FULL";
	public static final String PRI_EMAIL = "PRI_EMAIL";
	

	private Set<EntityAttribute> baseEntityAttributes = new HashSet<EntityAttribute>(0);


	/* Stores the links of BaseEntity to another BaseEntity */
	private Set<EntityEntity> links = new LinkedHashSet<>();


	private Set<EntityQuestion> questions = new HashSet<EntityQuestion>(0);


	private Set<AnswerLink> answers = new HashSet<AnswerLink>(0);


	private Boolean fromCache = false;


	private Map<String,EntityAttribute> attributeMap = null;
	
	
	
	public Map<String, EntityAttribute> getAttributeMap() {
		return attributeMap;
	}

	

	/**
	 * Constructor.
	 * 
	 * @param none
	 */

	public BaseEntity() {
		super();

	}

	/**
	 * Constructor.
	 * 
	 * @param Name the summary name of the core entity
	 */
	public BaseEntity(final String aName) {
		super(getDefaultCodePrefix() + UUID.randomUUID().toString(), aName);

	}

	/**
	 * Constructor.
	 * 
	 * @param Code the unique code of the core entity
	 * @param Name the summary name of the core entity
	 */
	public BaseEntity(final String aCode, final String aName) {
		super(aCode, aName);

	}

	/**
	 * @return the answers
	 */
	public Set<AnswerLink> getAnswers() {
		return answers;
	}

	/**
	 * @param answers the answers to set
	 */
	public void setAnswers(final Set<AnswerLink> answers) {
		this.answers = answers;
	}

	/**
	 * @return the baseEntityAttributes
	 */

	public Set<EntityAttribute> getBaseEntityAttributes() {
		return baseEntityAttributes;
	}

	/**
	 * @param baseEntityAttributes the baseEntityAttributes to set
	 */
	public void setBaseEntityAttributes(final Set<EntityAttribute> baseEntityAttributes) {
		this.baseEntityAttributes = baseEntityAttributes;
	}

	/**
	 * @return the links
	 */

	public Set<EntityEntity> getLinks() {
		return links;
	}

	/**
	 * Sets the Links of the BaseEntity with another BaseEntity
	 * 
	 * @param links the links to set
	 */
	public void setLinks(final Set<EntityEntity> links) {
		this.links = links;
	}

	/**
	 * @return the questions
	 */

	public Set<EntityQuestion> getQuestions() {
		return this.questions;
	}

	/**
	 * Sets the Questions of the BaseEntity with another BaseEntity
	 * 
	 * @param questions the questions to set
	 */
	public void setQuestions(final Set<EntityQuestion> questions) {
		this.questions = questions;
	}

	/**
	 * getDefaultCodePrefix This method is expected to be overridden in specialised
	 * child classes.
	 * 
	 * @return the default Code prefix for this class.
	 */

	static public String getDefaultCodePrefix() {
		return DEFAULT_CODE_PREFIX;
	}

	/**
	 * containsEntityAttribute This checks if an attribute exists in the baseEntity.
	 * 
	 * @param attributeCode
	 * @returns boolean
	 */
	public boolean containsEntityAttribute(final String attributeCode) {
		boolean ret = false;

		if (attributeMap != null) {
			return attributeMap.containsKey(attributeCode);
		}
		// Check if this code exists in the baseEntityAttributes
		if (getBaseEntityAttributes().parallelStream().anyMatch(ti -> ti.getAttributeCode().equals(attributeCode))) {
			ret = true;
		}
		return ret;
	}

	/**
	 * containsLink This checks if an attribute link code is linked to the
	 * baseEntity.
	 * 
	 * @param attributeCode
	 * @returns boolean
	 */
	public boolean containsLink(final String linkAttributeCode) {
		boolean ret = false;

		// Check if this code exists in the baseEntityAttributes
		if (getLinks().parallelStream().anyMatch(ti -> ti.getPk().getAttribute().getCode().equals(linkAttributeCode))) {
			ret = true;
		}
		return ret;
	}

	/**
	 * containsTarget This checks if another baseEntity is linked to the baseEntity.
	 * 
	 * @param targetCode
	 * @param linkAttributeCode
	 * @returns boolean
	 */
	public boolean containsTarget(final String targetCode, final String linkAttributeCode) {
		boolean ret = false;

		// Check if this code exists in the baseEntityAttributes
		if (getLinks().parallelStream().anyMatch(ti -> (ti.getLink().getAttributeCode().equals(linkAttributeCode)
				&& (ti.getLink().getTargetCode().equals(targetCode))))) {
			ret = true;
		}
		return ret;
	}

	/**
	 * findEntityAttribute This returns an attributeEntity if it exists in the
	 * baseEntity.
	 * 
	 * @param attributeCode
	 * @returns Optional<EntityAttribute>
	 */
	public Optional<EntityAttribute> findEntityAttribute(final String attributeCode) {
		//log.info("Hmmm which path in getValue are we taking, attributeCode:" + attributeCode);
		if (attributeMap != null) {
			//log.info("We are in the quick map part of getValue, attributeCode:" + attributeCode);
			return Optional.ofNullable(attributeMap.get(attributeCode));
		}
		Optional<EntityAttribute> foundEntity = null;

		try {
			//log.info("We are in the long filter part of getValue, attributeCode:" + attributeCode);
			foundEntity = getBaseEntityAttributes().stream().filter(x -> (x.getAttributeCode().equals(attributeCode)))
					.findFirst();
		} catch (Exception e) {
			log.error("Error in fetching attribute value");
		}

//    Optional.of(getBaseEntityAttributes().stream()
//            .filter(x -> (x.getAttribute().getCode().equals(attributeCode))).findFirst().get());

		return foundEntity;
	}

	/**
	 * findEntityAttribute This returns an attributeEntity if it exists in the
	 * baseEntity. Could be more efficient in retrival (ACC: test)
	 * 
	 * @param attribute
	 * @returns EntityAttribute
	 */
	public List<EntityAttribute> findPrefixEntityAttributes(final String attributePrefix) {
		List<EntityAttribute> foundEntitys = getBaseEntityAttributes().stream()
				.filter(x -> (x.getAttributeCode().startsWith(attributePrefix))).collect(Collectors.toList());

		return foundEntitys;
	}

	/**
	 * findEntityAttributes This returns attributeEntitys if it exists in the
	 * baseEntity. Could be more efficient in retrival (ACC: test)
	 * 
	 * @param attribute
	 * @returns EntityAttribute
	 */
	public EntityAttribute findEntityAttribute(final Attribute attribute) {
		final EntityAttribute foundEntity = getBaseEntityAttributes().stream()
				.filter(x -> (x.getAttributeCode().equals(attribute.getCode()))).findFirst().get();

		return foundEntity;
	}




	/**
	 * removeAttribute This removes an attribute and associated weight from the
	 * baseEntity. For efficiency we assume the attribute exists
	 * 
	 * @param attributeCode
	 * @param weight
	 */
	public Boolean removeAttribute(final String attributeCode) {
		Boolean removed = false;

		Iterator<EntityAttribute> i = this.baseEntityAttributes.iterator();
		while (i.hasNext()) {
			EntityAttribute ea = i.next();
			if (ea.getAttributeCode().equals(attributeCode)) {
				i.remove();
				removed = true;
				break;
			}
		}
		
		if (attributeMap != null) {
			attributeMap.remove(attributeCode);
		}

		return removed;
	}





	private <T> T getValue(final Attribute attribute) {
		// TODO Dumb find for attribute. needs a hashMap

		for (final EntityAttribute ea : this.getBaseEntityAttributes()) {
			if (ea.getAttribute().getCode().equalsIgnoreCase(attribute.getCode())) {
				return getValue(ea);
			}
		}
		return null;
	}


	private <T> T getValue(final EntityAttribute ea) {
		return ea.getValue();

	}


	public <T> Optional<T> getValue(final String attributeCode) {
		Optional<EntityAttribute> ea = this.findEntityAttribute(attributeCode);

		Optional<T> result = Optional.empty();
		if (ea.isPresent()) {
			if (ea.get() != null) {
				if (ea.get().getValue() != null) {
					result = Optional.of(ea.get().getValue());
				}
			}
		}
		return result;

	}


	public <T> Optional<T> getLoopValue(final String attributeCode) {
		Optional<EntityAttribute> ea = this.findEntityAttribute(attributeCode);

		Optional<T> result = Optional.empty();
		if (ea.isPresent()) {
			result = Optional.of(ea.get().getLoopValue());
		}
		return result;

	}


	public String getValueAsString(final String attributeCode) {
		Optional<EntityAttribute> ea = this.findEntityAttribute(attributeCode);
		String result = null;
		if (ea.isPresent()) {
			if (ea.get() != null) {
				if (ea.get().getValue() != null) {
					result = ea.get().getAsString();
				}
			}
		}
		return result;

	}


	public <T> T getValue(final String attributeCode, T defaultValue) {
		Optional<T> result = getValue(attributeCode);
		if (result.isPresent()) {
			if (!result.equals(Optional.empty())) {
				return result.get();
			}
		}
		return defaultValue;
	}


	public <T> T getLoopValue(final String attributeCode, T defaultValue) {
		Optional<T> result = getLoopValue(attributeCode);
		if (result.isPresent()) {
			if (!result.equals(Optional.empty())) {
				return result.get();
			}
		}
		return defaultValue;
	}


	public Boolean is(final String attributeCode) {
		Optional<EntityAttribute> ea = this.findEntityAttribute(attributeCode);
		Boolean result = false;

		if (ea.isPresent()) {
			result = ea.get().getValueBoolean();
			if (result == null) {
				return false;
			}
		}
		return result;

	}




	public void forcePrivate(final Attribute attribute, final Boolean state) {
		forcePrivate(attribute.getCode(), state);
	}


	public void forceInferred(final Attribute attribute, final Boolean state) {
		forceInferred(attribute.getCode(), state);
	}


	public void forceReadonly(final Attribute attribute, final Boolean state) {
		forceReadonly(attribute.getCode(), state);
	}


	public void forcePrivate(String attributeCode, Boolean state) {
		Optional<EntityAttribute> optEa = this.findEntityAttribute(attributeCode);
		if (optEa.isPresent()) {
			EntityAttribute ea = optEa.get();
			ea.setPrivacyFlag(state);
		}
	}


	public void forceInferred(final String attributeCode, final Boolean state) {
		Optional<EntityAttribute> optEa = this.findEntityAttribute(attributeCode);
		if (optEa.isPresent()) {
			EntityAttribute ea = optEa.get();
			ea.setInferred(state);
		}
	}


	public void forceReadonly(final String attributeCode, final Boolean state) {
		Optional<EntityAttribute> optEa = this.findEntityAttribute(attributeCode);
		if (optEa.isPresent()) {
			EntityAttribute ea = optEa.get();
			ea.setReadonly(state);
		}
	}

	/**
	 * @return the fromCache
	 */
	public Boolean getFromCache() {
		return fromCache;
	}

	/**
	 * @param fromCache the fromCache to set
	 */
	public void setFromCache(Boolean fromCache) {
		this.fromCache = fromCache;
	}

	@JsonbTransient
	public String[] getPushCodes() {
		return getPushCodes(new String[0]);
	}


	public String[] getPushCodes(String... initialCodes) {
		// go through all the links
		Set<String> codes = new HashSet<String>();
		codes.addAll(new HashSet<>(Arrays.asList(initialCodes)));
		if ((this.baseEntityAttributes != null) && (!this.baseEntityAttributes.isEmpty())) {
			for (EntityAttribute ea : this.baseEntityAttributes) {
			//	if (ea.getAttributeCode().startsWith("LNK_")) {
					String value = ea.getValueString();
					if (value != null) {
						if (value.startsWith("[")) {
							value = value.substring(2, value.length() - 2);
						}
						if (value.startsWith("PER")||(value.startsWith("CPY"))) {
							codes.add(value);
						}
					}
				}
			//}
			if (this.getCode().startsWith("PER")||(this.getCode().startsWith("CPY"))) {
				codes.add(this.getCode());
			}
		}

		return codes.toArray(new String[0]);
	}
	

	public Optional<EntityAttribute> getHighestEA(final String prefix) {
		// go through all the EA
		Optional<EntityAttribute> highest = Optional.empty();
		Double weight = -1000.0;
		
		if ((this.baseEntityAttributes != null) && (!this.baseEntityAttributes.isEmpty())) {
			for (EntityAttribute ea : this.baseEntityAttributes) {
				if (ea.getAttributeCode().startsWith(prefix)) {
					if (ea.getWeight() > weight) {
						highest = Optional.of(ea);
						weight = ea.getWeight();
					}
				
				}
			}
		
		}

		return highest;
	}
	
	
	public void setFastAttributes(Boolean fastMode)
	{
		if (fastMode) {
			attributeMap = new ConcurrentHashMap<String,EntityAttribute>();
		// Grab all the entityAttributes and create a fast HashMap lookup
		for (EntityAttribute ea : this.baseEntityAttributes) {
			attributeMap.put(ea.getAttributeCode(), ea);
		}
		} else {
			attributeMap = null;
		}

	}
	
	/**
	 * addAttribute This adds an attribute with default weight of 0.0 to the
	 * baseEntity. It auto creates the EntityAttribute object. For efficiency we
	 * assume the attribute does not already exist
	 * 
	 * @param ea
	 * @throws BadDataException
	 */
	public EntityAttribute addAttribute(final EntityAttribute ea) throws BadDataException {
		if (ea == null)
			throw new BadDataException("missing Attribute");

		return addAttribute(ea.getAttribute(), ea.getWeight(), ea.getValue());
	}

	/**
	 * addAttribute This adds an attribute and associated weight to the baseEntity.
	 * It auto creates the EntityAttribute object. For efficiency we assume the
	 * attribute does not already exist
	 * 
	 * @param attribute
	 * @param weight
	 * @throws BadDataException
	 */
	public EntityAttribute addAttribute(final Attribute attribute) throws BadDataException {

		return addAttribute(attribute, 1.0);
	}

	/**
	 * addAttribute This adds an attribute and associated weight to the baseEntity.
	 * It auto creates the EntityAttribute object. For efficiency we assume the
	 * attribute does not already exist
	 * 
	 * @param attribute
	 * @param weight
	 * @throws BadDataException
	 */
	public EntityAttribute addAttribute(final Attribute attribute, final Double weight) throws BadDataException {
		return addAttribute(attribute, weight, null);
	}

	/**
	 * addAttribute This adds an attribute and associated weight to the baseEntity.
	 * It auto creates the EntityAttribute object. For efficiency we assume the
	 * attribute does not already exist
	 * 
	 * @param attribute
	 * @param weight
	 * @param value     (of type String, LocalDateTime, Long, Integer, Boolean
	 * @throws BadDataException
	 */
	public EntityAttribute addAttribute(final Attribute attribute, final Double weight, final Object value)
			throws BadDataException {
		if (attribute == null)
			throw new BadDataException("missing Attribute");
		if (weight == null)
			throw new BadDataException("missing weight");

		final EntityAttribute entityAttribute = new EntityAttribute(this, attribute, weight, value);
		Optional<EntityAttribute> existing = findEntityAttribute(attribute.getCode());
		if (existing.isPresent()) {
			existing.get().setValue(value);
			existing.get().setWeight(weight);
			// removeAttribute(existing.get().getAttributeCode());
		} else {
			getBaseEntityAttributes().add(entityAttribute);
		}
		return entityAttribute;
	}

	/**
	 * addAttributeOmitCheck This adds an attribute and associated weight to the
	 * baseEntity. This method will NOT check and update any existing attributes.
	 * Use with Caution.
	 * 
	 * @param attribute
	 * @param weight
	 * @param value     (of type String, LocalDateTime, Long, Integer, Boolean
	 * @throws BadDataException
	 */
	public EntityAttribute addAttributeOmitCheck(final Attribute attribute, final Double weight, final Object value)
			throws BadDataException {
		if (attribute == null)
			throw new BadDataException("missing Attribute");
		if (weight == null)
			throw new BadDataException("missing weight");

		final EntityAttribute entityAttribute = new EntityAttribute(this, attribute, weight, value);
		getBaseEntityAttributes().add(entityAttribute);

		return entityAttribute;
	}

	@JsonbTransient
	public <T> Optional<T> setValue(final Attribute attribute, T value, Double weight) throws BadDataException {
		Optional<EntityAttribute> oldValue = this.findEntityAttribute(attribute.getCode());

		Optional<T> result = Optional.empty();
		if (oldValue.isPresent()) {
			if (oldValue.get().getLoopValue() != null) {
				result = Optional.of(oldValue.get().getLoopValue());
			}
			EntityAttribute ea = oldValue.get();
			ea.setValue(value);
			ea.setWeight(weight);
		} else {
			this.addAttribute(attribute, weight, value);
		}
		return result;
	}

	@JsonbTransient
	public <T> Optional<T> setValue(final Attribute attribute, T value) throws BadDataException {
		return setValue(attribute, value, 0.0);
	}

	@JsonbTransient
	public <T> Optional<T> setValue(final String attributeCode, T value) throws BadDataException {
		return setValue(attributeCode, value, 0.0);
	}

	@JsonbTransient
	public <T> Optional<T> setValue(final String attributeCode, T value, Double weight) throws BadDataException {
		Optional<EntityAttribute> oldValue = this.findEntityAttribute(attributeCode);

		Optional<T> result = Optional.empty();
		if (oldValue.isPresent()) {
			if (oldValue.get().getLoopValue() != null) {
				result = Optional.of(oldValue.get().getLoopValue());
			}
			EntityAttribute ea = oldValue.get();
			ea.setValue(value);
			ea.setWeight(weight);
		}
		return result;
	}

}
