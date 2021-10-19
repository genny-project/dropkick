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

package life.genny.qwanda;


import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.exception.BadDataException;


import javax.validation.Valid;

import org.jboss.logging.Logger;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * Question is the abstract base class for all questions managed in the Qwanda
 * library. A Question object is used as a means of requesting information from
 * a source about a target attribute. This question information includes:
 * <ul>
 * <li>The Human Readable name for this question (used for summary lists)
 * <li>A title for the question
 * <li>The text that presents the default question to the source
 * <li>The attribute that the question serves to fill
 * <li>The contexts that are mandatory for this question
 * <li>The default expiry duration that should be required to answer.
 * <li>The default media used to ask this question.
 * </ul>
 * <p>
 * Questions represent the major way of retrieving facts about a target from
 * sources. Each question is associated with an attribute which represents a
 * distinct fact about a target.
 * <p>
 * 
 * 
 * @author Adam Crow
 * @author Byron Aguirre
 * @version %I%, %G%
 * @since 1.0
 */

@RegisterForReflection
public class Question extends CodedEntity implements Serializable {

	/**
	 * Stores logger object.
	 */
	private static final Logger log = Logger.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
	private static final long serialVersionUID = 1L;

	private static final String DEFAULT_CODE_PREFIX = "QUE_";
	public static final String QUESTION_GROUP_ATTRIBUTE_CODE = "QQQ_QUESTION_GROUP";

	private Set<QuestionQuestion> childQuestions = new HashSet<QuestionQuestion>(0);

	private Attribute attribute;

	private ContextList contextList;

	private String attributeCode;

	private Boolean mandatory = false;

	private Boolean readonly = false;

	private Boolean oneshot = false;
	
	private String placeholder = "";

	private String directions = "";

	private String html;

	private String helper = "";

	private String icon;

	public String getHelper() {
		return helper;
	}

	public void setHelper(String helper) {
		this.helper = helper;
	}


	/**
	 * Constructor.
	 * 
	 * @param none
	 */
	@SuppressWarnings("unused")
	public Question() {
		// dummy for hibernate
	}

	/**
	 * Constructor.
	 * 
	 * @param aCode     The unique code for this Question
	 * @param aName     The human readable summary name
	 * @param attribute The associated attribute
	 */
	public Question(final String aCode, final String aName, final Attribute aAttribute) {
		this(aCode, aName, aAttribute, false);
	}

	/**
	 * Constructor.
	 * 
	 * @param aCode     The unique code for this Question
	 * @param aName     The human readable summary name
	 * @param attribute The associated attribute
	 */
	public Question(final String aCode, final String aName, final Attribute aAttribute,final String placeholder) {
		this(aCode, aName, aAttribute, false, aName, placeholder);
	}
	
	/**
	 * Constructor.
	 * 
	 * @param aCode     The unique code for this Question
	 * @param aName     The human readable summary name
	 * @param attribute The associated attribute
	 * @param mandatory
	 */
	public Question(final String aCode, final String aName, final Attribute aAttribute, final Boolean mandatory) {
		this(aCode, aName, aAttribute, mandatory, aName);
	}
	
	/**
	 * Constructor.
	 * 
	 * @param aCode     The unique code for this Question
	 * @param aName     The human readable summary name
	 * @param attribute The associated attribute
	 * @param mandatory
	 */
	public Question(final String aCode, final String aName, final Attribute aAttribute, final Boolean mandatory,
			final String html) {
		this(aCode, aName, aAttribute, mandatory, html, null);
	}

	
	/**
	 * Constructor.
	 * 
	 * @param aCode     The unique code for this Question
	 * @param aName     The human readable summary name
	 * @param attribute The associated attribute
	 * @param mandatory
	 */
	public Question(final String aCode, final String aName, final Attribute aAttribute, final Boolean mandatory,
			final String html, final String placeholder) {
		super(aCode, aName);
		if (aAttribute == null) {
			throw new InvalidParameterException("Attribute must not be null");
		}
		this.attribute = aAttribute;
		this.attributeCode = aAttribute.getCode();
		this.mandatory = mandatory;
		this.html = html;
		this.placeholder = placeholder;
	}

	/**
	 * Constructor.
	 * 
	 * @param aCode          The unique code for this Question
	 * @param aName          The human readable summary name
	 * @param childQuestions The associated child Questions in this question Group
	 */
	public Question(final String aCode, final String aName, final List<Question> childQuestions) {
		super(aCode, aName);
		if (childQuestions == null) {
			throw new InvalidParameterException("QuestionList must not be null");
		}
		this.attribute = null;
		this.attributeCode = QUESTION_GROUP_ATTRIBUTE_CODE;

		initialiseChildQuestions(childQuestions);
	}

	/**
	 * Constructor.
	 * 
	 * @param aCode The unique code for this empty Question Group
	 * @param aName The human readable summary name
	 */
	public Question(final String aCode, final String aName) {
		super(aCode, aName);
		if (childQuestions == null) {
			throw new InvalidParameterException("QuestionList must not be null");
		}
		this.attribute = null;
		this.attributeCode = QUESTION_GROUP_ATTRIBUTE_CODE;

	}


	public void initialiseChildQuestions(List<Question> childQuestions) {

		// Assume the list of Questions represents the order
		Double sortPriority = 10.0;
		this.setChildQuestions(new HashSet<QuestionQuestion>(0));

		for (Question childQuestion : childQuestions) {
			QuestionQuestion qq = new QuestionQuestion(this, childQuestion, sortPriority);
			this.getChildQuestions().add(qq);
			sortPriority += 10.0;
		}

	}

	/**
	 * addTarget This links this question to a target question and associated weight
	 * to the question. It auto creates the QuestionQuestion object and sets itself
	 * to be the source. For efficiency we assume the link does not already exist
	 * 
	 * @param target
	 * @param weight
	 * @throws BadDataException
	 */
	public QuestionQuestion addTarget(final Question target, final Double weight) throws BadDataException {
		if (target == null)
			throw new BadDataException("missing Target Entity");
		if (weight == null)
			throw new BadDataException("missing weight");

		final QuestionQuestion qq = new QuestionQuestion(this, target, weight);
		getChildQuestions().add(qq);
		return qq;
	}

	/**
	 * @return the attribute
	 */
	public Attribute getAttribute() {
		return attribute;
	}

	/**
	 * @param attribute the attribute to set
	 */
	public void setAttribute(final Attribute attribute) {
		this.attribute = attribute;
	}

	/**
	 * @return the contextList
	 */
	public ContextList getContextList() {
		return contextList;
	}

	/**
	 * @param contextList the contextList to set
	 */
	public void setContextList(final ContextList contextList) {
		this.contextList = contextList;
	}

	// /**
	// * @return the childQuestions
	// */
	// public Set<QuestionQuestion> getChildQuestions() {
	// return childQuestions;
	// }
	//
	// /**
	// * @param childQuestions the childQuestions to set
	// */
	// public void setChildQuestions(final Set<QuestionQuestion> childQuestions) {
	// this.childQuestions = childQuestions;
	// }

	/**
	 * @return the attributeCode
	 */
	public String getAttributeCode() {
		return attributeCode;
	}

	/**
	 * @param attributeCode the attributeCode to set
	 */
	public void setAttributeCode(final String attributeCode) {
		this.attributeCode = attributeCode;
	}

	/**
	 * getDefaultCodePrefix This method is overrides the Base class
	 * 
	 * @return the default Code prefix for this class.
	 */
	static public String getDefaultCodePrefix() {
		return DEFAULT_CODE_PREFIX;
	}

	/**
	 * @return the html
	 */
	public String getHtml() {
		return html;
	}

	/**
	 * @param html the html to set
	 */
	public void setHtml(String html) {
		this.html = html;
	}

	/**
	 * @return the mandatory
	 */
	public Boolean getMandatory() {
		return mandatory;
	}

	/**
	 * @return the placeholder
	 */
	public String getPlaceholder() {
		return placeholder;
	}

	/**
	 * @param placeholder the placeholder to set
	 */
	public void setPlaceholder(String placeholder) {
		this.placeholder = placeholder;
	}

	/**
	 * @param mandatory the mandatory to set
	 */
	public void setMandatory(Boolean mandatory) {
		this.mandatory = mandatory;
	}

	/**
	 * @return the directions
	 */
	public String getDirections() {
		return directions;
	}

	/**
	 * @param directions the directions to set
	 */
	public void setDirections(String directions) {
		this.directions = directions;
	}

	/**
	 * @return the childQuestions
	 */
	public Set<QuestionQuestion> getChildQuestions() {
		return childQuestions;
	}

	/**
	 * @param childQuestions the childQuestions to set
	 */
	public void setChildQuestions(Set<QuestionQuestion> childQuestions) {
		this.childQuestions = childQuestions;
	}

	/**
	 * addChildQuestion This adds an child Question with default weight of 0.0 to
	 * the question. It auto creates the QuestionQuestion object. For efficiency we
	 * assume the child question link does not exist
	 *
	 * @param ea
	 * @throws BadDataException
	 */
	public void addChildQuestion(final QuestionQuestion qq) throws BadDataException {
		if (qq == null)
			throw new BadDataException("missing Question");

		addChildQuestion(qq.getPk().getTargetCode(), qq.getWeight(), qq.getMandatory());
	}

	/**
	 * addChildQuestion This adds an child question and associated weight to the
	 * question group. It auto creates the QuestionQuestion object. For efficiency
	 * we assume the question link does not already exist
	 *
	 * @param childQuestion
	 * @param weight
	 * @throws BadDataException
	 */
	public void addChildQuestion(final String childQuestionCode) throws BadDataException {

		addChildQuestion(childQuestionCode, 1.0);
	}

	/**
	 * addChildQuestion This adds a child question and associated weight to the
	 * question group with no mandatory. It auto creates the QuestionQuestion
	 * object. For efficiency we assume the question link does not already exist
	 *
	 * @param childQuestion
	 * @param weight
	 * @throws BadDataException
	 */
	public void addChildQuestion(final String childQuestionCode, final Double weight) throws BadDataException {
		addChildQuestion(childQuestionCode, weight, false);
	}

	/**
	 * addChildQuestion This adds a child question and associated weight and
	 * mandatory setting to the question group. It auto creates the QuestionQuestion
	 * object. For efficiency we assume the question link does not already exist
	 *
	 * @param childQuestion
	 * @param weight
	 * @param mandatory     setting
	 * @throws BadDataException
	 */
	public QuestionQuestion addChildQuestion(final String childQuestionCode, final Double weight,
			final Boolean mandatory) throws BadDataException {
		if (childQuestionCode == null)
			throw new BadDataException("missing Question");
		if (weight == null)
			throw new BadDataException("missing weight");
		if (mandatory == null)
			throw new BadDataException("missing mandatory setting");

		log.trace("[" + this.getRealm() + "] Adding childQuestion..." + childQuestionCode + " to " + this.getCode());
		final QuestionQuestion questionLink = new QuestionQuestion(this, childQuestionCode, weight, mandatory);
		getChildQuestions().add(questionLink);
		return questionLink;
	}

	/**
	 * removeChildQuestion This removes a child Question from the question group.
	 * For efficiency we assume the child question exists
	 *
	 * @param questionCode
	 */
	public void removeChildQuestion(final String childQuestionCode) {
		final Optional<QuestionQuestion> optQuestionQuestion = findQuestionLink(childQuestionCode);
		getChildQuestions().remove(optQuestionQuestion);
	}

	/**
	 * findChildQuestion This returns an QuestionLink if it exists in the question
	 * group.
	 *
	 * @param childQuestionCode
	 * @returns Optional<QuestionQuestion>
	 */
	public Optional<QuestionQuestion> findQuestionLink(final String childQuestionCode) {
		final Optional<QuestionQuestion> foundEntity = Optional.of(getChildQuestions().parallelStream()
				.filter(x -> (x.getPk().getTargetCode().equals(childQuestionCode))).findFirst().get());

		return foundEntity;
	}

	/**
	 * findQuestionQuestion This returns an question link if it exists in the
	 * question group. Could be more efficient in retrival (ACC: test)
	 *
	 * @param questionLink
	 * @returns QuestionQuestion
	 */
	public QuestionQuestion findQuestionQuestion(final Question childQuestion) {
		final QuestionQuestion foundEntity = getChildQuestions().parallelStream()
				.filter(x -> (x.getPk().getTargetCode().equals(childQuestion.getCode()))).findFirst().get();

		return foundEntity;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.getCode() + ":" + getChildQuestionCodes();
	}


	private String getChildQuestionCodes() {
		List<QuestionQuestion> qqList = new CopyOnWriteArrayList<QuestionQuestion>(getChildQuestions());
		Collections.sort(qqList);
		String ret = "";
		if (getAttributeCode().equals(QUESTION_GROUP_ATTRIBUTE_CODE)) {
			for (QuestionQuestion childQuestion : qqList) {
				ret += childQuestion.getPk().getTargetCode() + ",";
			}
		} else {
			ret = getCode();
		}
		return ret;
	}

	/**
	 * @return the oneshot
	 */
	public Boolean getOneshot() {
		return oneshot;
	}

	/**
	 * @param oneshot the oneshot to set
	 */
	public void setOneshot(Boolean oneshot) {
		this.oneshot = oneshot;
	}

	/**
	 * @return the readonly
	 */
	public Boolean getReadonly() {
		return readonly;
	}

	/**
	 * @param readonly the readonly to set
	 */
	public void setReadonly(Boolean readonly) {
		this.readonly = readonly;
	} 

	/**
	 * @return the icon
	 */
	public String getIcon() {
		return icon;
	}

	/**
	 * @param readonly the readonly to set
	 */
	public void setIcon(String icon) {
		this.icon = icon;
	} 

}
