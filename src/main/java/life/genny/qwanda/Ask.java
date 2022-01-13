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

import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;


import javax.validation.Valid;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.commons.lang3.builder.CompareToBuilder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.exception.BadDataException;



/**
 * Ask represents the presentation of a Question to a source entity. A Question
 * object is refered to as a means of requesting information from a source about
 * a target attribute. This ask information includes:
 * <ul>
 * <li>The source of the answer (Who is being asked the question?)
 * <li>The target of the answer (To whom does the answer refer to?)
 * <li>The text that presents the question to the source
 * <li>The context entities that relate to the question
 * <li>The associated Question object
 * <li>The expiry duration that should be required to answer.
 * <li>The media used to ask this question.
 * <li>The associated answers List
 * </ul>
 * <p>
 * Asks represent the major way of retrieving facts (answers) about a target
 * from sources. Each ask is associated with an question which represents one or
 * more distinct fact about a target.
 * <p>
 * 
 * 
 * @author Adam Crow
 * @author Byron Aguirre
 * @version %I%, %G%
 * @since 1.0
 */

@RegisterForReflection
public class Ask extends CoreEntity implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Question question;

	private String sourceCode;
	private String targetCode;
	private String questionCode;
	private String attributeCode;

	private Boolean mandatory = false;
	private Boolean oneshot = false;

	private Boolean disabled = false;
	private Boolean hidden = false;

	private Boolean readonly = false;
	private Double weight = 0.0;

	private Long parentId = 0L;

	private Boolean formTrigger = false;
	
	private Boolean createOnTrigger = false;

	private Ask[] childAsks;

	// @Embedded
	// @Valid
	// @JsonInclude(Include.NON_NULL)
	// private AnswerList answerList;

	private ContextList contextList;

	/**
	 * Constructor.
	 * 
	 * @param none
	 */
	@SuppressWarnings("unused")
	public Ask() {
		// dummy for hibernate
	}



	/**
	 * @return the question
	 */
	public Question getQuestion() {
		return question;
	}

	/**
	 * @param question the question to set
	 */
	public void setQuestion(final Question question) {
		this.question = question;
		this.questionCode = question.getCode();
		this.attributeCode = question.getAttributeCode(); // .getAttribute().getCode();
	}

	// /**
	// * @return the answerList
	// */
	// public AnswerList getAnswerList() {
	// return answerList;
	// }

	// /**
	// * @param answerList the answerList to set
	// */
	// public void setAnswerList(final AnswerList answerList) {
	// this.answerList = answerList;
	// }

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

	/**
	 * @return the mandatory
	 */
	public Boolean getMandatory() {
		return mandatory;
	}

	/**
	 * @param mandatory the mandatory to set
	 */
	public void setMandatory(Boolean mandatory) {
		this.mandatory = mandatory;
	}

	/**
	 * @return the weight
	 */
	public Double getWeight() {
		return weight;
	}

	/**
	 * @param weight the weight to set
	 */
	public void setWeight(Double weight) {
		this.weight = weight;
	}

	/**
	 * @return the sourceCode
	 */
	public String getSourceCode() {
		return sourceCode;
	}

	/**
	 * @param sourceCode the sourceCode to set
	 */
	private void setSourceCode(final String sourceCode) {
		this.sourceCode = sourceCode;
	}

	/**
	 * @return the targetCode
	 */
	public String getTargetCode() {
		return targetCode;
	}

	/**
	 * @return the questionCode
	 */
	public String getQuestionCode() {
		return questionCode;
	}

	/**
	 * @param questionCode the questionCode to set
	 */
	public void setQuestionCode(final String questionCode) {
		this.questionCode = questionCode;
	}

	/**
	 * @return the disabled
	 */
	public Boolean getDisabled() {
		return disabled;
	}

	/**
	 * @param disabled the disabled to set
	 */
	public void setDisabled(Boolean disabled) {
		this.disabled = disabled;
	}

	/**
	 * @return the hidden
	 */
	public Boolean getHidden() {
		return hidden;
	}

	/**
	 * @param hidden the hidden to set
	 */
	public void setHidden(Boolean hidden) {
		this.hidden = hidden;
	}

	/**
	 * @return the parentId
	 */
	public Long getParentId() {
		return parentId;
	}

	/**
	 * @param parentId the parentId to set
	 */
	public void setParentId(Long parentId) {
		this.parentId = parentId;
	}

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
	 * @param targetCode the targetCode to set
	 */
	public void setTargetCode(final String targetCode) {
		this.targetCode = targetCode;
	}

	public void add(final Answer answer) throws BadDataException {
		if ((answer.getSourceCode().equals(sourceCode)) && (answer.getTargetCode().equals(targetCode))
				&& (answer.getAttributeCode().equals(attributeCode))) {
			// getAnswerList().getAnswerList().add(new AnswerLink(source, target, answer));
		} else {
			throw new BadDataException("Source / Target ids do not match Ask");
		}

	}

	@Override
	public int compareTo(Object o) {
		Ask myClass = (Ask) o;
		return new CompareToBuilder().append(questionCode, myClass.getQuestionCode())
				.append(targetCode, myClass.getTargetCode()).toComparison();
	}

	/**
	 * @return the childAsks
	 */

	public Ask[] getChildAsks() {
		return childAsks;
	}

	/**
	 * @param childAsks the childAsks to set
	 */
	public void setChildAsks(Ask[] childAsks) {
		this.childAsks = childAsks;
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
	 * @return the formTrigger
	 */
	public Boolean getFormTrigger() {
		return formTrigger;
	}

	/**
	 * @param formTrigger the formTrigger to set
	 */
	public void setFormTrigger(Boolean formTrigger) {
		this.formTrigger = formTrigger;
	}

	
	
	/**
	 * @return the createOnTrigger
	 */
	public Boolean getCreateOnTrigger() {
		return createOnTrigger;
	}

	/**
	 * @param createOnTrigger the createOnTrigger to set
	 */
	public void setCreateOnTrigger(Boolean createOnTrigger) {
		this.createOnTrigger = createOnTrigger;
	}


	public Boolean hasTriggerQuestion() {
		// recurse through the childAsks
		// this is used to tell if intermediate BaseEntity is to be created and then
		// copied in upon a trigger question
		if (this.formTrigger) {
			return true;
		} else {
			if ((this.childAsks!=null)&&(this.childAsks.length > 0)) {
				for (Ask childAsk : this.childAsks) {
					return childAsk.hasTriggerQuestion();
				}
			}
		}
		return false;
	}
	
	public static Ask clone(Ask ask) {
		Ask newAsk = new Ask();
		newAsk.sourceCode = ask.getSourceCode();
		newAsk.targetCode = ask.getTargetCode();
		newAsk.questionCode = ask.getQuestionCode();
		newAsk.question = ask.getQuestion();
		newAsk.attributeCode = ask.getAttributeCode();
		newAsk.mandatory = ask.getMandatory();
		newAsk.oneshot = ask.getOneshot();
		newAsk.disabled = ask.getDisabled();
		newAsk.readonly = ask.getReadonly();
		newAsk.weight = ask.getWeight();
		newAsk.parentId = ask.getParentId();
		newAsk.formTrigger = ask.getFormTrigger();
		newAsk.createOnTrigger = ask.getCreateOnTrigger();
		if(ask.getChildAsks() != null && ask.getChildAsks().length > 0){
			newAsk.childAsks = ask.getChildAsks();
		}
		if(ask.getContextList() != null ){
			newAsk.contextList = ask.getContextList();
		}
		return newAsk;
	}


	
}
