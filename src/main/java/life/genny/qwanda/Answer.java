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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.attribute.Attribute;

/**
 * Answer is the abstract base class for all answers managed in the Qwanda
 * library. An Answer object is used as a means of storing information from a
 * source about a target attribute. This answer information includes:
 * <ul>
 * <li>The Associated Ask
 * <li>The time at which the answer was created
 * <li>The status of the answer e.g Expired, Refused, Answered
 * </ul>
 * <p>
 * Answers represent the manner in which facts about a target from sources are
 * stored. Each Answer is associated with an attribute.
 * <p>
 * 
 * 
 * @author Adam Crow
 * @author Byron Aguirre
 * @version %I%, %G%
 * @since 1.0
 */

@RegisterForReflection
public class Answer implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Long id;

	/**
	 * Stores the Created UMT DateTime that this object was created
	 */

	private LocalDateTime created;

	/**
	 * Stores the Last Modified UMT DateTime that this object was last updated
	 */

	private LocalDateTime updated;

	/**
	 * A field that stores the human readable value of the answer.
	 * <p>
	 */

	private String value;

	/**
	 * A field that stores the human readable attributecode associated with this
	 * answer.
	 * <p>
	 */

	private String attributeCode;

	private Attribute attribute;

	// @JsonInclude(JsonInclude.Include.NON_EMPTY)
	// @JsonIgnore
	// @XmlTransient
	// @Transient
	// // @OneToOne(fetch = FetchType.LAZY)
	// // @JoinColumn(name = "ask_id", nullable = true)
	// private Ask ask;

	/**
	 * Store the askId (if present)
	 */

	private Long askId;

	/**
	 * A field that stores the human readable targetcode associated with this
	 * answer.
	 * <p>
	 */

	private String targetCode;

	/**
	 * A field that stores the human readable sourcecode associated with this
	 * answer.
	 * <p>
	 */

	private String sourceCode;

	/**
	 * Store the Expired boolean value of the attribute for the baseEntity
	 */

	private Boolean expired = false;

	/**
	 * Store the Refused boolean value of the attribute for the baseEntity
	 */

	private Boolean refused = false;

	/**
	 * Store the relative importance of the attribute for the baseEntity
	 */

	private Double weight = 0.0;

	/**
	 * Store whether this answer was inferred
	 */

	private Boolean inferred = false;

	private Boolean changeEvent = false;

	// Provide a clue to any new attribute type that may be needed if the attribute
	// does not exist yet, e.g. java.util.Double
	private String dataType = null;

	private String realm;

	/**
	 * Constructor.
	 * 
	 * @param none
	 */
	@SuppressWarnings("unused")
	public Answer() {
		// dummy for hibernate
	}

	/**
	 * @return the created
	 */
	public LocalDateTime getCreated() {
		return created;
	}

	/**
	 * @param created the created to set
	 */
	public void setCreated(final LocalDateTime created) {
		this.created = created;
	}

	/**
	 * @return the updated
	 */
	public LocalDateTime getUpdated() {
		return updated;
	}

	/**
	 * @param updated the updated to set
	 */
	public void setUpdated(final LocalDateTime updated) {
		this.updated = updated;
	}

	public Date getCreatedDate() {
		final Date out = Date.from(created.atZone(ZoneId.systemDefault()).toInstant());
		return out;
	}

	public Date getUpdatedDate() {
		final Date out = Date.from(updated.atZone(ZoneId.systemDefault()).toInstant());
		return out;
	}

	/**
	 * @return the id
	 */
	public Long getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(final Long id) {
		this.id = id;
	}

	/**
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(final String value) {
		if (value != null) {
			this.value = value.trim();
		} else {
			this.value = "";
		}
	}

	/**
	 * @return the expired
	 */
	public Boolean getExpired() {
		return expired;
	}

	/**
	 * @param expired the expired to set
	 */
	public void setExpired(final Boolean expired) {
		this.expired = expired;
	}

	/**
	 * @return the refused
	 */
	public Boolean getRefused() {
		return refused;
	}

	/**
	 * @param refused the refused to set
	 */
	public void setRefused(final Boolean refused) {
		this.refused = refused;
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
	public void setWeight(final Double weight) {
		this.weight = weight;
	}

	// /**
	// * @return the ask
	// */
	// public Ask getAsk() {
	// return ask;
	// }
	//
	// /**
	// * @param ask the ask to set
	// */
	// public void setAsk(final Ask ask) {
	// this.ask = ask;
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
	 * @return the askId
	 */
	public Long getAskId() {
		return askId;
	}

	/**
	 * @param askId the askId to set
	 */
	public void setAskId(final Long askId) {
		this.askId = askId;
	}

	/**
	 * @return the inferred
	 */
	public Boolean getInferred() {
		return inferred;
	}

	/**
	 * @param inferred the inferred to set
	 */
	public void setInferred(Boolean inferred) {
		this.inferred = inferred;
	}

	/**
	 * @return the targetCode
	 */
	public String getTargetCode() {
		return targetCode;
	}

	/**
	 * @param targetCode the targetCode to set
	 */
	public void setTargetCode(final String targetCode) {
		this.targetCode = targetCode;
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
	public void setSourceCode(final String sourceCode) {
		this.sourceCode = sourceCode;
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
		if (this.dataType == null) {
			setDataType(attribute.getDataType().getClassName());
		}
	}

	/**
	 * @return the changeEvent
	 */
	public Boolean getChangeEvent() {
		return changeEvent;
	}

	/**
	 * @param changeEvent the changeEvent to set
	 */
	public void setChangeEvent(Boolean changeEvent) {
		this.changeEvent = changeEvent;
	}

	/**
	 * @return the dataType
	 */
	public String getDataType() {
		return dataType;
	}

	/**
	 * @param dataType the dataType to set
	 */
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	/**
	 * @return the realm
	 */
	public String getRealm() {
		return realm;
	}

	/**
	 * @param realm the realm to set
	 */
	public void setRealm(String realm) {
		this.realm = realm;
	}

	public String getUniqueCode() {
		return getSourceCode() + ":" + getTargetCode() + ":" + getAttributeCode();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Answer [" + realm + "," + (created != null ? "created=" + created + ", " : "")
				+ (sourceCode != null ? "sourceCode=" + sourceCode + ", " : "")
				+ (targetCode != null ? "targetCode=" + targetCode + ", " : "")
				+ (attributeCode != null ? "attributeCode=" + attributeCode + ", " : "")
				+ (value != null ? "value=" + value + ", " : "") + (askId != null ? "askId=" + askId + ", " : "")
				+ (expired != null ? "expired=" + expired + ", " : "")
				+ (refused != null ? "refused=" + refused + ", " : "")
				+ (weight != null ? "weight=" + weight + ", " : "") + (inferred != null ? "inferred=" + inferred : "")
				+ "]";
	}

	private void checkInputs() {
		if (this.sourceCode == null)
			throw new NullPointerException("SourceCode cannot be null");
		if (this.targetCode == null)
			throw new NullPointerException("targetCode cannot be null");
		if (this.attributeCode == null)
			throw new NullPointerException("attributeCode cannot be null");
	}

}
