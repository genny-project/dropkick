/*
 * (C) Copyright 2017 GADA Technology (http://www.outcome-hub.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Adam Crow
 *     Byron Aguirre
 */

package life.genny.qwanda.attribute;

import java.io.Serializable;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.CodedEntity;
import life.genny.qwanda.datatype.DataType;

/**
 * Attribute represents a distinct abstract Fact about a target entity
 * managed in the Qwanda library.
 * An attribute may be used directly in processing meaning for a target
 * entity. Such processing may be in relation to a comparison score against
 * another target entity, or to generate more attribute information via
 * inference and induction  This
 * attribute information includes:
 * <ul>
 * <li>The Human Readable name for this attibute (used for summary lists)
 * <li>The unique code for the attribute
 * <li>The description of the attribute
 * <li>The answerType that represents the format of the attribute
 * </ul>
 * <p>
 * Attributes represent facts about a target.
 * <p>
 * 
 * 
 * @author      Adam Crow
 * @author      Byron Aguirre
 * @version     %I%, %G%
 * @since       1.0
 */


@RegisterForReflection
public class Attribute extends CodedEntity implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String DEFAULT_CODE_PREFIX = "PRI_";

//	@OneToMany(fetch = FetchType.EAGER, mappedBy = "pk.attribute")
//	@JsonManagedReference(value="attribute")
//	 @JsonIgnore
//	private Set<EntityAttribute> baseEntityAttributes = new HashSet<EntityAttribute>(0);


	public DataType dataType;
	

	private Boolean defaultPrivacyFlag = false;

	private String description;
	
	private String help;
	
	private String placeholder;
	
	private String defaultValue;
	
    private String icon;

	
	/**
	  * Constructor.
	  * 
	  * @param none
	  */
	@SuppressWarnings("unused")
	protected Attribute()
	{
		super();
		// dummy for hibernate
	}
	

	public Attribute(String aCode, String aName, DataType dataType)
	{
		super(aCode, aName);
		setDataType(dataType);
	}
	
	/**
	 * @return the dataType
	 */
	public DataType getDataType() {
		return dataType;
	}

	/**
	 * @param dataType the dataType to set
	 */
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

	/**
	 * getDefaultCodePrefix This method is overrides the Base class
	 * 
	 * @return the default Code prefix for this class.
	 */
	static public String getDefaultCodePrefix() {
		return DEFAULT_CODE_PREFIX;
	}


	@Override
	public String toString() {
		return getCode()+ ",dataType=" + dataType;
	}


	/**
	 * @return the defaultPrivacyFlag
	 */
	public Boolean getDefaultPrivacyFlag() {
		return defaultPrivacyFlag;
	}


	/**
	 * @param defaultPrivacyFlag the defaultPrivacyFlag to set
	 */
	public void setDefaultPrivacyFlag(Boolean defaultPrivacyFlag) {
		this.defaultPrivacyFlag = defaultPrivacyFlag;
	}


	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}


	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}


	/**
	 * @return the help
	 */
	public String getHelp() {
		return help;
	}


	/**
	 * @param help the help to set
	 */
	public void setHelp(String help) {
		this.help = help;
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
	 * @return the defaultValue
	 */
	public String getDefaultValue() {
		return defaultValue;
	}


	/**
	 * @param defaultValue the defaultValue to set
	 */
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	/*@Override
	public String toString() {
		return "Attribute:"+getCode()+"(" + getDataType()+") ";
	}*/
	
    public void setIcon(String icon) {
        this.icon = icon;
    }

    public String getIcon() {
        return this.icon;
    }

}
