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
import java.util.Objects;

import io.quarkus.runtime.annotations.RegisterForReflection;



/**
 * A Link object
 * is used as a means of storing information from a source to a target attribute. This link
 * information includes:
 * <ul>
 * <li>The SourceCode
 * <li>The TargetCode
 * <li> The LinkCode
 * </ul>
 * <p>
 * Link class represents a simple pojo about baseentity links are stored. 
 * <p>
 * 
 * 
 * @author Adam Crow
 * @author Byron Aguirre
 * @version %I%, %G%
 * @since 1.0
 */

@RegisterForReflection
public class Link implements Serializable {
	
	static public String RULE_PARENT_OVERRIDE = "PO";
	static public String RULE_CHILD_OVERRIDE = "CO";	
	static public String RULE_NO_OVERRIDE = null;
	
  /**
   * 
   */
 
  /**
   * A field that stores the human readable attributecode associated with this link.
   * <p>
   */

  private String attributeCode;

 
  /**
   * A field that stores the human readable targetcode associated with this link.
   * <p>
   */
  private String targetCode;

  /**
   * A field that stores the human readable sourcecode associated with this link.
   * <p>
   */

  private String sourceCode;


  /**
   * A field that stores the human readable link Value associated with this link.
   * <p>
   */
  private String linkValue;
  
  private Double weight;
  
  private String childColor;

  private String parentColor;

  private String rule;

  /**
   * Constructor.
   * 
   * @param none
   */
  @SuppressWarnings("unused")
  private Link() {
    // dummy for hibernate
  }



  /**
   * Constructor.
   * 
   * @param source The source associated with this Answer
   * @param target The target associated with this Answer
   * @param attribute The attribute associated with this Answer
   */
  public Link(final String source, final String target, final String linkCode) {
   this(source, target, linkCode, "LINK");  
}

 
  /**
   * Constructor.
   * 
   * @param source The source associated with this Answer
   * @param target The target associated with this Answer
   * @param attribute The attribute associated with this Answer
   * @param value The associated String value
   */
  public Link(final String source, final String target, final String linkCode, final String linkValue) {
   this(source,target,linkCode,linkValue,0.0);
  }

  /**
   * Constructor.
   * 
   * @param source The source associated with this Answer
   * @param target The target associated with this Answer
   * @param attribute The attribute associated with this Answer
   * @param value The associated String value
   */
  public Link(final String source, final String target, final String linkCode, final String linkValue, final Double weight) {
    this.sourceCode = source;
    this.targetCode = target;
    this.attributeCode = linkCode;
    this.linkValue = linkValue;
    this.weight = weight;
    this.rule = Link.RULE_NO_OVERRIDE;
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
public void setAttributeCode(String attributeCode) {
	this.attributeCode = attributeCode;
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
public void setTargetCode(String targetCode) {
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
public void setSourceCode(String sourceCode) {
	this.sourceCode = sourceCode;
}



/**
 * @return thelinkValue 
 */
public String getLinkValue() {
	return linkValue;
}



/**
 * @param sourceCode the linkValue to set
 */
public void setLinkValue(String linkValue) {
	this.linkValue = linkValue;
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
 * @return the childColor
 */
public String getChildColor() {
	return childColor;
}



/**
 * @param childColor the childColor to set
 */
public void setChildColor(String childColor) {
	this.childColor = childColor;
}



/**
 * @return the parentColor
 */
public String getParentColor() {
	return parentColor;
}



/**
 * @param parentColor the parentColor to set
 */
public void setParentColor(String parentColor) {
	this.parentColor = parentColor;
}



/**
 * @return the rule
 */
public String getRule() {
	return rule;
}



/**
 * @param rule the rule to set
 */
public void setRule(String rule) {
	this.rule = rule;
}



@Override
public int hashCode() {
	return Objects.hash(attributeCode, childColor, linkValue, parentColor, sourceCode, targetCode, weight);
}



@Override
public boolean equals(Object obj) {
	if (this == obj) {
		return true;
	}
	if (obj == null) {
		return false;
	}
	if (!(obj instanceof Link)) {
		return false;
	}
	Link other = (Link) obj;
	return Objects.equals(attributeCode, other.attributeCode) && Objects.equals(childColor, other.childColor)
			&& Objects.equals(linkValue, other.linkValue) && Objects.equals(parentColor, other.parentColor)
			&& Objects.equals(sourceCode, other.sourceCode) && Objects.equals(targetCode, other.targetCode)
			&& Objects.equals(weight, other.weight);
}



/* (non-Javadoc)
 * @see java.lang.Object#toString()
 */
@Override
public String toString() {
	return "Link [" + (sourceCode != null ? "sourceCode=" + sourceCode + ", " : "")
			+ (targetCode != null ? "targetCode=" + targetCode + ", " : "")
			+ (attributeCode != null ? "attributeCode=" + attributeCode + ", " : "")
			+ (linkValue != null ? "linkValue=" + linkValue + ", " : "")
			+ (weight != null ? "weight=" + weight + ", " : "")
			+ (childColor != null ? "childColor=" + childColor + ", " : "")
			+ (parentColor != null ? "parentColor=" + parentColor + ", " : "") + (rule != null ? "rule=" + rule : "")
			+ "]";
}
 

}