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

package life.genny.qwanda;

import java.io.Serializable;



import org.apache.commons.lang3.builder.CompareToBuilder;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.entity.BaseEntity;



/**
 * Context is the class for all entity contexts managed in the Qwanda library. A
 * Context object is used as a means of supplying information about a question
 * that assists in understanding what answer is required. This context
 * information includes:
 * <ul>
 * <li>The name of the context class
 * <li>The context unique code
 * <li>The key String for this context e.g. "employee" or "footballer" this is
 * saved in name field
 * </ul>
 * <p>
 * Contexts represent the major way of supplying info about a question that
 * permits a source to make a full decision. Contexts are also used in message
 * merging.
 * <p>
 *
 *
 * @author Adam Crow
 * @author Byron Aguirre
 * @version %I%, %G%
 * @since 1.0
 */

@RegisterForReflection
public class Context extends CoreEntity implements Serializable {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	private BaseEntity entity;

	private Double weight = 1.0;

	private String contextCode;

	private String dataType;
	
	private String dttCode;


	/**
	 * Constructor.
	 *
	 * @param none
	 */
	@SuppressWarnings("unused")
	public Context() {
		// dummy for hibernate
	}


	/**
	 * @return the entity
	 */
	public BaseEntity getEntity() {
		return entity;
	}
	/**
	 * @param the entity to set
	 */
	public void setEntity(BaseEntity aEntity) {
		this.entity = aEntity;
		this.contextCode = aEntity.getCode();
	}

	/**
	 * @return the weight
	 */
	public Double getWeight() {
		return weight;
	}

	/**
	 * @param weight
	 *            the weight to set
	 */
	public void setWeight(final Double weight) {
		this.weight = weight;
	}
	
	/**
	 * @return the contextCode
	 */
	public String getContextCode() {
		return contextCode;
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
	 * @return the dttCode
	 */
	public String getDttCode() {
		return dttCode;
	}

	/**
	 * @param dttCode the dttCode to set
	 */
	public void setDttCode(String dttCode) {
		this.dttCode = dttCode;
	}

	@Override
	 public int compareTo(Object o) {
		 Context myClass = (Context) o;
	     return new CompareToBuilder()
	       .append(entity,myClass.getEntity())
	       .toComparison();
	   }

	@Override
	public String toString() {
		return "Context [entity=" + entity + ", weight=" + weight + ", contextCode=" + contextCode + ", dataType="
				+ dataType + ", visualControlType="  + "]";
	}

	


}
