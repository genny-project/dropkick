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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;



import com.fasterxml.jackson.annotation.JsonIgnore;

import io.quarkus.runtime.annotations.RegisterForReflection;


/**
 * AttributeList represents a set of Attributes in the Qwanda library.
 * The attributes in the list can be applied to a passed value.
 * <ul>
 * <li>List of Attribute 
 * </ul>
 * <p>
 * 
 * <p>
 * 
 * 
 * @author      Adam Crow
 * @author      Byron Aguirre
 * @version     %I%, %G%
 * @since       1.0
 */


@RegisterForReflection
public class AttributeList implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	A fieldlist that stores the attributes for this object.
	<p>
	*/


	private List<Attribute> attributeList = new CopyOnWriteArrayList<Attribute>();

 
	
	/**
	  * Constructor.
	  * 
	  * @param none
	  */
	
	public AttributeList(List<Attribute> attributes) 
	{
		this.attributeList = attributes;
	}



	/**
	 * @return the attributeList
	 */
	public List<Attribute> getAttributeList() {
		return attributeList;
	}



	/**
	 * @param attributeList the attributeList to set
	 */
	public void setAttributeList(List<Attribute> attributeList) {
		this.attributeList = attributeList;
	}



}
