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
import java.util.List;

import io.quarkus.runtime.annotations.RegisterForReflection;




/**
 * ContextList represents a set of Contexts in the Qwanda library.
 * The Contexts in the list can be applied to a passed value.
 * <ul>
 * <li>List of Context 
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
public class ContextList implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	A fieldlist that stores the Contexts for this object.
	<p>
	*/


	private List<Context> contexts;

 
	
	/**
	  * Constructor.
	  * 
	  * @param none
	  */
	
	public ContextList()
	{
		
	}
	
	public ContextList(List<Context> Contexts) 
	{
		this.contexts = Contexts;
	}

	/**
	 * @return the contexts
	 */
	public List<Context> getContexts() {
		return contexts;
	}

	/**
	 * @param contexts the contexts to set
	 */
	public void setContexts(List<Context> contexts) {
		this.contexts = contexts;
	}

	/**
	 * @return the contexts
	 */
	public List<Context> getContextList() {
		return contexts;
	}

	/**
	 * @param contexts the contexts to set
	 */
	public void setContextList(List<Context> contexts) {
		this.contexts = contexts;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String ret = "ContextList->";
		for (Context context : contexts) {
			ret += context+",";
		}
		return ret;
	}


	
}
