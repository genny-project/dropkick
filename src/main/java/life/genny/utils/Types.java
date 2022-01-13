package life.genny.utils;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.TypeLiteral;

public enum Types {
	   LIST_ELEMENT(new TypeLiteral<List<String>>(){}),
	   MAP_STRING_ELEMENT(new TypeLiteral<Map<String, String>>(){});

	   private final Type type; // + getter

	   Types(TypeLiteral type) {
	     this.type = type.getType();
	   }

	/**
	 * @return the type
	 */
	public Type getType() {
		return type;
	}
	   
	   
	}