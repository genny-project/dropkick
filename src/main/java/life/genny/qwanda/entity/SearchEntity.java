package life.genny.qwanda.entity;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;
import java.util.UUID;

import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.annotation.JsonbTransient;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.EEntityStatus;
import life.genny.qwanda.attribute.AttributeBoolean;
import life.genny.qwanda.attribute.AttributeDate;
import life.genny.qwanda.attribute.AttributeDateTime;
import life.genny.qwanda.attribute.AttributeDouble;
import life.genny.qwanda.attribute.AttributeInteger;
import life.genny.qwanda.attribute.AttributeLong;
import life.genny.qwanda.attribute.AttributeText;
import life.genny.qwanda.attribute.AttributeTime;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.exception.BadDataException;


/* SearchEntity class implements the search of base entities applying different filters/search to the
 * baseEntity and its attributes
 */
@RegisterForReflection
public class SearchEntity extends BaseEntity {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	public static final String DEFAULT_PAGETYPE = EPageType.DEFAULT.toString();

	Double filterIndex = 1.0;
	Double colIndex = 1.0;

	Double actionIndex = 1.0;
	Double searchActionIndex = 1.0;

	Double sortIndex = 0.0;
	@JsonbTransient
	Double flcIndex = 1.0;
	Double groupIndex = 1.0;
	Double searchIndex = 1.0;
	Double combinedSearchIndex = 1.0;
	Double aliasIndex = 1.0;

	/*
	 * This Sort Enum is used to sort the search results in either Ascending and
	 * descending order
	 */
	public enum Sort {
		ASC {
			public String toString() {
				return "ASC";
			}
		},
		DESC {
			public String toString() {
				return "DESC";
			}
		}
	}

	/*
	 * This StringFilter Enum is used to put the filter to the search entity. It
	 * filters the string values in the attributes
	 */
	public enum StringFilter {
		EQUAL {
			public String toString() {
				return SearchEntity.convertToSaveable("=");
			}
		},
		NOT_EQUAL {
			public String toString() {
				return SearchEntity.convertToSaveable("!=");
			}
		},
		LIKE {
			public String toString() {
				return "LIKE";
			}
		},
		NOT_LIKE {
			public String toString() {
				return "NOT LIKE";
			}
		},
		RLIKE {
			public String toString() {
				return "RLIKE";
			}
		},
		NOT_RLIKE {
			public String toString() {
				return "NOT RLIKE";
			}
		},
		REGEXP {
			public String toString() {
				return "REGEXP";
			}
		},
		NOT_REGEXP {
			public String toString() {
				return "NOT REGEXP";
			}
		}

	}

	/*
	 * This Filter Enum is used to put the filter to the search entity. It filtesr
	 * the numeric and bit masked values of the attributes
	 */
	public enum Filter {
		EQUALS {
			public String toString() {
				return SearchEntity.convertToSaveable("=");
			}
		},
		NOT_EQUALS {
			public String toString() {
				return SearchEntity.convertToSaveable("!=");
			}
		},
		GREATER_THAN {
			public String toString() {
				return SearchEntity.convertToSaveable(">");
			}
		},
		GREATER_THAN_AND_EQUAL {
			public String toString() {
				return SearchEntity.convertToSaveable(">=");
			}
		},
		LESS_THAN_AND_EQUAL {
			public String toString() {
				return SearchEntity.convertToSaveable("<=");
			}
		},
		LESS_THAN {
			public String toString() {
				return SearchEntity.convertToSaveable("<");
			}
		},
		BIT_MASK_POSITIVE {
			public String toString() {
				return SearchEntity.convertToSaveable("&+");
			}
		},
		BIT_MASK_ZERO {
			public String toString() {
				return SearchEntity.convertToSaveable("&0");
			}
		}
	}

	static public SearchEntity.Filter convertOperatorToFilter(final String operator) {
			SearchEntity.Filter ret = null;
				switch (operator) {
				case ">": ret =  SearchEntity.Filter.GREATER_THAN; break;
				case "<": ret =  SearchEntity.Filter.LESS_THAN; break;
				case ">=": ret =  SearchEntity.Filter.GREATER_THAN_AND_EQUAL; break;
				case "<=": ret =  SearchEntity.Filter.LESS_THAN_AND_EQUAL; break;
				case "<>":
				case "!=": ret =  SearchEntity.Filter.NOT_EQUALS;break;
				default:
					ret = SearchEntity.Filter.EQUALS;
				}
				return ret;
				
		
		
	}
	
	static public SearchEntity.StringFilter convertOperatorToStringFilter(final String operator) {
		SearchEntity.StringFilter ret = null;
			switch (operator) {
			case "REGEXP": ret =  SearchEntity.StringFilter.REGEXP; break;
			case "NOT REGEXP": ret =  SearchEntity.StringFilter.NOT_REGEXP; break;
			case "RLIKE": ret =  SearchEntity.StringFilter.RLIKE; break;
			case "NOT RLIKE": ret =  SearchEntity.StringFilter.NOT_RLIKE; break;
			case "LIKE": ret =  SearchEntity.StringFilter.LIKE; break;
			case "NOT LIKE": ret =  SearchEntity.StringFilter.NOT_LIKE; break;
			case "<>":
			case "!=": ret =  SearchEntity.StringFilter.NOT_EQUAL;break;
			default:
				ret = SearchEntity.StringFilter.EQUAL;
			}
			return ret;
			
	
	
}
	
	
	public void convertBEToSaveable() {
		for (EntityAttribute ea : this.getBaseEntityAttributes()) {
			if (!((ea.getAttributeCode().startsWith("COL_")) || (ea.getAttributeCode().startsWith("CAL_"))
					|| (ea.getAttributeCode().startsWith("SCH_")))) {
				String name = ea.getAttributeName();
				name = name.replaceAll(">", "_GT_");
				name = name.replaceAll("<", "_LT_");
				name = name.replaceAll(">=", "_GTE_");
				name = name.replaceAll("<=", "_LTE_");
				name = name.replaceAll("=", "_EQ_");
				name = name.replaceAll("&", "_AMP_");
				name = name.replaceAll("+", "_PLUS_");
				name = name.replaceAll("-", "_MINUS_");
				name = name.replaceAll("&+", "_BPLUS_");
				name = name.replaceAll("&0", "_BZERO_");
				ea.setAttributeName(name);
			}
		}
	}

	public void convertBEFromSaveable() {
		for (EntityAttribute ea : this.getBaseEntityAttributes()) {
			if (!((ea.getAttributeCode().startsWith("COL_")) || (ea.getAttributeCode().startsWith("CAL_"))
					|| (ea.getAttributeCode().startsWith("SCH_")))) {
				String name = ea.getAttributeName();
				name = name.replaceAll("_GT_", ">");
				name = name.replaceAll("_GTE_", ">=");
				name = name.replaceAll("_LT_", "<");
				name = name.replaceAll("_LTE_", "<=");
				name = name.replaceAll("_EQ_", "=");
				name = name.replaceAll("_AMP_", "&");
				name = name.replaceAll("_PLUS_", "+");
				name = name.replaceAll("_MINUS_", "-");
				name = name.replaceAll("_BPLUS_", "&+");
				name = name.replaceAll("_BZERO_", "&0");
				ea.setAttributeName(name);
			}
		}
	}

	public static String convertToSaveable(String value) {
		String name = value.replaceAll("\\>", "_GT_");
		name = name.replaceAll("\\<", "_LT_");
		name = name.replaceAll("\\=", "_EQ_");
		name = name.replaceAll("\\&", "_AMP_");
		name = name.replaceAll("\\+", "_PLUS_");
		name = name.replaceAll("\\-", "_MINUS_");
		name = name.replaceAll("\\!", "_NOT_");
		return name;
	}

	public static String convertFromSaveable(String value) {
		if (value != null) {
			String name = value;
			name = name.replaceAll("_GT_", ">");
			name = name.replaceAll("_LT_", "<");
			name = name.replaceAll("_EQ_", "=");
			name = name.replaceAll("_AMP_", "&");
			name = name.replaceAll("_PLUS_", "+");
			name = name.replaceAll("_MINUS_", "-");
			name = name.replaceAll("_NOT_", "!");
			return name;
		} else {
			return null;
		}
	}

	/* Constructor to create SearchEntity with code and name */
	public SearchEntity() {
		this("SBE_" + UUID.randomUUID().toString().substring(0, 15), "Or Search");
	}

	/* Constructor to create SearchEntity with code and name */
	public SearchEntity(final String code, final String name) {
		super(code, name);
		setPageStart(0);
		setPageSize(20);
		setTitle(name);
		setPageType(DEFAULT_PAGETYPE);
	}

	/* Constructor to create SearchEntity passing BaseEntity */
	public SearchEntity(final BaseEntity be) {
		super(be.getCode(), be.getName());
		this.setCreated(be.getCreated());
		this.setUpdated(be.getUpdated());
		this.setBaseEntityAttributes(be.getBaseEntityAttributes());
	}

	/*
	 * This method allows to add the attributes to the SearchEntity that is required
	 * in the result BaseEntities
	 */
	public SearchEntity addColumn(final String attributeCode, final String columnName) {
		AttributeText attributeColumn = new AttributeText("COL_" + attributeCode, columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, colIndex);
			ea.setIndex(colIndex.intValue());
			colIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Column Initialisation");
		}
		return this;
	}

	/*
	 * This method allows to add the attributes to the SearchEntity that is required
	 * in the result BaseEntities
	 * 
	 * Additionally, you can pass the name of the icon to be shown.
	 */
	public SearchEntity addColumn(final String attributeCode, final String columnName, final String iconName) {
		AttributeText attributeColumn = new AttributeText("COL_" + attributeCode, columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, colIndex);
			ea.setIndex(colIndex.intValue());
			ea.setIcon(iconName);
			colIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Column Initialisation");
		}
		return this;
	}

		/*
	 * This method allows to add the action attributes to the SearchEntity that is
	 * required in the result BaseEntities
	 */
	public SearchEntity addAction(final String attributeCode, final String columnName) {
		return this.addAction(attributeCode, columnName, false);
	}

	public SearchEntity addAction(final String attributeCode, final String columnName, Boolean confirmationFlag) {
		AttributeText attributeColumn = new AttributeText("ACT_" + attributeCode, columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, actionIndex);
			ea.setIndex(actionIndex.intValue());
			ea.setConfirmationFlag(confirmationFlag);
			actionIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Column Initialisation");
		}
		return this;
	}

	/*
	 * This method allows to add the action attributes to the SearchEntity that is
	 * required in the result BaseEntities
	 */
	public SearchEntity addDefaultAction(final String attributeCode, final String columnName) {
		AttributeText attributeColumn = new AttributeText("ACT_" + attributeCode, columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, 0.0);
			ea.setIndex(0);
		} catch (BadDataException e) {
			log.error("Bad Column Initialisation");
		}
		return this;
	}

	/*
	 * This method allows to add the search action attributes to the SearchEntity
	 * not each result BaseEntities
	 */
	public SearchEntity addSearchAction(final String attributeCode, final String columnName) {
		AttributeText attributeColumn = new AttributeText("SCH_ACT_" + attributeCode, columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, searchActionIndex);
			ea.setIndex(searchActionIndex.intValue());
			searchActionIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Column Initialisation");
		}
		return this;
	}

	/*
	 * This method allows to add the default search action attributes to the
	 * SearchEntity not each result BaseEntities
	 */
	public SearchEntity addDefaultSearchAction(final String attributeCode, final String columnName) {
		AttributeText attributeColumn = new AttributeText("SCH_ACT_" + attributeCode, columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, 0.0);
			ea.setIndex(0);
		} catch (BadDataException e) {
			log.error("Bad Column Initialisation");
		}
		return this;
	}

	/*
	 * This method allows to add the row action attribute to the
	 * each result BaseEntities
	 */
	public SearchEntity addRowAction(final String attributeCode, final String columnName) {
		AttributeText attributeColumn = new AttributeText("ROW_ACT_" + attributeCode, columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, 0.0);
			ea.setIndex(0);
		} catch (BadDataException e) {
			log.error("Bad Column Initialisation");
		}
		return this;
	}

	/*
	 * This method allows to add the linked searchcodes to the SearchEntity as
	 * required
	 */
	public SearchEntity addLinkedSearch(final String searchCode, final String columnName) {
		AttributeText attributeColumn = new AttributeText(searchCode, columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, searchIndex);
			ea.setIndex(searchIndex.intValue());
			searchIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Linked Search Initialisation");
		}
		return this;
	}

	public SearchEntity addLinkedSearch(final String searchCode, final String association, final String columnName) {
		AttributeText attributeColumn = new AttributeText(searchCode+"."+association, columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, searchIndex);
			ea.setIndex(searchIndex.intValue());
			searchIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Linked Search Initialisation");
		}
		return this;
	}

	/*
	 * This method allows to add the combined searches to the SearchEntity.
	 * This will combine the results with the two searches
	 * NOTE: has only been implemented for counts so far
	 */
	public SearchEntity addCombinedSearch(final String attributeCode, final String columnName) {
		AttributeText attributeColumn = new AttributeText("CMB_"+attributeCode, columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, searchIndex);
			ea.setIndex(combinedSearchIndex.intValue());
			combinedSearchIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Combined Search Initialisation");
		}
		return this;
	}

	/*
	 * This method allows to add the associated attributes to the SearchEntity that
	 * is required in the result BaseEntities
	 */
	public SearchEntity addAssociatedColumn(final String attributeCode, final String associatedLinkedBaseEntityCodeAttribute,
			final String columnName) {
		AttributeText attributeColumn = new AttributeText("COL__" + attributeCode.toUpperCase()+"__"+associatedLinkedBaseEntityCodeAttribute.toUpperCase(), columnName);
		try {
			EntityAttribute ea = addAttribute(attributeColumn, colIndex);
			ea.setValue(associatedLinkedBaseEntityCodeAttribute);
			ea.setIndex(colIndex.intValue());
			colIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Associated Column Initialisation");
		}
		return this;
	}

	/*
	 * This method allows nested associated columns
	 */
	public SearchEntity addAssociatedColumn(final String attributeCode, String nestedAttributeCode,
			final String associatedLinkedBaseEntityCodeAttribute, final String columnName) {
		return addAssociatedColumn(attributeCode + "__" + nestedAttributeCode, associatedLinkedBaseEntityCodeAttribute, columnName);
	}

	/*
	 * This method allows double nested associated columns
	 */
	public SearchEntity addAssociatedColumn(final String attributeCode, String nestedAttributeCode, String doubleNestedAttributeCode,
			final String associatedLinkedBaseEntityCodeAttribute, final String columnName) {
		return addAssociatedColumn(attributeCode + "__" + nestedAttributeCode + "__" + doubleNestedAttributeCode, associatedLinkedBaseEntityCodeAttribute, columnName);
	}

	/*
	 * This method allows to add sorting to the attributes of the search results It
	 * can either sort in ascending or descending order
	 */
	public SearchEntity addSort(final String attributeCode, final String sortHelpText, final Sort sortType) {
		AttributeText attributeSort = new AttributeText("SRT_" + attributeCode, sortHelpText);
		try {
			addAttribute(attributeSort, sortIndex, sortType.toString());
			sortIndex += 1.0;

		} catch (BadDataException e) {
			log.error("Bad Sort Initialisation");
		}

		return this;
	}

	public SearchEntity addSortAttribute(final String attributeCode, final String aName) {
		AttributeText attributeSort = new AttributeText("ATTRSRT_" + attributeCode, aName);
		try {
			addAttribute(attributeSort, 1.0);
		} catch (BadDataException e) {
			log.error("Bad Sort Initialisation");
		}
		return this;
	}

	/**
	* This Method allows specifying columns that can be further filtered on by the user
	* @param attributeCode The code of the attribute
	* @param fName The name given to the filter column
	* @return SearchEntity the updated search base entity
	 */
	public SearchEntity addFilterableColumn(final String attributeCode, final String fName) {
		AttributeText attributeFLC = new AttributeText("FLC_" + attributeCode, fName);
		try {
			addAttribute(attributeFLC, flcIndex);
			flcIndex += 1.0;

		} catch (BadDataException e) {
			log.error("Bad Filterable Column Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to add grouping by a specific attribute. all BEs who's
	 * value corresponding to this attribute will be grouped together.
	 * NOTE: not implemented yet
	 */
	public SearchEntity addGroupBy(final String groupBy) {
		AttributeText attribute = new AttributeText("GPB_" + groupBy, "GroupBy");
		try {
			addAttribute(attribute, groupIndex, groupBy);
			groupIndex += 1.0;

		} catch (BadDataException e) {
			log.error("Bad Group By Initialisation");
		}

		return this;
	}

	public SearchEntity addAlias(final String attributeCode, final String alias) {
		AttributeText attribute = new AttributeText("ALS_" + attributeCode, alias);
		try {
			addAttribute(attribute, aliasIndex);
			aliasIndex += 1.0;

		} catch (BadDataException e) {
			log.error("Bad Alias Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set the filter for the integer value in the search
	 * 
	 * @param attributeCode - the attributeCode which holds integer value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
	public SearchEntity addFilter(final String attributeCode, final Filter filterType, final Integer value) {
		AttributeInteger attribute = new AttributeInteger(attributeCode, filterType.toString());

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Integer Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set the filter for the Long value in the search
	 * 
	 * @param attributeCode - the attributeCode which holds long value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
	public SearchEntity addFilter(final String attributeCode, final Filter filterType, final Long value) {
		AttributeLong attribute = new AttributeLong(attributeCode, filterType.toString());

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Long Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set the filter for the Double value in the search
	 * 
	 * @param attributeCode - the attributeCode which holds long value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
	public SearchEntity addFilter(final String attributeCode, final Filter filterType, final Double value) {
		AttributeDouble attribute = new AttributeDouble(attributeCode, filterType.toString());

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Double Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set the filter for the LocalDateTime value in the
	 * search
	 * 
	 * @param attributeCode - the attributeCode which holds LocalDateTime value
	 * where we apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
	public SearchEntity addFilter(final String attributeCode, final Filter filterType, final LocalDateTime value) {
		AttributeDateTime attribute = new AttributeDateTime(attributeCode, filterType.toString());

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad DateTime Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set the filter for the LocalDate value in the
	 * search
	 * 
	 * @param attributeCode - the attributeCode which holds LocalDate value
	 * where we apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
	public SearchEntity addFilter(final String attributeCode, final Filter filterType, final LocalDate value) {
		AttributeDate attribute = new AttributeDate(attributeCode, filterType.toString());

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Date Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set the filter for the LocalDate value in the
	 * search
	 * 
	 * @param attributeCode - the attributeCode which holds LocalDate value
	 * where we apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
	public SearchEntity addFilter(final String attributeCode, final Filter filterType, final LocalTime value) {
		AttributeTime attribute = new AttributeTime(attributeCode, filterType.toString());

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Date Filter Initialisation");
		}

		return this;
	}


	/*
	 * This method allows to set the filter for the Boolean value in the search
	 * 
	 * @param attributeCode - the attributeCode which holds Boolean value where we
	 * apply the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
	public SearchEntity addFilter(final String attributeCode, final Boolean value) {
		AttributeBoolean attribute = new AttributeBoolean(attributeCode, "=");

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Double Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set the filter for the String value in the search
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the string filter
	 * 
	 * @param value - filter against (search for) this value
	 */
	public SearchEntity addFilter(final String attributeCode, final StringFilter filterType, final String value) {
		AttributeText attribute = new AttributeText(attributeCode, filterType.toString());

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad String Filter Initialisation");
		}
		
		return this;
	}

	/*
	 * This method allows to set the filter for the LocalDate value in the
	 * search
	 * 
	 * @param attributeCode - the attributeCode which holds LocalDate value
	 * where we apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
	public SearchEntity addFilterAsString(final String attributeCode, final Filter filterType, final String value) {
		AttributeText attribute = new AttributeText(attributeCode, filterType.toString());

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad Filter As String Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set an OR filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addOr(final String attributeCode, final Filter filterType, final Integer value) {
		AttributeInteger attribute = new AttributeInteger(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "OR") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("OR_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad OR Integer Filter Initialisation");
		}

		return this;
	}
    
	/*
	 * This method allows to set an OR filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addOr(final String attributeCode, final Filter filterType, final Long value) {
		AttributeLong attribute = new AttributeLong(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "OR") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("OR_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad OR Long Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set an OR filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addOr(final String attributeCode, final Filter filterType, final Double value) {
		AttributeDouble attribute = new AttributeDouble(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "OR") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("OR_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad OR Double Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set an OR filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addOr(final String attributeCode, final Filter filterType, final LocalDateTime value) {
		AttributeDateTime attribute = new AttributeDateTime(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "OR") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("OR_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad OR LocalDateTime Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set an OR filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addOr(final String attributeCode, final Filter filterType, final LocalDate value) {
		AttributeDate attribute = new AttributeDate(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "OR") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("OR_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad OR LocalDate Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set an OR filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the string filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addOr(final String attributeCode, final StringFilter filterType, final String value) {
		AttributeText attribute = new AttributeText(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "OR") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("OR_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad OR String Filter Initialisation");
		}

		return this;
	}
    
	/*
	 * This method allows to set an AND filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addAnd(final String attributeCode, final Filter filterType, final Integer value) {
		AttributeInteger attribute = new AttributeInteger(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "AND") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("AND_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad AND Integer Filter Initialisation");
		}

		return this;
	}
    
	/*
	 * This method allows to set an AND filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addAnd(final String attributeCode, final Filter filterType, final Long value) {
		AttributeLong attribute = new AttributeLong(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "AND") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("AND_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad AND Long Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set an AND filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addAnd(final String attributeCode, final Filter filterType, final Double value) {
		AttributeDouble attribute = new AttributeDouble(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "AND") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("AND_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad AND Double Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set an AND filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addAnd(final String attributeCode, final Filter filterType, final LocalDateTime value) {
		AttributeDateTime attribute = new AttributeDateTime(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "AND") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("AND_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad AND LocalDateTime Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set an AND filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addAnd(final String attributeCode, final Filter filterType, final LocalDate value) {
		AttributeDate attribute = new AttributeDate(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "AND") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("AND_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad AND LocalDate Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set an AND filter for an attribute
	 * 
	 * @param attributeCode - the attributeCode which holds String value where we
	 * apply the filter
	 * 
	 * @param filterType - type of the string filter
	 * 
	 * @param value - filter against (search for) this value
	 */
    
	public SearchEntity addAnd(final String attributeCode, final StringFilter filterType, final String value) {
		AttributeText attribute = new AttributeText(attributeCode, filterType.toString());
		Integer count = countOccurrences(attributeCode, "AND") + 1;

		for (int i = 0; i < count; i++) {
			attribute.setCode("AND_"+attribute.getCode());
		}

		try {
			addAttribute(attribute, filterIndex, value);
			filterIndex += 1.0;
		} catch (BadDataException e) {
			log.error("Bad AND String Filter Initialisation");
		}

		return this;
	}

	public SearchEntity addConditional(String attributeCode, String condition) {

		Optional<EntityAttribute> existing = findEntityAttribute("CND_"+attributeCode);
		if (existing.isPresent()) {
			existing.get().setValue(existing.get().getValue().toString() + ":::" + condition);
		} else {
			AttributeText attribute = new AttributeText("CND_"+attributeCode, "CND_"+attributeCode);
			try {
				log.info("Adding a CND = " + attributeCode);
				addAttribute(attribute, 1.0, condition);
				filterIndex += 1.0;
			} catch (BadDataException e) {
				log.error("Bad Conditional Initialisation");
			}
		}
		return this;
	}
	/*
	 * This method allows to set the LinkWeight to the resulted BaseEntities to its
	 * parent
	 * 
	 * @param value - value/linkWeight to be set
	 */
	public SearchEntity setLinkWeight(final Double value) {
		AttributeDouble attribute = new AttributeDouble("SCH_LINK_WEIGHT", "LinkWeight");
		try {
			addAttribute(attribute, 1.0, value);
		} catch (BadDataException e) {
			log.error("Bad String Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set the filter based on the linkweight value of
	 * BaseEntities to its parent
	 * 
	 * @param filterType - type of the filter set to the linkWeight
	 */
	public SearchEntity addFilterToLinkWeight(final Filter filterType) {
		AttributeText attribute = new AttributeText("SCH_LINK_FILTER", "LinkFilterByWeight");
		try {
			addAttribute(attribute, 1.0, filterType.toString());
		} catch (BadDataException e) {
			log.error("Bad String Filter Initialisation");
		}

		return this;
	}

	/*
	 * This method allows to set the title of the results data to be sent
	 * 
	 * @param title - The page Title
	 */
	public SearchEntity setTitle(final String title) {
		AttributeText attributeTitle = new AttributeText("SCH_TITLE", "Title");
		try {
			addAttribute(attributeTitle, 5.0, title);
		} catch (BadDataException e) {
			log.error("Bad Title ");
		}

		return this;
	}

		/*
	 * This method allows to set the parentCode of the SearchEntity
	 * 
	 * @param title - parentCode
	 */
	public SearchEntity setParentCode(final String parentCode) {
		AttributeText attributeTitle = new AttributeText("SCH_PARENT_CODE", "Parent Code");
		try {
			addAttribute(attributeTitle, 1.0, parentCode);
		} catch (BadDataException e) {
			log.error("Bad Title ");
		}

		return this;
	}

	/*
	 * This method allows to set the start/begining number of the range(page) of the
	 * results data to be sent
	 * 
	 * @param pageStart - start of the page number
	 */
	public SearchEntity setPageStart(final Integer pageStart) {
		AttributeInteger attributePageStart = new AttributeInteger("SCH_PAGE_START", "PageStart");
		try {
			addAttribute(attributePageStart, 3.0, pageStart);
		} catch (BadDataException e) {
			log.error("Bad Page Start ");
		}

		return this;
	}

	/*
	 * This method allows to set size of the selection allowed for a searchEntity
	 * 
	 * @param pageStart - size of selection
	 */
	public SearchEntity setSelectSize(final Integer selectSize) {
		AttributeInteger attributeSelectSize = new AttributeInteger("SCH_SELECT_SIZE", "SelectSize");
		try {
			addAttribute(attributeSelectSize, 1.0, selectSize);
		} catch (BadDataException e) {
			log.error("Bad Page Start ");
		}

		return this;
	}

	/*
	 * This method allows to set the total number of the results (BaseEntites) to be
	 * sent
	 * 
	 * @param pageSize - total number of data's to be sent
	 */
	public SearchEntity setPageSize(final Integer pageSize) {
		AttributeInteger attributePageSize = new AttributeInteger("SCH_PAGE_SIZE", "PageSize");
		try {
			addAttribute(attributePageSize, 3.0, pageSize);
		} catch (BadDataException e) {
			log.error("Bad Page Size");
		}

		return this;
	}

	/*
	 * This method allows to set the stakeholder/user code to the search. It will
	 * search for the BaseEntites that the given user is stakeholder of.
	 * 
	 * @param stakeholderCode - userCode
	 */
	public SearchEntity setStakeholder(final String stakeholderCode) {
		AttributeText attribute = new AttributeText("SCH_STAKEHOLDER_CODE", "Stakeholder");
		try {
			addAttribute(attribute, 1.0, stakeholderCode);
		} catch (BadDataException e) {
			log.error("Bad Stakeholder");
		}

		return this;
	}

	/*
	 * This method allows to set the stakeholder/user code to the parent/source
	 * Basentity involved in the search. It will search for the BaseEntites under
	 * the give source BE that the given user is stakeholder of.
	 * 
	 * @param sourceStakeholderCode - userCode
	 */
	public SearchEntity setSourceStakeholder(final String sourceStakeholderCode) {
		AttributeText attribute = new AttributeText("SCH_SOURCE_STAKEHOLDER_CODE", "SourceStakeholder");
		try {
			addAttribute(attribute, 1.0, sourceStakeholderCode);
		} catch (BadDataException e) {
			log.error("Bad Source Stakeholder");
		}

		return this;
	}

	/*
	 * This method allows to set the stakeholder/user code to the parent/source
	 * Basentity involved in the search. It will search for the BaseEntites under
	 * the give source BE that the given user is stakeholder of.
	 * 
	 * @param sourceStakeholderCode - userCode
	 */
	public SearchEntity setLinkCode(final String linkCode) {
		AttributeText attribute = new AttributeText("SCH_LINK_CODE", "LinkCode");
		try {
			addAttribute(attribute, 1.0, linkCode);
		} catch (BadDataException e) {
			log.error("Bad Stakeholder");
		}

		return this;
	}

	/*
	 * This method allows to set the link value the result of the search.
	 * 
	 * @param linkValue - linkValue of the sourceCode to the results (BaseEntities)
	 * of the search
	 */
	public SearchEntity setLinkValue(final String linkValue) {
		AttributeText attribute = new AttributeText("SCH_LINK_VALUE", "LinkValue");
		try {
			addAttribute(attribute, 1.0, linkValue);
		} catch (BadDataException e) {
			log.error("Bad Link Value");
		}

		return this;
	}

	public SearchEntity setSourceCode(final String sourceCode) {
		AttributeText attribute = new AttributeText("SCH_SOURCE_CODE", "SourceCode");
		try {
			addAttribute(attribute, 1.0, sourceCode);
		} catch (BadDataException e) {
			log.error("Bad SourceCode");
		}
		return this;
	}

	public SearchEntity setTargetCode(final String targetCode) {
		AttributeText attribute = new AttributeText("SCH_TARGET_CODE", "TargetCode");
		try {
			addAttribute(attribute, 1.0, targetCode);
		} catch (BadDataException e) {
			log.error("Bad Target Code");
		}

		return this;
	}

	public SearchEntity setDisplayMode(final String displayMode) {
		AttributeText attribute = new AttributeText("SCH_DISPLAY_MODE", "DisplayMode");
		try {
			addAttribute(attribute, 1.0, displayMode);
		} catch (BadDataException e) {
			log.error("Bad Display Mode");
		}

		return this;
	}

	public SearchEntity setSearchQuestionCode(final String questionCode) {
		AttributeText attribute = new AttributeText("SCH_QUESTION_CODE", "Question Code");
		try {
			addAttribute(attribute, 1.0, questionCode);
		} catch (BadDataException e) {
			log.error("Bad Question Code!");
		}

		return this;
	}

	/**
	* Set the validation attribute.
	* The search will then look to this attribute to find its validation state.
	* @param validationAttribute
	* @return SearchEntity the updated search
	 */
	public SearchEntity setValidationAttribute(final String validationAttribute) {
		AttributeText attribute = new AttributeText("SCH_VALIDATION_ATTRIBUTE", "ValidationAttribute");
		try {
			addAttribute(attribute, 1.0, validationAttribute);
		} catch (BadDataException e) {
			log.error("Bad Validation Attribute");
		}

		return this;
	}

	/**
	* This method allows users to set the dropdown target.
	* Used to pass information about the entity concerning a dropdown.
	* @param dropdownTarget A code, or other information about the target
	* entity of a dropdown.
	 */
	public SearchEntity setDropdownTarget(final String dropdownTarget) {
		AttributeText attribute = new AttributeText("SCH_DROPDOWN_TARGET", "Dropdown Target");
		try {
			addAttribute(attribute, 1.0, dropdownTarget);
		} catch (BadDataException e) {
			log.error("Bad Dropdown Target");
		}

		return this;
	}

	/*
	 * This method allows to set thewildcard of the results data to be sent
	 * 
	 * @param wildcard - the widlcard
	 */
	public SearchEntity setWildcard(String wildcard) {
		// only allow characters
		wildcard = wildcard.replaceAll(("[^A-Za-z0-9 ]"), "");

		AttributeText attributeWildcard = new AttributeText("SCH_WILDCARD", "Wildcard");
		try {
			addAttribute(attributeWildcard, 1.0, wildcard);
		} catch (BadDataException e) {
			log.error("Bad Wildcard ");
		}

		return this;
	}

	/*
	 * This method allows to set the status of the result BEs
	 * 
	 * @param wildcard - the widlcard
	 */
	public SearchEntity setSearchStatus(EEntityStatus status) {

		AttributeInteger attributeStatus = new AttributeInteger("SCH_STATUS", "Status");
		try {
			addAttribute(attributeStatus, 1.0, status.ordinal());
		} catch (BadDataException e) {
			log.error("Bad Search Status");
		}

		return this;
	}
	
	/*
	 * This method allows to set the cachable of the result BEs for initial page
	 * 
	 * @param cachable - true or false. true means cache the result for subsequent lookup
	 */
	public SearchEntity setCachable(Boolean cachable) {

		AttributeBoolean attributeCachable = new AttributeBoolean("SCH_CACHABLE", "Cachable");
		try {
			addAttribute(attributeCachable, 1.0, cachable);
		} catch (BadDataException e) {
			log.error("Bad Search cachable");
		}
		
		return this;
	}

	/*
	 * This method allows to set the type of range data that the search relates to.
	 * This is important for pagination that needs to page across data spans such as
	 * Months, days, weeks, years, etc.
	 * 
	 * @param pageStart - start of the page number
	 */

	public SearchEntity setPageType(final String pageType) {
		return setPageType(EPageType.valueOf(pageType));
	}

	public SearchEntity setPageType(final EPageType pageType) {
		AttributeText attributePageStart = new AttributeText("SCH_PAGE_TYPE", "PageType");
		try {
			addAttribute(attributePageStart, 3.0, pageType.toString());
			if (!EPageType.DEFAULT.equals(pageType)) {
				switch (pageType.getCategory()) {
					case DATE:
						// Now set a default PageIndexDate
						AttributeDate pageDate = new AttributeDate("SCH_PAGE_DATE", "Page Date");
						try {
							addAttribute(pageDate, 1.0, LocalDateTime.now());
						} catch (BadDataException e) {
							log.error("Bad Wildcard ");
						}

						break;
					case DATETIME:
						// Now set a default PageIndexDate
						AttributeDateTime pageDateTime = new AttributeDateTime("SCH_PAGE_DATETIME", "Page DateTime");
						try {
							addAttribute(pageDateTime, 1.0, LocalDateTime.now());
						} catch (BadDataException e) {
							log.error("Bad Wildcard ");
						}

						break;

					case GROUP:
						AttributeText pageText = new AttributeText("SCH_PAGE_TEXT", "Page Text");
						try {
							addAttribute(pageText, 1.0, "");
						} catch (BadDataException e) {
							log.error("Bad Wildcard ");
						}

						break;
					default:
				}
			}
		} catch (BadDataException e) {
			log.error("Bad Page Start ");
		}

		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		 Jsonb jsonb = JsonbBuilder.create();
		 return jsonb.toJson(this);
	}

	public Integer getPageStart(Integer defaultPageSize) {
		Integer pageStart = getValue("SCH_PAGE_START", defaultPageSize);
		return pageStart;
	}

	public Integer getPageSize(Integer defaultPageSize) {
		Integer pageSize = getValue("SCH_PAGE_SIZE", defaultPageSize);
		return pageSize;
	}

	public Double getColIndex() {
		return colIndex;
	}

	public void setColIndex(Double colIndex) {
		this.colIndex = colIndex;
	}

	public Double getSortIndex() {
		return sortIndex;
	}

	public void setSortIndex(Double sortIndex) {
		this.sortIndex = sortIndex;
	}

	@JsonbTransient
	public Double getFLCIndex() {
		return flcIndex;
	}
	

	public void setFLCIndex(Double flcIndex) {
		this.flcIndex = flcIndex;
	}

	public Double getActionIndex() {
		return actionIndex;
	}

	public void setActionIndex(Double actionIndex) {
		this.actionIndex = actionIndex;
	}

	public Double getSearchActionIndex() {
		return searchActionIndex;
	}

	public void setSearchActionIndex(Double searchActionIndex) {
		this.searchActionIndex = searchActionIndex;
	}

	public Double getGroupIndex() {
		return groupIndex;
	}

	public void setGroupIndex(Double groupIndex) {
		this.groupIndex = groupIndex;
	}

	public Double getSearchIndex() {
		return searchIndex;
	}

	public void setSearchIndex(Double searchIndex) {
		this.searchIndex = searchIndex;
	}

	/*
	 * This method allows to remove the attributes from the SearchEntity
	 */
	public SearchEntity removeColumn(final String attributeCode) {
		removeAttribute("COL_" + attributeCode);
		return this;
	}

	/*
	 * This method allows to set the total number of the results (BaseEntites) from
	 * the search
	 * 
	 * @param totalResults - total results from the search
	 */
	public SearchEntity setTotalResults(final Integer totalResults) {
		AttributeInteger attributeTotalResults = new AttributeInteger("PRI_TOTAL_RESULTS", "Total Results");
		try {
			addAttribute(attributeTotalResults, 3.0, totalResults);
		} catch (BadDataException e) {
			log.error("Bad Total Results");
		}

		return this;
	}

	/*
	 * This method allows to set the page index of the search
	 * 
	 * @param totalResults - page index of the search
	 */
	public SearchEntity setPageIndex(final Integer pageIndex) {
		AttributeInteger attributePageIndex = new AttributeInteger("PRI_INDEX", "Page Index");
		try {
			addAttribute(attributePageIndex, 3.0, pageIndex);
		} catch (BadDataException e) {
			log.error("Bad Page Index");
		}

		return this;
	}

	/*
	 * This method helps calculate the index of an OR filter
	 * 
	 * @param attributeCode - the attributeCode for which to count
	 * @param prefix - prefix to count occurences of
	 */
	public Integer countOccurrences(final String attributeCode, final String prefix) {
        Integer count = -1;
        for (EntityAttribute ea : this.getBaseEntityAttributes()) {
            if (ea.getAttributeCode().endsWith(attributeCode)) {
                Integer occurs = ( ea.getAttributeCode().split(prefix+"_", -1).length ) - 1;
                if (occurs > count) {
                    count = occurs;
                }
            }
        }
		return count;
	}

	@JsonbTransient
	public Double getMaximumFilterWeight() {

		Double maxWeight = 0.0;
		for (EntityAttribute ea : getBaseEntityAttributes()) {
			if (ea.getAttributeCode().startsWith("PRI_") || ea.getAttributeCode().startsWith("LNK_") ||
				ea.getAttributeCode().startsWith("AND_") || ea.getAttributeCode().startsWith("OR_")) {
				if (ea.getWeight() > maxWeight) {
					maxWeight = ea.getWeight();
				}
			}
		}
		return maxWeight;
	}

	public void setFilterIndex(Double filterIndex) {
			this.filterIndex = filterIndex;
	}

	public Double getFilterIndex() {
			return this.filterIndex;
	}
}
