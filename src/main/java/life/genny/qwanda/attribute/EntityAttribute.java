package life.genny.qwanda.attribute;

import java.lang.invoke.MethodHandles;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.json.bind.annotation.JsonbTransient;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.time.DateUtils;
import org.jboss.logging.Logger;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.entity.BaseEntity;



@RegisterForReflection
public class EntityAttribute implements java.io.Serializable, Comparable<Object> {

	/**
	 * Stores logger object.
	 */
	private static final Logger log = Logger.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

	private static final long serialVersionUID = 1L;

	private String baseEntityCode;

	private String attributeCode;

	private String attributeName;

	private Boolean readonly = false;
	
	private String realm;
	

	private Integer index=0;  // used to assist with ordering 


	private String feedback = null;

	
	/**
	 * @return the baseEntityCode
	 */
	public String getBaseEntityCode() {
		return baseEntityCode;
	}

	/**
	 * @param baseEntityCode
	 *            the baseEntityCode to set
	 */
	public void setBaseEntityCode(final String baseEntityCode) {
		this.baseEntityCode = baseEntityCode;
	}

	/**
	 * @return the attributeCode
	 */
	public String getAttributeCode() {
		return attributeCode;
	}

	/**
	 * @param attributeCode
	 *            the attributeCode to set
	 */
	public void setAttributeCode(final String attributeCode) {
		this.attributeCode = attributeCode;
	}


	public EntityAttributeId pk = new EntityAttributeId();

	/**
	 * Stores the Created UMT DateTime that this object was created
	 */

	private LocalDateTime created;

	/**
	 * Stores the Last Modified UMT DateTime that this object was last updated
	 */

	private LocalDateTime updated;

	/**
	 * The following fields can be subclassed for better abstraction
	 */

	/**
	 * Store the Double value of the attribute for the baseEntity
	 */

	private Double valueDouble;

	/**
	 * Store the Boolean value of the attribute for the baseEntity
	 */

	private Boolean valueBoolean;
	/**
	 * Store the Integer value of the attribute for the baseEntity
	 */

	private Integer valueInteger;

	/**
	 * Store the Long value of the attribute for the baseEntity
	 */

	private Long valueLong;

	/**
	 * Store the LocalDateTime value of the attribute for the baseEntity
	 */

	private LocalTime valueTime;

	/**
	 * Store the LocalDateTime value of the attribute for the baseEntity
	 */

	private LocalDateTime valueDateTime;

	/**
	 * Store the LocalDate value of the attribute for the baseEntity
	 */

	private LocalDate valueDate;
	
//	@Expose
//	private Range<LocalDate> valueDateRange;

	/**
	 * Store the String value of the attribute for the baseEntity
	 */

	private String valueString;

	
	/**
	 * Store the relative importance of the attribute for the baseEntity
	 */

	private Double weight;

	/**
	 * Store the relative importance of the attribute for the baseEntity
	 */

	private Boolean inferred = false;

	/**
	 * Store the privacy of this attribute , i.e. Don't display
	 */

	private Boolean privacyFlag = false;

	/**
	 * Store the confirmation
	 */

	private Boolean confirmationFlag = false;

	/**
	 * Store the icon name for this attribute , i.e. Don't display
	 */

	private String icon;

	// @Version
	// private Long version = 1L;

	public EntityAttribute() {
	}

	/**
	 * Constructor.
	 * 
	 * @param BaseEntity
	 *            the entity that needs to contain attributes
	 * @param Attribute
	 *            the associated Attribute
	 * @param Weight
	 *            the weighted importance of this attribute (relative to the other
	 *            attributes)
	 */
	public EntityAttribute(final BaseEntity baseEntity, final Attribute attribute, Double weight) {
		setBaseEntity(baseEntity);
		setAttribute(attribute);
		if (weight == null) {
			weight = 0.0; // This permits ease of adding attributes and hides
							// attribute from scoring.
		}
		setWeight(weight);
		setReadonly(false);
	}

	/**
	 * Constructor.
	 * 
	 * @param BaseEntity
	 *            the entity that needs to contain attributes
	 * @param Attribute
	 *            the associated Attribute
	 * @param Weight
	 *            the weighted importance of this attribute (relative to the other
	 *            attributes)
	 * @param Value
	 *            the value associated with this attribute
	 */
	public EntityAttribute(final BaseEntity baseEntity, final Attribute attribute, Double weight, final Object value) {
		setBaseEntity(baseEntity);
		setAttribute(attribute);
		this.setPrivacyFlag(attribute.getDefaultPrivacyFlag());
		if (weight == null) {
			weight = 0.0; // This permits ease of adding attributes and hides
							// attribute from scoring.
		}
		setWeight(weight);
		// Assume that Attribute Validation has been performed
		if (value != null) {
			setValue(value);
		}
	}

	public EntityAttributeId getPk() {
		return pk;
	}

	public void setPk(final EntityAttributeId pk) {
		this.pk = pk;
	}


	public Attribute getAttribute() {
		return getPk().getAttribute();
	}

	public void setAttribute(final Attribute attribute) {
		getPk().setAttribute(attribute);
		this.attributeCode = attribute.getCode();
		this.attributeName = attribute.getName();
	}

	/**
	 * @return the created
	 */
	public LocalDateTime getCreated() {
		return created;
	}

	/**
	 * @param created
	 *            the created to set
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
	 * @param updated
	 *            the updated to set
	 */
	public void setUpdated(final LocalDateTime updated) {
		this.updated = updated;
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
	 * @return the valueDouble
	 */
	public Double getValueDouble() {
		return valueDouble;
	}

	/**
	 * @param valueDouble
	 *            the valueDouble to set
	 */
	public void setValueDouble(final Double valueDouble) {
		this.valueDouble = valueDouble;
	}

	/**
	 * @return the valueInteger
	 */
	public Integer getValueInteger() {
		return valueInteger;
	}

	/**
	 * @param valueInteger
	 *            the valueInteger to set
	 */
	public void setValueInteger(final Integer valueInteger) {
		this.valueInteger = valueInteger;
	}

	/**
	 * @return the valueLong
	 */
	public Long getValueLong() {
		return valueLong;
	}

	/**
	 * @param valueLong
	 *            the valueLong to set
	 */
	public void setValueLong(final Long valueLong) {
		this.valueLong = valueLong;
	}

	public LocalDate getValueDate() {
		return valueDate;
	}

	public void setValueDate(LocalDate valueDate) {
		this.valueDate = valueDate;
	}

	/**
	 * @return the valueDateTime
	 */
	public LocalDateTime getValueDateTime() {
		return valueDateTime;
	}

	/**
	 * @param valueDateTime
	 *            the valueDateTime to set
	 */
	public void setValueDateTime(final LocalDateTime valueDateTime) {
		this.valueDateTime = valueDateTime;
	}

	/**
	 * @return the valueTime
	 */
	public LocalTime getValueTime() {
		return valueTime;
	}

	/**
	 * @param valueTime
	 *            the valueTime to set
	 */
	public void setValueTime(LocalTime valueTime) {
		this.valueTime = valueTime;
	}

	/**
	 * @return the valueString
	 */
	public String getValueString() {
		return valueString;
	}

	/**
	 * @param valueString
	 *            the valueString to set
	 */
	public void setValueString(final String valueString) {
		this.valueString = valueString;
	}

	public Boolean getValueBoolean() {
		return valueBoolean;
	}

	public void setValueBoolean(Boolean valueBoolean) {
		this.valueBoolean = valueBoolean;
	}


	/**
	 * @return the privacyFlag
	 */
	public Boolean getPrivacyFlag() {
		return privacyFlag;
	}

	/**
	 * @param privacyFlag
	 *            the privacyFlag to set
	 */
	public void setPrivacyFlag(Boolean privacyFlag) {
		this.privacyFlag = privacyFlag;
	}

	/**
	 * @return the inferred
	 */
	public Boolean getInferred() {
		return inferred;
	}

	/**
	 * @param inferred
	 *            the inferred to set
	 */
	public void setInferred(Boolean inferred) {
		this.inferred = inferred;
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
	 * @return the feedback
	 */
	public String getFeedback() {
		return feedback;
	}

	/**
	 * @param feedback the feedback to set
	 */
	public void setFeedback(String feedback) {
		this.feedback = feedback;
	}

	@JsonbTransient
	public Date getCreatedDate() {
		final Date out = Date.from(created.atZone(ZoneId.systemDefault()).toInstant());
		return out;
	}

	@JsonbTransient
	public Date getUpdatedDate() {
		if (updated==null) return null;
		final Date out = Date.from(updated.atZone(ZoneId.systemDefault()).toInstant());
		return out;
	}

	@JsonbTransient
	public <T> T getValue() {
		if ((getPk()==null)||(getPk().attribute==null)) {
			return getLoopValue();
		}
		final String dataType = getPk().getAttribute().getDataType().getClassName();
		switch (dataType) {
		case "java.lang.Integer":
		case "Integer":
			return (T) getValueInteger();
		case "java.time.LocalDateTime":
		case "LocalDateTime":
			return (T) getValueDateTime();
		case "java.time.LocalTime":
		case "LocalTime":
			return (T) getValueTime();
		case "java.lang.Long":
		case "Long":
			return (T) getValueLong();
		case "java.lang.Double":
		case "Double":
			return (T) getValueDouble();
		case "java.lang.Boolean":
		case "Boolean":
			return (T) getValueBoolean();
		case "java.time.LocalDate":
		case "LocalDate":
			return (T) getValueDate();
		case "org.javamoney.moneta.Money":
		case "java.lang.String":
		default:
			return (T) getValueString();
		}
	      
	}

	@JsonbTransient
	public <T> void setValue(final Object value) {
		setValue(value,false);
	}
	
	@JsonbTransient
	public <T> void setValue(final Object value, final Boolean lock) {
		if (this.getReadonly()) {
			log.error("Trying to set the value of a readonly EntityAttribute! "+this.getBaseEntityCode()+":"+this.attributeCode);
			return; 
		}
		if (getAttribute()==null) { 
			setLoopValue(value);
			return;
		}
		if (value instanceof String) {
			String result = (String) value;
			try {
				if (getAttribute().getDataType().getClassName().equalsIgnoreCase(String.class.getCanonicalName())) {
					setValueString(result);
				} else if (getAttribute().getDataType().getClassName()
						.equalsIgnoreCase(LocalDateTime.class.getCanonicalName())) {
					List<String> formatStrings = Arrays.asList("yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
							"yyyy-MM-dd'T'HH:mm:ss.SSSZ","yyyy-MM-dd HH:mm:ss.SSSZ");
					for (String formatString : formatStrings) {
						try {
							Date olddate = new SimpleDateFormat(formatString).parse(result);
							final LocalDateTime dateTime = olddate.toInstant().atZone(ZoneId.systemDefault())
									.toLocalDateTime();
							setValueDateTime(dateTime);
							break;
							
						} catch (ParseException e) {
						}

					}
					// Date olddate = null;
					// olddate = DateTimeUtils.parseDateTime(result,
					// "yyyy-MM-dd","yyyy-MM-dd'T'HH:mm:ss","yyyy-MM-dd'T'HH:mm:ss.SSSZ");
					// final LocalDateTime dateTime =
					// olddate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
					// setValueDateTime(dateTime);
				} else if (getAttribute().getDataType().getClassName()
						.equalsIgnoreCase(LocalDate.class.getCanonicalName())) {
					Date olddate = null;
					try {
						olddate = DateUtils.parseDate(result, "M/y", "yyyy-MM-dd", "yyyy/MM/dd",
								"yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
					} catch (java.text.ParseException e) {
						olddate = DateUtils.parseDate(result, "yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
								"yyyy-MM-dd'T'HH:mm:ss.SSSZ","yyyy-MM-dd HH:mm:ss.SSSZ");
					}
					final LocalDate date = olddate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
					setValueDate(date);
				} else if (getAttribute().getDataType().getClassName()
						.equalsIgnoreCase(LocalTime.class.getCanonicalName())) {
					final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
					final LocalTime date = LocalTime.parse(result, formatter);
					setValueTime(date);
				} else if (getAttribute().getDataType().getClassName()
						.equalsIgnoreCase(Integer.class.getCanonicalName())) {
					final Integer integer = Integer.parseInt(result);
					setValueInteger(integer);
				} else if (getAttribute().getDataType().getClassName()
						.equalsIgnoreCase(Double.class.getCanonicalName())) {
					final Double d = Double.parseDouble(result);
					setValueDouble(d);
				} else if (getAttribute().getDataType().getClassName()
						.equalsIgnoreCase(Long.class.getCanonicalName())) {
					final Long l = Long.parseLong(result);
					setValueLong(l);
				} else if (getAttribute().getDataType().getClassName()
						.equalsIgnoreCase(Boolean.class.getCanonicalName())) {
					final Boolean b = Boolean.parseBoolean(result);
					setValueBoolean(b);
				} else {
					setValueString(result);
				}
			} catch (Exception e) {
				log.error("Conversion Error :" + value + " for attribute " + getAttribute() + " and SourceCode:"
						+ this.baseEntityCode);
			}
		} else {

			switch (this.getAttribute().getDataType().getClassName()) {
			case "java.lang.Integer":
			case "Integer":
				setValueInteger((Integer) value);
				break;
			case "java.time.LocalDateTime":
			case "LocalDateTime":
				setValueDateTime((LocalDateTime) value);
				break;
			case "java.time.LocalDate":
			case "LocalDate":
				setValueDate((LocalDate) value);
				break;
			case "java.lang.Long":
			case "Long":
				setValueLong((Long) value);
				break;
			case "java.time.LocalTime":
			case "LocalTime":
				setValueTime((LocalTime) value);
				break;
			case "org.javamoney.moneta.Money":
			case "java.lang.Double":
			case "Double":
				setValueDouble((Double) value);
				break;
			case "java.lang.Boolean":
			case "Boolean":
				setValueBoolean((Boolean) value);
				break;
//			case "range.LocalDate":
//				setValueDateRange((Range<LocalDate>) value);
//				break;
			case "java.lang.String":
			default:
				if (value instanceof Boolean) {
					log.error("Value is boolean being saved to String. DataType = "+this.getAttribute().getDataType().getClassName()+" and attributecode="+this.getAttributeCode());
					setValueBoolean((Boolean)value);
				} else {
				setValueString((String) value);
				}
				break;
			}
		}

		// if the lock is set then 'Lock it in Eddie!'. 
		if (lock)
		{
			this.setReadonly(true);
		}
	}


	@JsonbTransient
	public <T> void setLoopValue(final Object value) {
		setLoopValue(value,false);
	}
	
	@JsonbTransient
	public <T> void setLoopValue(final Object value, final Boolean lock) {
		if (this.getReadonly()) {
			log.error("Trying to set the value of a readonly EntityAttribute! "+this.getBaseEntityCode()+":"+this.attributeCode);
			return; 
		}


 if (value instanceof Integer)
				setValueInteger((Integer) value);
			else if (value instanceof LocalDateTime)
				setValueDateTime((LocalDateTime) value);
			else if (value instanceof LocalDate)
				setValueDate((LocalDate) value);
			else if (value instanceof Long)
				setValueLong((Long) value);
			else if (value instanceof LocalTime)
				setValueTime((LocalTime) value);
			else if (value instanceof Double)
				setValueDouble((Double) value);
			else if (value instanceof Boolean)
				setValueBoolean((Boolean) value);
//			else if (value instanceof Range<?>)
//				setValueDateRange((Range<LocalDate>) value);
			else
				setValueString((String) value);
 
			if (lock) {
				this.setReadonly(true);
			}

	}

	@JsonbTransient
	public String getAsString() {
		if ((getPk()==null)||(getPk().attribute==null)) {
			return getAsLoopString();
		}

		if(getValue() == null) {
			return null;
		}
		final String dataType = getPk().getAttribute().getDataType().getClassName();
		switch (dataType) {
		case "java.lang.Integer":
		case "Integer":
			return "" + getValueInteger();
		case "java.time.LocalDateTime":
		case "LocalDateTime":
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
			Date datetime = Date.from(getValueDateTime().atZone(ZoneId.systemDefault()).toInstant());
			String dout = df.format(datetime);
			return dout;
		case "java.lang.Long":
		case "Long":
			return "" + getValueLong();
		case "java.time.LocalTime":
		case "LocalTime":
			DateFormat df2 = new SimpleDateFormat("HH:mm");			
			String dout2 = df2.format(getValueTime());
			return dout2;
		case "org.javamoney.moneta.Money":
		case "java.lang.Double":
		case "Double":
			return getValueDouble().toString();
		case "java.lang.Boolean":
		case "Boolean":
			return getValueBoolean() ? "TRUE" : "FALSE";
		case "java.time.LocalDate":
		case "LocalDate":
			df2 = new SimpleDateFormat("yyyy-MM-dd");
			Date date = Date.from(getValueDate().atStartOfDay(ZoneId.systemDefault()).toInstant());
		    dout2 = df2.format(date);
			return dout2;

		case "java.lang.String":
		default:
			return getValueString();
		}

	}
	
	@JsonbTransient
	public String getAsLoopString() {
		String ret = "";
		if( getValueString() != null) {
			return getValueString();
		}
		if(getValueInteger() != null) {
			return getValueInteger().toString();
		}
		if(getValueDateTime() != null) {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
			Date datetime = Date.from(getValueDateTime().atZone(ZoneId.systemDefault()).toInstant());
			String dout = df.format(datetime);
			return dout;
		}
		if(getValueDate() != null) {
			DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
			Date date = Date.from(getValueDate().atStartOfDay(ZoneId.systemDefault()).toInstant());
			String dout2 = df2.format(date);
			return dout2;
		}
		if(getValueTime() != null) {
			DateFormat df2 = new SimpleDateFormat("HH:mm");			
			String dout2 = df2.format(getValueTime());
			return dout2;
		}
		if(getValueLong() != null) {
		    return getValueLong().toString();
		}
		if(getValueDouble() != null) {
		    return getValueDouble().toString();
		}
		if(getValueBoolean() != null) {
			return getValueBoolean() ? "TRUE" : "FALSE";
		}
		
		
		return ret;
		
	}
	
	@JsonbTransient
	public  <T> T getLoopValue() {
		if (getValueString() != null) {
			return  (T) getValueString();
		} else if(getValueBoolean() != null) {
			return  (T) getValueBoolean();
		} else if(getValueDateTime() != null) {
			return  (T) getValueDateTime();
		} else if(getValueDouble() != null) {
		    return  (T) getValueDouble();
		} else if(getValueInteger() != null) {
			return (T)  getValueInteger();
		}else if(getValueDate() != null) {
			return  (T) getValueDate();
		} else if(getValueTime() != null) {
			return  (T) getValueTime();
		} else if(getValueLong() != null) {
		    return  (T) getValueLong();
//		}  else if (getValueDateRange() != null) {
//			return (T) getValueDateRange();
		}
		return null;
		
	}
	@Override
	public int hashCode() {

		HashCodeBuilder hcb = new HashCodeBuilder();
		hcb.append(baseEntityCode);
		hcb.append(attributeCode);
		return hcb.toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof EntityAttribute)) {
			return false;
		}
		EntityAttribute that = (EntityAttribute) obj;
		EqualsBuilder eb = new EqualsBuilder();
		eb.append(baseEntityCode, that.baseEntityCode);
		eb.append(attributeCode, that.attributeCode);
		return eb.isEquals();
	}

	public int compareTo(Object o) {
		EntityAttribute myClass = (EntityAttribute) o;
		final String dataType = getPk().getAttribute().getDataType().getClassName();
		switch (dataType) {
		case "java.lang.Integer":
		case "Integer":
			return new CompareToBuilder().append(this.getValueInteger(), myClass.getValueInteger()).toComparison();
		case "java.time.LocalDateTime":
		case "LocalDateTime":
			return new CompareToBuilder().append(this.getValueDateTime(), myClass.getValueDateTime()).toComparison();
		case "java.time.LocalTime":
		case "LocalTime":
			return new CompareToBuilder().append(this.getValueTime(), myClass.getValueTime()).toComparison();
		case "java.lang.Long":
		case "Long":
			return new CompareToBuilder().append(this.getValueLong(), myClass.getValueLong()).toComparison();
		case "java.lang.Double":
		case "Double":
			return new CompareToBuilder().append(this.getValueDouble(), myClass.getValueDouble()).toComparison();
		case "java.lang.Boolean":
		case "Boolean":
			return new CompareToBuilder().append(this.getValueBoolean(), myClass.getValueBoolean()).toComparison();
		case "java.time.LocalDate":
		case "LocalDate":
			return new CompareToBuilder().append(this.getValueDate(), myClass.getValueDate()).toComparison();
		case "org.javamoney.moneta.Money":
		case "java.lang.String":
		default:
			return new CompareToBuilder().append(this.getValueString(), myClass.getValueString()).toComparison();

		}

	}



	@Override
	public String toString() {
		return "attributeCode=" + attributeCode + ", value="
				+ getObjectAsString() + ", weight=" + weight + ", inferred=" + inferred + "] be="+this.getBaseEntityCode();
	}


	@JsonbTransient
	public <T> T getObject() {

		if (getValueInteger() != null) {
			return (T) getValueInteger();
		}

		if (getValueDateTime() != null) {
			return (T) getValueDateTime();
		}

		if (getValueLong() != null) {
			return (T) getValueLong();
		}

		if (getValueDouble() != null) {
			return (T) getValueDouble();
		}

		if (getValueBoolean() != null) {
			return (T) getValueBoolean();
		}

		if (getValueDate() != null) {
			return (T) getValueDate();
		}
		if (getValueTime() != null) {
			return (T) getValueTime();
		}

		if (getValueString() != null) {
			return (T) getValueString();
		}

		return (T) getValueString();

	}

	@JsonbTransient
	public String getObjectAsString() {

		if (getValueInteger() != null) {
			return "" + getValueInteger();
		}

		if (getValueDateTime() != null) {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
			Date datetime = Date.from(getValueDateTime().atZone(ZoneId.systemDefault()).toInstant());
			String dout = df.format(datetime);
			return dout;
		}

		
		if (getValueLong() != null) {
			return "" + getValueLong();
		}

		if (getValueDouble() != null) {
			return getValueDouble().toString();
		}

		if (getValueBoolean() != null) {
			return getValueBoolean() ? "TRUE" : "FALSE";
		}

		if (getValueDate() != null) {
			DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
			Date date = Date.from(getValueDate().atStartOfDay(ZoneId.systemDefault()).toInstant());
			String dout2 = df2.format(date);
			return dout2;
		}
		if (getValueTime() != null) {

			String dout2 = getValueTime().toString();
			return dout2;
		}

		if (getValueString() != null) {
			return getValueString();
		}
		
//		if (getValueDateRange() != null) {
//			return getValueDateRange().toString();
//		}

		return getValueString();

	}

	/**
	 * @return the attributeName
	 */
	public String getAttributeName() {
		return attributeName;
	}

	/**
	 * @param attributeName the attributeName to set
	 */
	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}

	/**
	 * @return the index
	 */
	public Integer getIndex() {
		return index;
	}

	/**
	 * @param index the index to set
	 */
	public void setIndex(Integer index) {
		this.index = index;
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

	/**
	 * @param icon the name of the icon to display
	 */
	public void setIcon(String icon) {
		this.icon = icon;
	}

	/**
	 * @return the icon
	 */
	public String getIcon() {
		return icon;
	}

	public Boolean getConfirmationFlag() {
		return confirmationFlag;
	}

	public void setConfirmationFlag(Boolean confirmationFlag) {
		this.confirmationFlag = confirmationFlag;
	}
	

	
	// @Transient
	// @JsonIgnore
	// @XmlTransient
	// public BaseEntity getBaseEntity() {
	// return getPk().getBaseEntity();
	// }

	public void setBaseEntity(final BaseEntity baseEntity) {
		getPk().setBaseEntity(baseEntity);
		this.baseEntityCode = baseEntity.getCode();
		this.realm = baseEntity.getRealm();
	}
}
