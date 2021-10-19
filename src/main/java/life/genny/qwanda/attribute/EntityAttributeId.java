package life.genny.qwanda.attribute;

import javax.json.bind.annotation.JsonbTransient;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.entity.BaseEntity;

@RegisterForReflection
public class EntityAttributeId implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@JsonbTransient
	public BaseEntity baseEntity;

	public Attribute attribute;




	public Attribute getAttribute() {
		return attribute;
	}

	public void setAttribute(final Attribute attribute) {
		this.attribute = attribute;
	}

	public void setBaseEntity(final BaseEntity baseEntity) {
		this.baseEntity = baseEntity;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EntityAttributeId other = (EntityAttributeId) obj;
		if (attribute == null) {
			if (other.attribute != null)
				return false;
		} else if (!attribute.equals(other.attribute))
			return false;
		return true;
	}




}
