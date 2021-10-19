package life.genny.qwanda;



import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.entity.EntityEntityId;


@RegisterForReflection
public class QuestionQuestionId implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	private Question source;
	

	private String targetCode;
	private String sourceCode;
	

	


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

	//
	/**
	 * @return the source
	 */

	public Question getSource() {
		return source;
	}

	/**
	 * @param source the source to set
	 */
	public void setSource(final Question source) {
		this.source = source;
		this.sourceCode = source.getCode();
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

	@Override
    public int hashCode() {
//        int result;
        HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(sourceCode);
        hcb.append(targetCode);
        return hcb.toHashCode();
    }      
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof QuestionQuestionId)) {
                return false;
            }
            QuestionQuestionId that = (QuestionQuestionId) obj;
            EqualsBuilder eb = new EqualsBuilder();
            eb.append(sourceCode, that.sourceCode);
            eb.append(targetCode, that.targetCode);
            return eb.isEquals();
        }
        

}
