package life.genny.qwanda;

import javax.json.bind.annotation.JsonbTransient;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.entity.BaseEntity;

@RegisterForReflection
public class AnswerLinkId implements java.io.Serializable {


  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@JsonbTransient
private BaseEntity source;

	@JsonbTransient
  private BaseEntity target;

	@JsonbTransient
  private Attribute attribute;


  /**
   * @return the source
   */
  public BaseEntity getSource() {
    return source;
  }

  /**
   * @param source the source to set
   */
  public void setSource(final BaseEntity source) {
    this.source = source;
  }

  /**
   * @return the target
   */
  public BaseEntity getTarget() {
    return target;
  }

  /**
   * @param target the target to set
   */
  public void setTarget(final BaseEntity target) {
    this.target = target;
  }



  /**
   * @return the linkAttribute
   */
  public Attribute getAttribute() {
    return attribute;
  }

  /**
   * @param linkAttribute the linkAttribute to set
   */
  public void setAttribute(final Attribute attribute) {
    this.attribute = attribute;
  }



  // /**
  // * @return the ask
  // */
  // public Ask getAsk() {
  // return ask;
  // }
  //
  // /**
  // * @param ask the ask to set
  // */
  // public void setAsk(final Ask ask) {
  // this.ask = ask;
  // }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final AnswerLinkId that = (AnswerLinkId) o;

    if (source != null ? !source.equals(that.source) : that.source != null)
      return false;
    if (target != null ? !target.equals(that.target) : that.target != null)
      return false;
    if (attribute != null ? !attribute.equals(that.attribute) : that.attribute != null)
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result;
    result = (source != null ? source.hashCode() : 0);
    result = 31 * result + (target != null ? target.hashCode() : 0);
    result = 127 * result + (attribute != null ? attribute.hashCode() : 0);
    return result;
  }

}
