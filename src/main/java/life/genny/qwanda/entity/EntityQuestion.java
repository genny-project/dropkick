package life.genny.qwanda.entity;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.Link;

@RegisterForReflection
public class EntityQuestion implements java.io.Serializable, Comparable<Object> {

	private static final long serialVersionUID = 1L;



  private String valueString;


  private Double weight;


	private Link link;

  public EntityQuestion() {}

  public EntityQuestion(Link link) {
    this.link = link;
  }

	@Override
	public int compareTo(Object o) {
		return 0;
	}
}
