package eu.stratosphere.peel.analyser.util;

/**
 * A parameter to be added to a query.
 * Created by Fabian on 04.01.2015.
 */
public class QueryParameter {
  private String key;
  private Object value;

  public QueryParameter(String key, Object value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }
}
