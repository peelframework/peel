package eu.stratosphere.peel.analyser.parser.events;

/**
 * Created by Fabian on 18.02.2015.
 */
public interface Event {
  /**
   Returns the Type of Event that is described by this Template
   (available: Date.class, Integer.class, Double.class, String.class, Date.class)
   */
  public Class<?> eventType();

  /**
   Returns the eventName of the Field
   */
  public String eventName();

}
