package eu.stratosphere.peel.analyser.parser.events;

/**
 * Created by Fabian on 18.02.2015.
 */
public class  Event {
  private Class eventType;
  private String eventName;

  public Event(Class<?> eventType, String eventName){
    this.eventName = eventName;
    this.eventType = eventType;
  }
  /**
   Returns the Type of Event that is described by this Template
   (available: Date.class, Integer.class, Double.class, String.class, Date.class)
   */
  public Class<?> getEventType(){
    return eventType;
  }

  /**
   Returns the getEventName of the Field
   */
  public String getEventName(){
    return eventName;
  }

}
