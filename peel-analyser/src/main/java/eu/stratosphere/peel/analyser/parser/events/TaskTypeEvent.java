package eu.stratosphere.peel.analyser.parser.events;

/**
 * Created by Fabian on 26.02.2015.
 */
public class TaskTypeEvent implements Event{

  @Override public Class<?> eventType() {
    return String.class;
  }

  @Override public String eventName() {
    return "Task Type";
  }
}
