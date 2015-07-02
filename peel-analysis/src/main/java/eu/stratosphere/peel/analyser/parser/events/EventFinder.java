package eu.stratosphere.peel.analyser.parser.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Fabian on 28.02.2015.
 */
public class EventFinder {

  private static final Logger LOGGER = LoggerFactory
		  .getLogger(EventFinder.class);
  private static final Set<Event> EVENTS = new HashSet<>();

  static {
    Event runTimeEvent = new Event(Integer.class, "Executor Run Time");
    EVENTS.add(runTimeEvent);
  }
  public static Set<Event> findEvents() {
    return EVENTS;
  }
}
