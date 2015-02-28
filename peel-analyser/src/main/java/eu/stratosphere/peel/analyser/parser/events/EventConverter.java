package eu.stratosphere.peel.analyser.parser.events;

import eu.stratosphere.peel.analyser.model.TaskInstanceEvents;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Fabian on 28.02.2015.
 */
public class EventConverter {
  private Set<Event> events = new HashSet<>();
  private static final Logger LOGGER = LoggerFactory
		  .getLogger(EventConverter.class);

  public EventConverter() {
    events = EventFinder.findEvents();
  }

  public Set<TaskInstanceEvents> getTaskInstanceEventsByLine(String line) {
    JSONObject jsonObject = new JSONObject(line);
    HashSet<TaskInstanceEvents> taskInstanceEventsHashSet = new HashSet<>();
    for (Event event : events) {
      Object result = getJsonEntry(jsonObject, event.getEventName());
      if (result != null) {
	taskInstanceEventsHashSet.add(convertToEvent(result, event.getEventName(),
			event.getEventType()));
      }
    }
    return taskInstanceEventsHashSet;
  }

  private Object getJsonEntry(JSONObject jsonObject, String name) {

    Object result = "";
    try {
      result = jsonObject.get(name);
      return result;
    } catch (JSONException e){
    }

    Set<String> childObjects = jsonObject.keySet();
    for (String child : childObjects) {
      if(child.equals(name)){
	return jsonObject.getString(name);
      }
      JSONObject childJSON;
      try {
	 childJSON = jsonObject.getJSONObject(child);
      } catch (JSONException e){
	continue;
      }

      if (childJSON != null) {
	result = getJsonEntry(childJSON, name);
	if (result != null) {
	  return result;
	}
      }
    }
    return null;
  }

  private TaskInstanceEvents convertToEvent(Object input, String name,
		  Class<?> convertType) {
    TaskInstanceEvents event = new TaskInstanceEvents();
    event.setEventName(name);
    try {
      if (convertType == String.class) {
	event.setValueVarchar((String) input);
	return event;
      } else if (convertType == Double.class) {
	event.setValueDouble(Double.valueOf((Double) input));
	return event;
      } else if (convertType == Integer.class) {
	event.setValueInt(Integer.valueOf((Integer) input));
	return event;
      } else if (convertType == Date.class) {
	event.setValueTimestamp(new Date(Long.valueOf((Long) input)));
	return event;
      }
    } catch (Exception e) {
      LOGGER.error("Error in converting the input value " + input
		      + " into type " + convertType.getCanonicalName(), e);
    }
    LOGGER.error("Wasn't able to convert to type " + convertType);
    return null;
  }
}
