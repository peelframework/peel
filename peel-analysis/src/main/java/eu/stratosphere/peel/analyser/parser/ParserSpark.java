package eu.stratosphere.peel.analyser.parser;

import eu.stratosphere.peel.analyser.exception.PeelAnalyserException;
import eu.stratosphere.peel.analyser.model.ExperimentRun;
import eu.stratosphere.peel.analyser.model.Task;
import eu.stratosphere.peel.analyser.model.TaskInstance;
import eu.stratosphere.peel.analyser.model.TaskInstanceEvents;
import eu.stratosphere.peel.analyser.parser.events.EventConverter;
import eu.stratosphere.peel.analyser.util.HibernateUtil;
import eu.stratosphere.peel.analyser.util.ORM;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Date;
import java.util.Set;

/**
 * Created by Fabian on 02.11.2014.
 */
public class ParserSpark implements Parser {
  private ExperimentRun experimentRun;
  private Date firstEntry = null;
  private Date lastEntry = null;
  private boolean skipInstances;
  private ORM orm = HibernateUtil.getORM();
  private EventConverter eventConverter;

  private static final Logger LOGGER = LoggerFactory
		  .getLogger(ParserSpark.class);

  public ParserSpark(boolean skipInstances) {
    this.skipInstances = skipInstances;
    eventConverter = new EventConverter();
  }

  public ParserSpark(ExperimentRun experimentRun) {
    this.experimentRun = experimentRun;
    eventConverter = new EventConverter();
  }

  public ExperimentRun getExperimentRun() {
    return experimentRun;
  }

  public void setExperimentRun(ExperimentRun experimentRun) {
    this.experimentRun = experimentRun;
  }

  public void parse(BufferedReader in)
		  throws IOException, PeelAnalyserException {
    Task task;
    orm.beginTransaction();

    String line;
    while ((line = in.readLine()) != null) {
      if (ParserSparkHelper.getEvent(line).equals("SparkListenerTaskEnd")
		      && !skipInstances) {
	task = getTaskByLine(line);
	task.getTaskInstances().add(getTaskInstanceByLine(line, task));
	orm.update(experimentRun);
      } else if (ParserSparkHelper.getEvent(line)
		      .equals("SparkListenerApplicationStart")) {
	setSubmitTime(line);
      } else if (ParserSparkHelper.getEvent(line).equals("SparkListenerTaskEnd")
		      && skipInstances) {
	if (lastEntry == null) {
	  lastEntry = ParserSparkHelper.getFinishTime(line);
	} else if (lastEntry.getTime() < ParserSparkHelper.getFinishTime(line)
			.getTime()) {
	  lastEntry = ParserSparkHelper.getFinishTime(line);
	}
	if (firstEntry == null) {
	  firstEntry = ParserSparkHelper.getLaunchTime(line);
	}
      }
    }
    experimentRun.setDeployed(firstEntry);
    experimentRun.setFinished(lastEntry);
    orm.update(experimentRun);
    orm.commitTransaction();
  }

  /**
   * this method will parse the line and will create a new TaskInstance object with the parsed information
   *
   * @param input a logfile line
   * @return TaskInstance with all related events
   */
  private TaskInstance getTaskInstanceByLine(String input, Task task) {
    TaskInstance taskInstance = new TaskInstance();
    taskInstance.setTask(task);
    taskInstance.setSubTaskNumber(ParserSparkHelper.getTaskID(input));
    orm.save(taskInstance);

    TaskInstanceEvents eventLaunch = new TaskInstanceEvents();
    eventLaunch.setEventName("Launch");
    eventLaunch.setValueTimestamp(ParserSparkHelper.getLaunchTime(input));
    taskInstance.getTaskInstanceEventsSet().add(eventLaunch);
    eventLaunch.setTaskInstance(taskInstance);
    orm.save(eventLaunch);

    if (firstEntry == null) {
      firstEntry = eventLaunch.getValueTimestamp();
    }

    TaskInstanceEvents eventFinished = new TaskInstanceEvents();
    eventFinished.setEventName("Finished");
    eventFinished.setValueTimestamp(ParserSparkHelper.getFinishTime(input));
    taskInstance.getTaskInstanceEventsSet().add(eventFinished);
    eventFinished.setTaskInstance(taskInstance);
    orm.save(eventFinished);

    if (lastEntry == null) {
      lastEntry = eventFinished.getValueTimestamp();
    } else if (lastEntry.getTime() < eventFinished.getValueTimestamp()
		    .getTime()) {
      lastEntry = eventFinished.getValueTimestamp();
    }

    Set<TaskInstanceEvents> otherEvents = eventConverter.getTaskInstanceEventsByLine(
		    input);
    for (TaskInstanceEvents event : otherEvents) {
      event.setTaskInstance(taskInstance);
      taskInstance.getTaskInstanceEventsSet().add(event);
      orm.save(event);
    }

    return taskInstance;
  }

  /**
   * returns the task of the given line. If the task is not created yet, it will create it.
   *
   * @param input - a String line
   * @return Task of given line
   */
  private Task getTaskByLine(String input) {
    String taskType = ParserSparkHelper.getTaskType(input);
    Task task = experimentRun.taskByTaskType(taskType);
    if (task == null) {
      task = new Task();
      task.setExperimentRun(experimentRun);
      task.setTaskType(taskType);
      experimentRun.getTaskSet().add(task);
      orm.save(task);
    }
    return task;
  }

  private void setSubmitTime(String line) {
    JSONObject jsonObject = new JSONObject(line);
    experimentRun.setSubmitTime(new Date(jsonObject.getLong("Timestamp")));
  }
}
