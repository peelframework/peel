package eu.stratosphere.peel.analyser.parser;

import eu.stratosphere.peel.analyser.exception.PeelAnalyserException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Fabsi on 25.10.2014.
 */
class ParserFlinkHelper {

    private static final Pattern patternSubtaskNumber = Pattern.compile("[0-9]+(?=/[0-9]+\\))");
    private static final Pattern patternTimestamp = Pattern.compile("([0-2][0-9]):([0-5][0-9]):([0-5][0-9])(,)([0-9]{3})");
    private static final Pattern patternStatusChange = Pattern.compile("[A-Z]+( to )[A-Z]+|Starting|(?<=switched)( to [A-Z]+)");
    private static final Pattern patternIsJob = Pattern.compile("(Creating initial)|(Job input)|(Scheduling job)|(Requesting [0-9]+ slot)|(Releasing instance)|(Status of job)|(receives remote file input)|(Received job)|(Creating new execution graph)|(Job[A-z0-9 ()-]+switched)|(Sync)");
    private static final Pattern patternRequestingSlots = Pattern.compile("(Requesting [0-9]+ slots for)");
    private static final Pattern patternReleasingInstance = Pattern.compile("(Releasing instance)");
    private static final Pattern patternJobStatusChange = Pattern.compile("(Status of job)[a-z, A-Z 0-9,()]+(changed to)");
    private static final Pattern patternReceiveFileInput = Pattern.compile("(receives input split)");
    private static final Pattern patternSubmitJob = Pattern.compile("(Creating initial execution)|(Creating new execution)");
    private static final Pattern patternDeployedJob = Pattern.compile("(Scheduling job)");
    private static final Pattern patternFinishedJob = Pattern.compile("(changed to FINISHED)|(switched to FINISHED)");

    public static Date getTimeStamp(String input) throws PeelAnalyserException {
        String timestamp;
        Date timestampDate;
        Matcher matcherTimestamp = patternTimestamp.matcher(input);
        if (matcherTimestamp.find()){
            timestamp = matcherTimestamp.group();
            DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss,SSS");
            try {
                timestampDate = dateFormat.parse(timestamp);
            } catch (ParseException e) {
                throw new PeelAnalyserException("ParserFlink handleTaskInstanceInput - error during parsing of timestamp");
            }
        } else {
            throw new PeelAnalyserException("ParserFlink handleTaskInstanceInput - could not find timestamp");
        }
        return timestampDate;
    }

    public static int getSubTaskNumber(String input) throws PeelAnalyserException {
        String subTaskNumber;
        Matcher matcherSubtaskNumber = patternSubtaskNumber.matcher(input);
        if(matcherSubtaskNumber.find()) {
            subTaskNumber = matcherSubtaskNumber.group();
        } else {
            throw new PeelAnalyserException("ParserFlink handleTaskInstanceInput - could not find a subtaskID");
        }
        return Integer.parseInt(subTaskNumber);
    }

    public static String getStatusChange(String input) throws PeelAnalyserException {
        String statusChange = "";
        Matcher matcherStatusChange = patternStatusChange.matcher(input);
        if(matcherStatusChange.find()){
            statusChange = matcherStatusChange.group();
        } else if(patternReceiveFileInput.matcher(input).find()) {
            //TODO: implement receive File Split
        } else {
            throw new PeelAnalyserException("ParserFlink handleTaskInstanceInput - could not find a statusChange");
        }
        return statusChange;
    }

    public static boolean isRequestSlotsEntry(String input) {
        Matcher matcherRequestSlotsEntry = patternRequestingSlots.matcher(input);
        return matcherRequestSlotsEntry.find();
    }

    public static boolean isReleasingInstance(String input) {
        Matcher matcherReleasingInstance = patternReleasingInstance.matcher(input);
        return matcherReleasingInstance.find();
    }

    public static boolean isSubmitJob(String input) {
        Matcher matcherSubmitJob = patternSubmitJob.matcher(input);
        return matcherSubmitJob.find();
    }

    public static boolean isDeploydJob(String input){
        Matcher matcherSchedulingJob = patternDeployedJob.matcher(input);
        return matcherSchedulingJob.find();
    }

    public static boolean isFinishedJob(String input) {
        Matcher matcherFinishedJob = patternFinishedJob.matcher(input);
        return matcherFinishedJob.find();
    }

    /**
     * this method looks if the logfile statement specifies a job or not.
     * @param input a logfile line
     * @return true if the logfile statement is a job and false if it's not
     */
    public static boolean isJob(String input){
        Matcher matcherIsJob = patternIsJob.matcher(input);
        return matcherIsJob.find();
    }
}
