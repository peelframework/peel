package Parser;

import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

public class ParserSparkHelperTest {

    @Test
    public void testGetTaskID() throws Exception {
        String input = "{\"Event\":\"SparkListenerTaskStart\",\"Stage ID\":0,\"Stage Attempt ID\":0,\"Task Info\":{\"Task ID\":0,\"Index\":2,\"Attempt\":0,\"Launch Time\":1414094709760,\"Executor ID\":\"1\",\"Host\":\"wally107.cit.tu-berlin.de\",\"Locality\":\"NODE_LOCAL\",\"Speculative\":false,\"Getting Result Time\":0,\"Finish Time\":0,\"Failed\":false,\"Accumulables\":[]}}";
        int result = ParserSparkHelper.getTaskID(input);
        assertEquals(0, result);
    }

    @Test
    public void testGetTaskID2() throws Exception {
        String input = "{\"Event\":\"SparkListenerTaskEnd\",\"Stage ID\":13,\"Stage Attempt ID\":0,\"Task Type\":\"ShuffleMapTask\",\"Task End Reason\":{\"Reason\":\"Success\"},\"Task Info\":{\"Task ID\":2885,\"Index\":3,\"Attempt\":0,\"Launch Time\":1414094815615,\"Executor ID\":\"5\",\"Host\":\"wally102.cit.tu-berlin.de\",\"Locality\":\"PROCESS_LOCAL\",\"Speculative\":false,\"Getting Result Time\":0,\"Finish Time\":1414094817066,\"Failed\":false,\"Accumulables\":[]},\"Task Metrics\":{\"Host Name\":\"wally102.cit.tu-berlin.de\",\"Executor Deserialize Time\":12,\"Executor Run Time\":1426,\"Result Size\":2194,\"JVM GC Time\":60,\"Result Serialization Time\":0,\"Memory Bytes Spilled\":0,\"Disk Bytes Spilled\":0,\"Shuffle Write Metrics\":{\"Shuffle Bytes Written\":1120,\"Shuffle Write Time\":52525},\"Input Metrics\":{\"Data Read Method\":\"Memory\",\"Bytes Read\":57050184}}}";
        int result = ParserSparkHelper.getTaskID(input);
        assertEquals(2885, result);
    }

    @Test
    public void testGetLaunchTime() throws Exception {
        String input = "{\"Event\":\"SparkListenerTaskStart\",\"Stage ID\":0,\"Stage Attempt ID\":0,\"Task Info\":{\"Task ID\":0,\"Index\":2,\"Attempt\":0,\"Launch Time\":1414094709760,\"Executor ID\":\"1\",\"Host\":\"wally107.cit.tu-berlin.de\",\"Locality\":\"NODE_LOCAL\",\"Speculative\":false,\"Getting Result Time\":0,\"Finish Time\":0,\"Failed\":false,\"Accumulables\":[]}}";
        Date result = ParserSparkHelper.getLaunchTime(input);
        assertEquals(result, new Date(1414094709760L));
    }

    @Test
    public void testGetFinishTime() throws Exception {
        String input = "{\"Event\":\"SparkListenerTaskEnd\",\"Stage ID\":6,\"Stage Attempt ID\":0,\"Task Type\":\"ResultTask\",\"Task End Reason\":{\"Reason\":\"Success\"},\"Task Info\":{\"Task ID\":1720,\"Index\":40,\"Attempt\":0,\"Launch Time\":1414094807053,\"Executor ID\":\"2\",\"Host\":\"wally110.cit.tu-berlin.de\",\"Locality\":\"PROCESS_LOCAL\",\"Speculative\":false,\"Getting Result Time\":0,\"Finish Time\":1414094807133,\"Failed\":false,\"Accumulables\":[]},\"Task Metrics\":{\"Host Name\":\"wally110.cit.tu-berlin.de\",\"Executor Deserialize Time\":11,\"Executor Run Time\":47,\"Result Size\":838,\"JVM GC Time\":0,\"Result Serialization Time\":0,\"Memory Bytes Spilled\":0,\"Disk Bytes Spilled\":0,\"Shuffle Read Metrics\":{\"Shuffle Finish Time\":-1,\"Remote Blocks Fetched\":0,\"Local Blocks Fetched\":0,\"Fetch Wait Time\":0,\"Remote Bytes Read\":0}}}";
        Date result = ParserSparkHelper.getFinishTime(input);
        assertEquals(result, new Date(1414094807133L));
    }

    @Test
    public void testGetFinishTime2() throws Exception {
        String input = "{\"Event\":\"SparkListenerTaskEnd\",\"Stage ID\":13,\"Stage Attempt ID\":0,\"Task Type\":\"ShuffleMapTask\",\"Task End Reason\":{\"Reason\":\"Success\"},\"Task Info\":{\"Task ID\":2885,\"Index\":3,\"Attempt\":0,\"Launch Time\":1414094815615,\"Executor ID\":\"5\",\"Host\":\"wally102.cit.tu-berlin.de\",\"Locality\":\"PROCESS_LOCAL\",\"Speculative\":false,\"Getting Result Time\":0,\"Finish Time\":1414094817066,\"Failed\":false,\"Accumulables\":[]},\"Task Metrics\":{\"Host Name\":\"wally102.cit.tu-berlin.de\",\"Executor Deserialize Time\":12,\"Executor Run Time\":1426,\"Result Size\":2194,\"JVM GC Time\":60,\"Result Serialization Time\":0,\"Memory Bytes Spilled\":0,\"Disk Bytes Spilled\":0,\"Shuffle Write Metrics\":{\"Shuffle Bytes Written\":1120,\"Shuffle Write Time\":52525},\"Input Metrics\":{\"Data Read Method\":\"Memory\",\"Bytes Read\":57050184}}}";
        Date result = ParserSparkHelper.getFinishTime(input);
        assertEquals(new Date(1414094817066L), result);
    }


    @Test
    public void testGetEvent() throws Exception{
        String input = "{\"Event\":\"SparkListenerTaskEnd\",\"Stage ID\":6,\"Stage Attempt ID\":0,\"Task Type\":\"ResultTask\",\"Task End Reason\":{\"Reason\":\"Success\"},\"Task Info\":{\"Task ID\":1720,\"Index\":40,\"Attempt\":0,\"Launch Time\":1414094807053,\"Executor ID\":\"2\",\"Host\":\"wally110.cit.tu-berlin.de\",\"Locality\":\"PROCESS_LOCAL\",\"Speculative\":false,\"Getting Result Time\":0,\"Finish Time\":1414094807133,\"Failed\":false,\"Accumulables\":[]},\"Task Metrics\":{\"Host Name\":\"wally110.cit.tu-berlin.de\",\"Executor Deserialize Time\":11,\"Executor Run Time\":47,\"Result Size\":838,\"JVM GC Time\":0,\"Result Serialization Time\":0,\"Memory Bytes Spilled\":0,\"Disk Bytes Spilled\":0,\"Shuffle Read Metrics\":{\"Shuffle Finish Time\":-1,\"Remote Blocks Fetched\":0,\"Local Blocks Fetched\":0,\"Fetch Wait Time\":0,\"Remote Bytes Read\":0}}}";
        String result = ParserSparkHelper.getEvent(input);
        assertEquals("SparkListenerTaskEnd", result);
    }

    @Test
    public void testGetTaskType() throws Exception{
        String input = "{\"Event\":\"SparkListenerTaskEnd\",\"Stage ID\":15,\"Stage Attempt ID\":0,\"Task Type\":\"ResultTask\",\"Task End Reason\":{\"Reason\":\"Success\"},\"Task Info\":{\"Task ID\":3676,\"Index\":76,\"Attempt\":0,\"Launch Time\":1414094830841,\"Executor ID\":\"4\",\"Host\":\"wally109.cit.tu-berlin.de\",\"Locality\":\"PROCESS_LOCAL\",\"Speculative\":false,\"Getting Result Time\":0,\"Finish Time\":1414094831251,\"Failed\":false,\"Accumulables\":[]},\"Task Metrics\":{\"Host Name\":\"wally109.cit.tu-berlin.de\",\"Executor Deserialize Time\":15,\"Executor Run Time\":389,\"Result Size\":601,\"JVM GC Time\":0,\"Result Serialization Time\":0,\"Memory Bytes Spilled\":0,\"Disk Bytes Spilled\":0}}";
        String result = ParserSparkHelper.getTaskType(input);
        assertEquals("ResultTask", result);
    }

    @Test
    public void testGetTaskType2() throws Exception{
        String input = "{\"Event\":\"SparkListenerTaskEnd\",\"Stage ID\":13,\"Stage Attempt ID\":0,\"Task Type\":\"ShuffleMapTask\",\"Task End Reason\":{\"Reason\":\"Success\"},\"Task Info\":{\"Task ID\":2885,\"Index\":3,\"Attempt\":0,\"Launch Time\":1414094815615,\"Executor ID\":\"5\",\"Host\":\"wally102.cit.tu-berlin.de\",\"Locality\":\"PROCESS_LOCAL\",\"Speculative\":false,\"Getting Result Time\":0,\"Finish Time\":1414094817066,\"Failed\":false,\"Accumulables\":[]},\"Task Metrics\":{\"Host Name\":\"wally102.cit.tu-berlin.de\",\"Executor Deserialize Time\":12,\"Executor Run Time\":1426,\"Result Size\":2194,\"JVM GC Time\":60,\"Result Serialization Time\":0,\"Memory Bytes Spilled\":0,\"Disk Bytes Spilled\":0,\"Shuffle Write Metrics\":{\"Shuffle Bytes Written\":1120,\"Shuffle Write Time\":52525},\"Input Metrics\":{\"Data Read Method\":\"Memory\",\"Bytes Read\":57050184}}}";
        String result = ParserSparkHelper.getTaskType(input);
        assertEquals("ShuffleMapTask", result);
    }
}