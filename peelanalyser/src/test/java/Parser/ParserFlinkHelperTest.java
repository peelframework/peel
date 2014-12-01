package Parser;

import org.junit.Test;

import static org.junit.Assert.*;

public class ParserFlinkHelperTest {

    @Test
    public void testIsJob() throws Exception {
        String input = "15:32:25,579 INFO  org.apache.flink.runtime.jobmanager.JobManager                - Creating initial execution graph from job graph WordCount Example";
        boolean result = ParserFlinkHelper.isJob(input);
        assertTrue(result);

        input = "15:32:25,596 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Job input vertex CHAIN DataSource (TextInputFormat (hdfs://localhost:9000/tmp/input/hamlet.txt) - UTF-8) -> FlatMap (org.apache.flink.example.java.wordcount.WordCount$Tokenizer) -> Combine(SUM(1)) generated 4 input splits";
        result = ParserFlinkHelper.isJob(input);
        assertTrue(result);

        input = "15:32:25,650 INFO  org.apache.flink.runtime.jobmanager.JobManager                - Scheduling job WordCount Example";
        result = ParserFlinkHelper.isJob(input);
        assertTrue(result);

        input = "15:32:25,652 INFO  org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler  - Requesting 4 slots for job 303a5e9e4a389c0044a227d32eec8c00";
        result = ParserFlinkHelper.isJob(input);
        assertTrue(result);

        input = "15:32:27,774 INFO  org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler  - Releasing instance localhost (ipcPort=60181, dataPort=60516)";
        result = ParserFlinkHelper.isJob(input);
        assertTrue(result);

        input = "15:32:27,819 INFO  org.apache.flink.runtime.jobmanager.JobManager                - Status of job WordCount Example(303a5e9e4a389c0044a227d32eec8c00) changed to FINISHED";
        result = ParserFlinkHelper.isJob(input);
        assertTrue(result);

        input = "15:32:26,824 INFO  org.apache.flink.runtime.execution.ExecutionStateTransition   - JM: ExecutionState set from STARTING to RUNNING for task DataSink(CsvOutputFormat (path: hdfs://localhost:9000/tmp/output/wc, delimiter:  )) (3/4)";
        result = ParserFlinkHelper.isJob(input);
        assertFalse(result);

        input = "15:32:26,803 INFO  org.apache.flink.runtime.execution.ExecutionStateTransition   - JM: ExecutionState set from READY to STARTING for task DataSink(CsvOutputFormat (path: hdfs://localhost:9000/tmp/output/wc, delimiter:  )) (2/4)";
        result = ParserFlinkHelper.isJob(input);
        assertFalse(result);

        input = "15:32:25,814 INFO  org.apache.flink.runtime.jobmanager.JobManager                - Starting task Reduce (SUM(1)) (3/4) on localhost (ipcPort=60181, dataPort=60516)";
        result = ParserFlinkHelper.isJob(input);
        assertFalse(result);

        input = "15:32:25,716 INFO  org.apache.flink.runtime.jobmanager.splitassigner.InputSplitManager  - CHAIN DataSource (TextInputFormat (hdfs://localhost:9000/tmp/input/hamlet.txt) - UTF-8) -> FlatMap (org.apache.flink.example.java.wordcount.WordCount$Tokenizer) -> Combine(SUM(1)) (3/4) receives input split 2";
        result = ParserFlinkHelper.isJob(input);
        assertFalse(result);

        input = "15:32:25,694 INFO  org.apache.flink.runtime.jobmanager.splitassigner.file.FileInputSplitList  - localhost (ipcPort=60181, dataPort=60516) receives remote file input split (distance 2147483647)";
        result = ParserFlinkHelper.isJob(input);
        assertTrue(result);

        input = "10:35:54,406 INFO  org.apache.flink.runtime.jobmanager.JobManager                - Received job dcdb933a5b36e7ece7a6d85f81c4e718 (KMeans Multi-Dimension)";
        result = ParserFlinkHelper.isJob(input);
        assertTrue(result);

        input = "10:35:54,408 INFO  org.apache.flink.runtime.jobmanager.JobManager                - Creating new execution graph for job dcdb933a5b36e7ece7a6d85f81c4e718 (KMeans Multi-Dimension)";
        result = ParserFlinkHelper.isJob(input);
        assertTrue(result);

        input = "10:35:54,862 INFO  org.apache.flink.runtime.jobmanager.JobManager                - Job dcdb933a5b36e7ece7a6d85f81c4e718 (KMeans Multi-Dimension) switched to RUNNING.";
        result = ParserFlinkHelper.isJob(input);
        assertTrue(result);
    }

    @Test
    public void testSubtaskNumber() throws Exception{
        String input = "10:37:18,983 INFO  org.apache.flink.runtime.jobmanager.EventCollector            - 11/06/2014 10:37:18:\tCHAIN DataSource (TextInputFormat (hdfs://cloud-7:45010/tmp/input/kmeans.D3K8.10m/points) - UTF-8) -> Map (com.github.projectflink.testPlan.KMeansArbitraryDimension$ConvertToPoint) (12/64) switched to DEPLOYING";
        int subtaskNumber = 12;
        int result = ParserFlinkHelper.getSubTaskNumber(input);
        assertEquals(subtaskNumber, result);
    }
}