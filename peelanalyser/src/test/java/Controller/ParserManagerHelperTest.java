package Controller;

import org.easymock.EasyMock;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

public class ParserManagerHelperTest {

    @Test
    public void testExperimentName() throws Exception{
        JSONObject jsonObject = EasyMock.createMock(JSONObject.class);
        EasyMock.expect(jsonObject.getString("name")).andReturn("kmeans.D3K8.10m.dop64.men36G.run01");
        EasyMock.replay(jsonObject);

        String result = ParserManagerHelper.getExperimentName(jsonObject);
        assertEquals("kmeans.D3K8.10m.dop64.men36G", result);
    }

    @Test
    public void testIsJobmanager() throws Exception {
        String input = "flink-impro-jobmanager-cloud-7.log";
        boolean result = ParserManagerHelper.isJobmanager(input, "flink");
        assertEquals(true, result);

        input = "flink-impro-jobmanager-cloud-7.out";
        result = ParserManagerHelper.isJobmanager(input, "flink");
        assertEquals(false, result);

        input = "flink-impro-taskmanager-cloud-9.out";
        result = ParserManagerHelper.isJobmanager(input, "flink");
        assertEquals(false, result);

        input = "flink-impro-flink-client-cloud-7";
        result = ParserManagerHelper.isJobmanager(input, "flink");
        assertEquals(false, result);

        input = "EVENT_LOG_1";
        result = ParserManagerHelper.isJobmanager(input, "spark");
        assertEquals(true, result);
    }

    @Test
    public void testIsFailedExperimentRun() throws Exception{
        JSONObject jsonObject = EasyMock.createMock(JSONObject.class);
        EasyMock.expect(jsonObject.get("runExitCode")).andReturn(0);
        EasyMock.replay(jsonObject);

        boolean result = ParserManagerHelper.isFailedExperimentRun(jsonObject);
        assertFalse(result);
        EasyMock.verify();

        jsonObject = EasyMock.createMock(JSONObject.class);
        EasyMock.expect(jsonObject.get("runExitCode")).andReturn(1);
        EasyMock.replay(jsonObject);

        result = ParserManagerHelper.isFailedExperimentRun(jsonObject);
        assertTrue(result);
        EasyMock.verify(jsonObject);
    }

}