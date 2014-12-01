package Parser;

import org.json.JSONObject;

import java.util.Date;

/**
 * Created by Fabian on 02.11.2014.
 */
public class ParserSparkHelper {

    public static int getTaskID(String input){
        JSONObject jsonObject = new JSONObject(input);
        return jsonObject.getJSONObject("Task Info").getInt("Task ID");
    }

    public static Date getLaunchTime(String input){
        JSONObject jsonObject = new JSONObject(input);
        return new Date(jsonObject.getJSONObject("Task Info").getLong("Launch Time"));
    }

    public static Date getFinishTime(String input){
        JSONObject jsonObject = new JSONObject(input);
        return new Date(jsonObject.getJSONObject("Task Info").getLong("Finish Time"));
    }

    public static String getEvent(String input){
        JSONObject jsonObject = new JSONObject(input);
        return jsonObject.getString("Event");
    }

    public static String getTaskType(String input){
        JSONObject jsonObject = new JSONObject(input);
        return jsonObject.getString("Task Type");
    }
}
