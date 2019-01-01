package features;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import utils.Utils;
import writables.ReduceResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MapGrouper {

    private static final String DELIMETER = "<MG>";

    private HashMap<String, List<String>> groups = new HashMap<>();

    public void parseString(String key, String value)
    {
        groups = new HashMap<>();
        List<String> valuesList = new ArrayList<>();

        String[] args = value.split(DELIMETER);
        valuesList.addAll(Arrays.asList(args));
        groups.put(key, valuesList);
    }


    public List<String> get(String key)
    {
        return groups.get(key);
    }



    public void write(String key, String value){
        if(!groups.containsKey(key))
        {
            groups.put(key, new ArrayList<>());
        }
        groups.get(key).add(value);
    }


    public void flush(Mapper.Context context) throws IOException, InterruptedException {
        for(String key : groups.keySet())
        {
            String value = Utils.listToString(groups.get(key), DELIMETER);
            context.write(new Text(key), new Text(value));
        }
    }


}
