package colaborative;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;

public class ProcessDistances {

    public static HashSet<String> readDistancesUrlsSet(Configuration conf, String filepath, double threshold) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        HashSet<String> res = new HashSet<>();

        for(FileStatus fileStatus: fileSystem.listStatus(new Path(filepath))) {

            Path unique_file = fileStatus.getPath();
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(fileSystem.open(unique_file), StandardCharsets.UTF_8));

            String line = br.readLine();
            while (line != null) {
                String[] args = line.split("\t");

                String knownQuery = args[0];

                res.add(knownQuery);

                for(int i = 1; i< args.length; i++)
                {
                    String q = args[i].split(",")[0];
                    double dist = Double.parseDouble(args[i].split(",")[1]);
                    if(dist > threshold) {
                        res.add(q);
                    }
                }

                line = br.readLine();
            }
        }
        return res;
    }



    public static void main(String[] args) throws IOException {
        String distances_file = args[0];
        String queries_filename = args[1];
        String output_file = args[2];

        HashSet<String> similarQueries = readDistancesUrlsSet(new Configuration(),
                distances_file, 0.1);

        HashMap<String, Integer> queries_map = new HashMap<>();
        Utils.readToHashMap(new Configuration(), queries_filename,
                queries_map,null, true);


        int newIdx = 0;
        for(Integer i : queries_map.values())
        {
            if(i > newIdx){
                newIdx = i;
            }
        }
        newIdx++;

        HashMap<String, Integer> newQueriesMap = new HashMap<>(queries_map);


        for(String q : similarQueries)
        {
            if(!queries_map.containsKey(q)){
                newQueriesMap.put(q, newIdx);
                newIdx += 1;
            }
        }

        PrintWriter writer = new PrintWriter(output_file, "UTF-8");
        for(String q: newQueriesMap.keySet())
        {
            writer.println(q + "\t" + Integer.toString(newQueriesMap.get(q)));
        }
        writer.close();

    }
}
