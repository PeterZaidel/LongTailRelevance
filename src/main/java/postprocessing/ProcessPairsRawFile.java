package postprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import utils.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ProcessPairsRawFile {
    private static final String DELIMETER = "\t";

    private static class QDMPair
    {
        public int query_id;
        public int url_id;
        public int mark = -1;

        public String query;
        public String url;

        public QDMPair()
        {}

        public QDMPair(int query_id, int url_id)
        {
            this.query_id = query_id;
            this.url_id = url_id;
        }

        public QDMPair(int query_id, int url_id, int mark)
        {
            this.query_id = query_id;
            this.url_id = url_id;
            this.mark = mark;
        }
    }

    private static List<QDMPair> readTrainFile(String filename) throws IOException {

        List<QDMPair> res = new ArrayList<>();

        FileSystem fs = FileSystem.get(new Configuration());

        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(filename)),
                StandardCharsets.UTF_8) );
        String line = br.readLine();
        while (line != null){
            try {

                String[] args = line.split("\t");
                int qid = Integer.parseInt(args[0]);
                int uid = Integer.parseInt(args[1]);
                int mark = Integer.parseInt(args[2]);

                QDMPair p = new QDMPair(qid, uid, mark);
                res.add(p);
            } catch (Exception ex) {}

            line=br.readLine();
        }

        return res;
    }

    private static List<QDMPair> readTestFile(String filename) throws IOException {

        List<QDMPair> res = new ArrayList<>();

        FileSystem fs = FileSystem.get(new Configuration());

        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(filename)),
                StandardCharsets.UTF_8) );
        String line = br.readLine();
        while (line != null){
            String[] args = line.split(",");
            try {

                int qid = Integer.parseInt(args[0]);
                int uid = Integer.parseInt(args[1]);

                QDMPair p = new QDMPair(qid, uid, -1);
                res.add(p);
            } catch (Exception ex) {}

            line=br.readLine();
        }

        return res;
    }


    public static void process(String[] args) throws IOException {
        String train_file = args[0];
        String test_file = args[1];

        String output_train_file = args[2];
        String output_test_file = args[3];

        String queries_filename = args[4];
        String urls_filename = args[5];

        HashMap<String, Integer> queries_map = new HashMap<>();
        HashMap<Integer, String> inv_queries_map = new HashMap<>();

        HashMap<String, Integer> urls_map = new HashMap<>();
        HashMap<Integer, String> inv_urls_map = new HashMap<>();

        Utils.readToHashMap(new Configuration(), queries_filename, queries_map, inv_queries_map, true);
        Utils.readToHashMap(new Configuration(), urls_filename, urls_map, inv_urls_map, false);

        List<QDMPair> trainPairs = readTrainFile(train_file);
        List<QDMPair> testPairs = readTestFile(test_file);


        for(QDMPair p : trainPairs)
        {
            p.query = inv_queries_map.get(p.query_id);
            p.url = inv_urls_map.get(p.url_id);
        }

        for(QDMPair p : testPairs)
        {
            p.query = inv_queries_map.get(p.query_id);
            p.url = inv_urls_map.get(p.url_id);
        }

        {
            PrintWriter writer = new PrintWriter(output_train_file, "UTF-8");
            for (QDMPair p : trainPairs) {
                String out = p.query + DELIMETER + p.url + DELIMETER + p.mark + '\n';
                writer.write(out);
            }
            writer.close();
        }
        {
            PrintWriter writer = new PrintWriter(output_test_file, "UTF-8");
            for (QDMPair p : testPairs) {
                String out = p.query + DELIMETER + p.url + DELIMETER + p.mark + '\n';
                writer.write(out);
            }
            writer.close();
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("ProcessPairsRawFile Started!");

        process(args);
    }
}
