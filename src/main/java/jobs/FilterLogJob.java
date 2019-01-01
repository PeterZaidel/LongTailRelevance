package jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import writables.QRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class FilterLogJob extends Configured implements Tool {

    private static boolean FILTER_QUERY = true;
    private static boolean FILTER_URl = false;

    private static final String TAG_DELIMETER = "<TAG>";
    private static final String CLICK_TAG = "<CLICK>";
    private static final String SHOWN_TAG = "<SHOWN>";
    private static final String QURL_PAIR_DELIMETER = "<QPAIR>";
    private static String DELIMETER = "\t";
    private static String FIELD_DELIMETER = "#";
    private static String LIST_DELIMETER = ",";

    private static String queries_filename = "";//"/home/peter/Yandex.Disk/MyStudyDocs/Technosphere/3sem/InfoSearch2/Data/hw5/data/queries.tsv";
    private static String url_data_filename = "";//"/home/peter/Yandex.Disk/MyStudyDocs/Technosphere/3sem/InfoSearch2/Data/hw5/data/url.data";

    private static final String QUERIES_FILENAME_TAG = "queries_filename";
    private static final String URLS_FILENAME_TAG = "urls_filename";

    public static class FilterJobMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text one = new Text("1");

        private HashMap<String, Integer> queries_map = new HashMap<>();
        private HashMap<String, Integer> urls_map = new HashMap<>();

        private void readToHashMap(BufferedReader br, HashMap<String, Integer> map) throws IOException {
            String line = br.readLine();
            while (line != null){
                String[] args = line.split("\t");

                map.put(args[1], Integer.parseInt(args[0]));

                line=br.readLine();
            }
        }

        public void setup(Mapper.Context context) throws IOException
        {
            FileSystem fs = FileSystem.get(new Configuration());

            queries_filename = context.getConfiguration().get(QUERIES_FILENAME_TAG);
            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);

            BufferedReader br_queries =new BufferedReader(new InputStreamReader(fs.open(new Path(queries_filename)),
                    StandardCharsets.UTF_8) );

            BufferedReader br_urls =new BufferedReader(new InputStreamReader(fs.open(new Path(url_data_filename)),
                    StandardCharsets.UTF_8) );

            queries_map = new HashMap<>();
            readToHashMap(br_queries, queries_map);

            urls_map = new HashMap<>();
            readToHashMap(br_urls, urls_map);


        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            QRecord record = new QRecord();
            record.parseString(value.toString());

            if(!queries_map.containsKey(record.query) && FILTER_QUERY)
            {
                return;
            }

            if(FILTER_URl)
            {
                boolean contain_url = false;
                for (String url : record.shownLinks)
                {
                    if(urls_map.containsKey(url))
                    {
                        contain_url = true;
                    }
                }

                for(String url: record.clickedLinks)
                {
                    if(urls_map.containsKey(url))
                    {
                        contain_url = true;
                    }
                }

                if(!contain_url)
                {
                    return;
                }
            }

            context.write(value, one);
        }
    }

    public static class FilterJobReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
        String input_file = args[0];
        String output_file = args[1];
        String queries_filename = args[2];
        String urls_filename = args[3];


        Configuration conf = getConf();

        conf.set(QUERIES_FILENAME_TAG, queries_filename);
        conf.set(URLS_FILENAME_TAG, urls_filename);

        Job job = GetJobConf(conf,input_file, output_file );
        return job.waitForCompletion(true) ? 0 : 1;
    }


    private static Job GetJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(FilterLogJob.class);
        job.setJobName(FilterLogJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));

        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(FilterJobMapper.class);
        job.setReducerClass(FilterJobReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(15);

        return job;
    }

    static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("JoinOnPairsJob Log Started!");

        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new FilterLogJob(), args);
        System.exit(exitCode);
    }
}