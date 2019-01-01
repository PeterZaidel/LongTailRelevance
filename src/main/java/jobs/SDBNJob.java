package jobs;

import utils.Utils;
import writables.DistributedIndex;
import writables.DistributedParameter;
import writables.QRecord;
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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class SDBNJob extends Configured implements Tool {


    public static class SDBNParameters
    {
        public static final String GROUP_NAME = "SDBN";
        public static final String A_N = "A_N";
        public static final String A_D = "A_D";
        public static final String S_N = "S_N";
        public static final String S_D = "S_D";

        public static double ALPHA_A = 0.1;
        public static double ALPHA_S = 0.1;
        public static double BETA_A = 0.1;
        public static double BETA_S = 0.1;
    }


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

    public static class SDBNMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static LongWritable one = new LongWritable(1);

        private HashMap<String, Integer> queries_map = new HashMap<>();
        private HashMap<String, Integer> urls_map = new HashMap<>();
//
//        private void readToHashMap(BufferedReader br, HashMap<String, Integer> map) throws IOException {
//            String line = br.readLine();
//            while (line != null){
//                String[] args = line.split("\t");
//
//                map.put(args[1], Integer.parseInt(args[0]));
//
//                line=br.readLine();
//            }
//        }

        public void setup(Mapper.Context context) throws IOException
        {
//            FileSystem fs = FileSystem.get(new Configuration());

            queries_filename = context.getConfiguration().get(QUERIES_FILENAME_TAG);
            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);

//            BufferedReader br_queries =new BufferedReader(new InputStreamReader(fs.open(new Path(queries_filename)),
//                    StandardCharsets.UTF_8) );
//
//            BufferedReader br_urls =new BufferedReader(new InputStreamReader(fs.open(new Path(url_data_filename)),
//                    StandardCharsets.UTF_8) );

            queries_map = new HashMap<>();
            //readToHashMap(br_queries, queries_map);
            Utils.readToHashMap(context.getConfiguration(), queries_filename, queries_map, null, true);

            urls_map = new HashMap<>();
            //readToHashMap(br_urls, urls_map);
            Utils.readToHashMap(context.getConfiguration(), url_data_filename, urls_map, null, true);


        }

        private void emit_sdbn(QRecord record, Context context) throws IOException, InterruptedException {

            String satisfaction_url = record.clickedLinks.get(record.clickedLinks.size()-1);

            Set<String> clicked_set = new HashSet<>();
            clicked_set.addAll(record.clickedLinks);

            String last_clicked_url = record.clickedLinks.get(0);
            for(String url : record.shownLinks)
            {
                if(clicked_set.contains(url))
                {
                    last_clicked_url = url;
                }
            }

            //S_N parameter update
            {
                DistributedParameter param = new DistributedParameter();
                param.paramName = SDBNParameters.S_N;
                param.paramGroup = SDBNParameters.GROUP_NAME;

                DistributedIndex.QueryUrlIndex qu_index = new DistributedIndex.QueryUrlIndex(record.query,
                        satisfaction_url);
                param.paramIndex = qu_index.toString();
                param.paramValue = Integer.toString(1);

                context.write(param.extractKey(), param.extractValue());
            }

            for(String url : record.shownLinks)
            {
                DistributedParameter param = new DistributedParameter();
                param.paramName = SDBNParameters.A_D;
                param.paramGroup = SDBNParameters.GROUP_NAME;

                DistributedIndex.QueryUrlIndex qu_index = new DistributedIndex.QueryUrlIndex(record.query, url);
                param.paramIndex = qu_index.toString();
                param.paramValue = Integer.toString(1);

                context.write(param.extractKey(), param.extractValue());


                if(url.equals(last_clicked_url))
                {
                    break;
                }
            }

            for(String url : record.clickedLinks)
            {
                DistributedIndex.QueryUrlIndex qu_index = new DistributedIndex.QueryUrlIndex(record.query, url);

                // A_N param update
                {
                    DistributedParameter param_an = new DistributedParameter();
                    param_an.paramName = SDBNParameters.A_N;
                    param_an.paramGroup = SDBNParameters.GROUP_NAME;
                    param_an.paramIndex = qu_index.toString();

                    param_an.paramValue = Integer.toString(1);
                    context.write(param_an.extractKey(), param_an.extractValue());
                }

                // S_D param update
                {
                    DistributedParameter param_sd = new DistributedParameter();
                    param_sd.paramName = SDBNParameters.S_D;
                    param_sd.paramGroup = SDBNParameters.GROUP_NAME;
                    param_sd.paramIndex = qu_index.toString();

                    param_sd.paramValue = Integer.toString(1);
                    context.write(param_sd.extractKey(), param_sd.extractValue());
                }

            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            QRecord record = new QRecord();
            record.parseString(value.toString());


            if(!queries_map.containsKey(record.query) && FILTER_QUERY)
            {
                return;
            }

            emit_sdbn(record, context);
        }
    }

    public static class SDBNReducer extends Reducer<Text, Text, Text, Text>
    {

        private void reduce_sdbn(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            DistributedParameter param = new DistributedParameter();
            param.parseKey(key.toString());

            DistributedIndex.QueryUrlIndex index = new DistributedIndex.QueryUrlIndex();
            index.parseString(param.paramIndex);

            double A_N = 0;
            double A_D = 0;
            double S_N = 0;
            double S_D = 0;

            for (Text v : values)
            {
                param.parseValue(v.toString());

                double val = Double.parseDouble(param.paramValue);
                switch (param.paramName)
                {
                    case SDBNParameters.A_D:
                        A_D += val;
                        break;
                    case SDBNParameters.A_N:
                        A_N += val;
                        break;

                    case SDBNParameters.S_N:
                        S_N += val;
                        break;
                    case SDBNParameters.S_D:
                        S_D += val;
                        break;

                }
            }

            double A_U = (A_N + SDBNParameters.ALPHA_A)/(A_D + SDBNParameters.ALPHA_A + SDBNParameters.BETA_A);
            double S_U = (S_N + SDBNParameters.ALPHA_S)/(S_D + SDBNParameters.ALPHA_S + SDBNParameters.BETA_S);
            double R_U = A_U * S_U;

            String value = "";
            value += index.url + DELIMETER + Double.toString(A_U)
                    + DELIMETER + Double.toString(S_U)
                    + DELIMETER + Double.toString(R_U);

            context.write(new Text(index.query), new Text(value));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            reduce_sdbn(key, values, context);
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
        job.setJarByClass(SDBNJob.class);
        job.setJobName(SDBNJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));

        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(SDBNMapper.class);
        job.setReducerClass(SDBNReducer.class);


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
        System.out.println("SDBN Started!");

        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new SDBNJob(), args);
        System.exit(exitCode);
    }
}