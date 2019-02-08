package colaborative;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.Utils;
import writables.QRecord;

import java.io.*;
import java.util.*;



public class CollectQuerySimFeaturesJob extends Configured implements Tool {

    private static final String DELIMETER = "\t";
    private static final String PAIRS_FILE_TAG = "<PFTAG>";
    private static final String INPUT_FILE_TAG = "<INPUT_TAG>";
    private static final String TAG_DELIMETER = "<DELIM_TAG>";
    private static final String SVM_DELIMETER = " ";


    private static String queries_filename = "";
    private static String url_data_filename = "";


    private static final String QUERIES_FILENAME_TAG = "queries_filename";
    private static final String URLS_FILENAME_TAG = "urls_filename";

    private static final String KNOWN_QUERY_GROUP = "KNOWNQ";
    private static final String ALL_QUERY_GROUP = "ALLQ";


    public static class CollectQuerySimFeaturesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text one = new Text("1");
        private HashMap<String, Integer> urls_map = new HashMap<>();


        public void setup(Mapper.Context context) throws IOException, IOException {

            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);
            urls_map = new HashMap<>();
            Utils.readToHashMap(context.getConfiguration(), url_data_filename, urls_map, null, false);
        }



        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                QRecord record = new QRecord();
                record.parseString(value.toString());

                QueryFeatures queryFeatures = new QueryFeatures(record);
                context.write(new Text(queryFeatures.getKey()),
                        new Text(queryFeatures.getValue()));
            }
            catch (Exception e)
            {
                System.out.println("sss");
            }

//            String resValue = "";
//            String resKey = record.query;
//
//            for (String url : record.shownLinks)
//            {
//                if(urls_map.containsKey(url)) {
//                    resValue += url + DELIMETER;
//                }
//            }
//            resValue = resValue.substring(0, resValue.length()-1);

//            if(!resValue.isEmpty()) {
//                context.write(new Text(resKey), new Text(resValue));
//            }
        }
    }

    public static class CollectQuerySimFeaturesCombiner extends Reducer<Text, Text, Text, Text>
    {
        Text emp  = new Text("");

        private HashMap<String, Integer> queries_map = new HashMap<>();
        private HashMap<String, Integer> urls_map = new HashMap<>();



        public void setup(Reducer.Context context) throws IOException, IOException {
            queries_filename = context.getConfiguration().get(QUERIES_FILENAME_TAG);
            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);

            queries_map = new HashMap<>();
            Utils.readToHashMap(context.getConfiguration(), queries_filename, queries_map, null, true);

            urls_map = new HashMap<>();
            Utils.readToHashMap(context.getConfiguration(), url_data_filename, urls_map, null, false);
        }



        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            QueryFeatures queryFeatures = new QueryFeatures();
            queryFeatures.parseKeyString(key.toString());

            for (Text v: values)
            {
                queryFeatures.parseValueString(v.toString());
            }

            if(queries_map.containsKey(queryFeatures.query))
            {
                context.write(new Text(queryFeatures.getKey()),
                        new Text(queryFeatures.getValue()));
            }

            boolean containsKnownUrl = false;
            for(String url : queryFeatures.shownLinks)
            {
                if(urls_map.containsKey(url))
                {
                    containsKnownUrl = true;
                    break;
                }
            }

            if(containsKnownUrl)
            {
                context.write(new Text(queryFeatures.getKey()),
                        new Text(queryFeatures.getValue()));
            }
        }
    }


    public static class CollectQuerySimFeaturesReducer extends Reducer<Text, Text, Text, Text>
    {
        Text emp  = new Text("");

        private HashMap<String, Integer> queries_map = new HashMap<>();
        private HashMap<String, Integer> urls_map = new HashMap<>();

        private MultipleOutputs<Text, Text> multipleOutputs;


        public void setup(Reducer.Context context) throws IOException, IOException {
            multipleOutputs = new MultipleOutputs(context);
            queries_filename = context.getConfiguration().get(QUERIES_FILENAME_TAG);
            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);

            queries_map = new HashMap<>();
            Utils.readToHashMap(context.getConfiguration(), queries_filename, queries_map, null, true);

            urls_map = new HashMap<>();
            Utils.readToHashMap(context.getConfiguration(), url_data_filename, urls_map, null, false);
        }



        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            QueryFeatures queryFeatures = new QueryFeatures();
            queryFeatures.parseKeyString(key.toString());

            for (Text v: values)
            {
                queryFeatures.parseValueString(v.toString());
            }

            if(queries_map.containsKey(queryFeatures.query))
            {
                multipleOutputs.write(KNOWN_QUERY_GROUP, new Text(queryFeatures.getKey()),
                        new Text(queryFeatures.getValue()),
                        KNOWN_QUERY_GROUP + "/part");

                multipleOutputs.write(ALL_QUERY_GROUP, new Text(queryFeatures.getKey()),
                        new Text(queryFeatures.getValue()),
                        ALL_QUERY_GROUP + "/part");
            }
            else {

                multipleOutputs.write(ALL_QUERY_GROUP, new Text(queryFeatures.getKey()),
                        new Text(queryFeatures.getValue()),
                        ALL_QUERY_GROUP + "/part");
            }


//            String query = key.toString();
//            HashSet<String> ShownUrls = new HashSet<>();
//            for(Text v : values)
//            {
//                for(String url : v.toString().split(DELIMETER))
//                {
//                    ShownUrls.add(url);
//                }
//            }
//
//            String strValue = "";
//            for (String url : ShownUrls)
//            {
//                strValue += url + DELIMETER;
//            }
//            strValue = strValue.substring(0, strValue.length()-1);
//
//            if(queries_map.containsKey(query))
//            {
//                multipleOutputs.write(KNOWN_QUERY_GROUP, new Text(query), new Text(strValue),
//                        KNOWN_QUERY_GROUP + "/part");
//            }
//
//            multipleOutputs.write(ALL_QUERY_GROUP, new Text(query), new Text(strValue),
//                    ALL_QUERY_GROUP + "/part");
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            multipleOutputs.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
        String input_file = args[0];

        String output_file = args[1];
        Utils.deleteDirectory(new File(output_file));

        String queries_filename = args[2];
        String urls_filename = args[3];

        Configuration conf = getConf();

        conf.set(QUERIES_FILENAME_TAG, queries_filename);
        conf.set(URLS_FILENAME_TAG, urls_filename);

        conf.set("mapreduce.map.output.compress", "true");
        conf.set("use mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");


        Job job = GetJobConf(conf,input_file,  output_file );
        return job.waitForCompletion(true) ? 0 : 1;
    }


    private static Job GetJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(CollectQuerySimFeaturesJob.class);
        job.setJobName(CollectQuerySimFeaturesJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);

        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        TextOutputFormat.setOutputPath(job, new Path(output));

        MultipleOutputs.addNamedOutput(job, KNOWN_QUERY_GROUP, TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, ALL_QUERY_GROUP, TextOutputFormat.class,
                Text.class, Text.class);

        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(CollectQuerySimFeaturesMapper.class);
//        job.setCombinerClass(CollectQuerySimFeaturesCombiner.class);
        job.setReducerClass(CollectQuerySimFeaturesReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(15);

        return job;
    }


    public static void main(String[] args) throws Exception {
        System.out.println("CollectQuerySimFeaturesJob Started!");

//        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new CollectQuerySimFeaturesJob(), args);
        System.exit(exitCode);
    }
}