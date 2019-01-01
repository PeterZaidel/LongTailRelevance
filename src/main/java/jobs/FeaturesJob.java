package jobs;

import features.*;
import utils.Utils;
import writables.QRecord;
import writables.ReduceResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.*;


public class FeaturesJob extends Configured implements Tool {

    public static final String QD_GROUP_NAME = "QD";
    public static final String DOC_GROUP_NAME = "URL";
    public static final String QUERY_GROUP_NAME = "QUERY";

    public static final String DELIMETER = "\t";

    private static String queries_filename = "";
    private static String url_data_filename = "";

    private static final String QUERIES_FILENAME_TAG = "queries_filename";
    private static final String URLS_FILENAME_TAG = "urls_filename";

    private static final boolean FILTER_URL =  true;
    private static final  boolean FILTER_QUERY = true;
    private static final boolean FILTER_ZERO_VALS = true;




    private static List<FeatureExtractor> genFeatureExtractors()
    {
        return Arrays.asList(new CTRFeatureExtractor(),
                new SDBNFeatureExtractor(),
                new QueryFeatureExtractor(),
                new UrlFeatureExtractor());
    }

    public static class FeaturesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static LongWritable one = new LongWritable(1);


        private HashMap<String, Integer> queries_map = new HashMap<>();
        private HashMap<String, Integer> urls_map = new HashMap<>();

//        private void readToHashMap(BufferedReader br, HashMap<String, Integer> map, boolean isQuery) throws IOException {
//            String line = br.readLine();
//            while (line != null){
//                String[] args = line.split("\t");
//
//                String val = args[1];
//                if(isQuery)
//                {
//                    val = Utils.prepareQuery(val);
//                }
//                else
//                {
//                    val = Utils.prepareUrl(val);
//                }
//
//                map.put(val, Integer.parseInt(args[0]));
//
//                line=br.readLine();
//            }
//        }

        public void setup(Mapper.Context context) throws IOException
        {
            context.getConfiguration();
            //FileSystem fs = FileSystem.get(new Configuration());

            queries_filename = context.getConfiguration().get(QUERIES_FILENAME_TAG);
            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);

//            BufferedReader br_queries =new BufferedReader(new InputStreamReader(fs.open(new Path(queries_filename)),
//                    StandardCharsets.UTF_8) );
//
//            BufferedReader br_urls =new BufferedReader(new InputStreamReader(fs.open(new Path(url_data_filename)),
//                    StandardCharsets.UTF_8) );

            queries_map = new HashMap<>();
            Utils.readToHashMap(context.getConfiguration(), queries_filename, queries_map,null, true);

            urls_map = new HashMap<>();
            Utils.readToHashMap(context.getConfiguration(), url_data_filename, urls_map,null, false);
        }

        private boolean filterRecord(QRecord record)
        {
            boolean containsUrls = false;
            boolean containsQuery = false;

            for(String url : record.shownLinks)
            {
                url = Utils.prepareUrl(url);
                if(urls_map.containsKey(url))
                {
                    containsUrls = true;
                }
            }
            //с помощью каких явлений которые вы наблюдаете в жизни можно доказать


      //      record.query = Utils.prepareQuery(record.query);
  //          containsQuery = queries_map.containsKey(record.query);

//            if(!containsQuery && FILTER_QUERY)
//            {
////                if(record.query.equals("с помощью каких явлений которые вы наблюдаете в жизни можно доказать"))
////                {
////                    System.out.println("FILTER_ERROR: " + record.query + " | " + Boolean.toString(queries_map.containsKey(record.query)));
////                }
//
//                return false;
//            }

            if(!containsUrls && FILTER_URL)
            {
                return false;
            }

            return true;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<FeatureExtractor> featureExtractors = genFeatureExtractors();

             QRecord record = new QRecord();
//            if(FILTER_URL){
             record.parseString(value.toString(), urls_map);
//            }
//            else {
//                record.parseString(value.toString());
//            }

//            if(!filterRecord(record))
//            {
//                return;
//            }


            for(FeatureExtractor fe: featureExtractors)
            {
                fe.setQueriesMap(this.queries_map);
                fe.setUrlsMap(this.urls_map);
                fe.map(key, value, context);
            }
        }
    }

    public static class FeaturesCombiner  extends Reducer<Text, Text, Text, Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<FeatureExtractor> featureExtractors = genFeatureExtractors();
            for (Text v : values) {
                for (FeatureExtractor fe : featureExtractors) {
                    if (!fe.filter_reduce(key, v))
                        continue;

                    fe.combiner_update(key, v, context);
                }
            }

            for(FeatureExtractor fe : featureExtractors)
            {
                fe.combiner_final(key, context);
            }
        }
    }

    public static class FeaturesReducer extends Reducer<Text, Text, Text, Text>
    {

        private MultipleOutputs<Text, Text> multipleOutputs;


        public void setup(Reducer.Context context)
        {
            multipleOutputs = new MultipleOutputs(context);

        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                List<FeatureExtractor> featureExtractors = genFeatureExtractors();
                for (Text v : values) {
                    for (FeatureExtractor fe : featureExtractors) {
                        if (!fe.filter_reduce(key, v))
                            continue;

                        fe.reduce_update(key, v, context);
                    }
                }

                HashMap<String, List<ReduceResult>> reduceResultHashMap = new HashMap<>();
                for (FeatureExtractor fe : featureExtractors) {
                    ReduceResult rr = fe.reduce_final(key, context);
                    if(rr == null)
                    {
//                        System.out.println("SSS");
                        continue;
                    }

                    if (reduceResultHashMap.containsKey(rr.Group)) {
                        try {
                            reduceResultHashMap.get(rr.Group).add(rr);
                        }catch (Exception ex)
                        {
                            ex.printStackTrace();
                            System.out.println(ex);
                        }
                    } else {
                        List<ReduceResult> l = new ArrayList<>();
                        l.add(rr);

                        reduceResultHashMap.put(rr.Group, l);
                    }
                }

                for (String group : reduceResultHashMap.keySet())
                {
                    boolean isAllNotZero = false;
                    for (ReduceResult rr : reduceResultHashMap.get(group)) {
                         isAllNotZero = isAllNotZero || rr.isNotZero;
                    }
//
//                    if(isAllNotZero == false && FILTER_ZERO_VALS)
//                    {
//                        continue;
//                    }

                    StringBuilder out_names_sb = new StringBuilder();

                    StringBuilder out_sb = new StringBuilder();
                    String rrKey = "";

                    int valIdx = 0;
                    for (ReduceResult rr : reduceResultHashMap.get(group)) {
                        rrKey = rr.Key;

                        for(ReduceResult.IdxFeaturePair pair : rr.Value)
                        {

                            out_names_sb.append(Integer.toString(valIdx) + ":" + pair.name + " ");

                            if(pair.isNotNull) {
                                out_sb.append(Integer.toString(valIdx) + ":");
                                out_sb.append(pair.value);
                                out_sb.append(" ");
                            }
                            valIdx += 1;
                        }
                    }

                    String value = out_sb.toString();

                    if(value.length() == 0)
                    {
                        continue;
                    }

                    value = value.substring(0, value.length() - 1);

                    String filename = group + "/part";

                    try {

                        multipleOutputs.write(group, new Text(rrKey), new Text(value), filename);

                        multipleOutputs.write(group+"name", new Text(""),
                                new Text(out_names_sb.toString()), "FeatureNames/" + group + "/part");

                    }catch (Exception ex)
                    {
                        System.out.println("OUTPUT_WRITE_EX: " + ex);
                        ex.printStackTrace();
                    }
                }
            }catch (Exception ex)
            {
                System.out.println("REDUCE_EX: " + ex);
                ex.printStackTrace();
            }


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
        String queries_filename = args[2];
        String urls_filename = args[3];


        Configuration conf = getConf();

        conf.set(QUERIES_FILENAME_TAG, queries_filename);
        conf.set(URLS_FILENAME_TAG, urls_filename);

        conf.set("mapreduce.map.output.compress", "true");
        conf.set("use mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");

        //conf.set("mapred.output.compress", "true");
       // conf.set("mapred.output.compression.type", "org.apache.hadoop.io.compress.GzipCodec");

        Job job = GetJobConf(conf,input_file, output_file );
        return job.waitForCompletion(true) ? 0 : 1;
    }


    private static Job GetJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(FeaturesJob.class);
        job.setJobName(FeaturesJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);

        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        TextOutputFormat.setOutputPath(job, new Path(output));

        MultipleOutputs.addNamedOutput(job, QD_GROUP_NAME, TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QD_GROUP_NAME+"name", TextOutputFormat.class,
                Text.class, Text.class);

        MultipleOutputs.addNamedOutput(job, DOC_GROUP_NAME, TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, DOC_GROUP_NAME + "name", TextOutputFormat.class,
                Text.class, Text.class);

        MultipleOutputs.addNamedOutput(job, QUERY_GROUP_NAME, TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QUERY_GROUP_NAME + "name", TextOutputFormat.class,
                Text.class, Text.class);




        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(FeaturesMapper.class);
        //job.setCombinerClass(FeaturesCombiner.class);
        job.setReducerClass(FeaturesReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(10);

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
        //BasicConfigurator.configure();

        System.out.println("jobs.FeaturesJob Started!");

        deleteDirectory(new File(args[1]));

        long startTime = System.nanoTime();

        int exitCode = ToolRunner.run(new FeaturesJob(), args);

        long endTime = System.nanoTime();

        long duration = (endTime - startTime)/10000000;

        System.out.println("TIME: " + duration);

        System.exit(exitCode);
    }
}