//package colaborative;
//
//import org.apache.commons.compress.compressors.CompressorException;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.*;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.io.compress.BZip2Codec;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import utils.Utils;
//
//import java.io.*;
//import java.nio.charset.StandardCharsets;
//import java.util.*;
//
//
//
//public class IndexQueryDistancesJob extends Configured implements Tool {
//
//    private static final String DELIMETER = "\t";
//    private static final String PAIRS_FILE_TAG = "<PFTAG>";
//    private static final String INPUT_FILE_TAG = "<INPUT_TAG>";
//    private static final String TAG_DELIMETER = "<DELIM_TAG>";
//    private static final String SVM_DELIMETER = " ";
//
//
//    private static String queries_filename = "";
//    private static String url_data_filename = "";
//
//    private static  final String SIMILAR_BY_TEXT_TAG = "TEXT";
//    private static final String SIMILAR_BY_LINKS_TAG = "LINKS";
//
//    private static final String TEXT_DIST_TAG = "<TEXT>";
//    private static final String LINK_DIST_TAG = "<LINK>";
//
//    private static final String QUERIES_FILENAME_TAG = "queries_filename";
//    private static final String URLS_FILENAME_TAG = "urls_filename";
//
//
//    private static int MAX_SIMILAR = 20;
//
//
//
//
//    public static class QueryDistancesMapper extends Mapper<LongWritable, Text, Text, Text> {
//        private static Text one = new Text("1");
//        private HashMap<String, Integer> queries_map = new HashMap<>();
//
//
//
//        public void setup(Mapper.Context context) throws IOException, IOException {
//
//            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);
//            queries_filename = context.getConfiguration().get(QUERIES_FILENAME_TAG);
//
//
//            queries_map = new HashMap<>();
//            Utils.readToHashMap(context.getConfiguration(), queries_filename, queries_map,
//                    null, true);
//
//
//        }
//
//
//
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//
//           String[] args = value.toString().split(DELIMETER, 2);
//           String word = args[0];
//           String[] queries = args[1].split(DELIMETER);
//
//           List<String> knownQueries = new ArrayList<>();
//           for(String q : queries)
//           {
//               if(queries_map.containsKey(q))
//               {
//                   knownQueries.add(q);
//               }
//           }
//
//           for(String knownQuery : knownQueries)
//           {
//               for(String q : queries)
//               {
//                   context.write(new Text(knownQuery + TAG_DELIMETER + q), new Text("1"));
//               }
//           }
//        }
//    }
//
//    public static class QueryDistancesReducer extends Reducer<Text, Text, Text, Text>
//    {
//        Text emp  = new Text("");
//
//        private HashMap<String, Integer> queries_map = new HashMap<>();
//        private HashMap<String, Integer> urls_map = new HashMap<>();
//
//        private MultipleOutputs<Text, Text> multipleOutputs;
//
//
//        public void setup(Reducer.Context context) throws IOException, IOException {
//            multipleOutputs = new MultipleOutputs(context);
//            queries_filename = context.getConfiguration().get(QUERIES_FILENAME_TAG);
//            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);
//
//            queries_map = new HashMap<>();
//            Utils.readToHashMap(context.getConfiguration(), queries_filename, queries_map, null, true);
//
//            urls_map = new HashMap<>();
//            Utils.readToHashMap(context.getConfiguration(), url_data_filename, urls_map, null, false);
//        }
//
//
//        @Override
//        protected void reduce(TextFloatPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            String[] keyArgs = key.getFirst().toString().split(TAG_DELIMETER);
//
//
//            String distTag = keyArgs[0];
//            String knownQuery = keyArgs[1];
//
//            List<String> similarQueries = new ArrayList<>();
//
//
//            int counter = 0;
//            for(Text v: values)
//            {
//                float dist1 = key.getSecond().get();
//                float dist2 = Float.parseFloat(v.toString().split(DELIMETER)[1]);
//
////                if(Math.abs())
////                {
////                    throw new InterruptedException(key.getFirst().toString() + "| " + v.toString() + "| " + Double.toString(dist1));
////                }
//
//                if(!knownQuery.equals(key.getFirst().toString().split(TAG_DELIMETER)[1]))
//                {
//                    throw new InterruptedException("REDUCER EXC: " + knownQuery + " ||| " + key.getFirst().toString());
//                }
//
//                String q2 = v.toString().split(DELIMETER)[0];
//
//                similarQueries.add(q2 + "," + Double.toString(dist1));
//                counter++;
//
//                if(counter >= MAX_SIMILAR) {
////                    System.out.println(counter);
//                    break;
//                }
//            }
//
//            String outValue = Utils.listToString(similarQueries, DELIMETER);
//            context.write(new Text(knownQuery), new Text(outValue));
//
//            if(distTag.equals(TEXT_DIST_TAG))
//            {
//                String filename = SIMILAR_BY_TEXT_TAG + "/part";
//                multipleOutputs.write(SIMILAR_BY_TEXT_TAG, new Text(knownQuery), new Text(outValue), filename);
//            }
//
//            if(distTag.equals(LINK_DIST_TAG))
//            {
//                String filename = SIMILAR_BY_LINKS_TAG + "/part";
//                multipleOutputs.write(SIMILAR_BY_LINKS_TAG, new Text(knownQuery), new Text(outValue), filename);
//            }
//
//
//        }
//
//        public void cleanup(Context context) throws IOException, InterruptedException {
//            super.cleanup(context);
//            multipleOutputs.close();
//        }
//    }
//
//    @Override
//    public int run(String[] args) throws Exception
//    {
//        String input_file = args[0];
//
//        String output_file = args[1];
//        deleteDirectory(new File(output_file));
//
//        String queries_filename = args[2];
//        String urls_filename = args[3];
//
//        String known_queries_dir = args[4];
//
//        Configuration conf = getConf();
//
//        conf.set(QUERIES_FILENAME_TAG, queries_filename);
//        conf.set(URLS_FILENAME_TAG, urls_filename);
//        conf.set(KNOWN_QUERIES_DIR, known_queries_dir);
//
//        conf.set("mapreduce.map.output.compress", "true");
//        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
//
//
//        Job job = GetJobConf(conf,input_file,  output_file );
//        return job.waitForCompletion(true) ? 0 : 1;
//    }
//
//
//    private static Job GetJobConf(Configuration conf, String input, String output) throws IOException {
//        Job job = Job.getInstance(conf);
//        job.setJarByClass(QueryDistancesJob.class);
//        job.setJobName(QueryDistancesJob.class.getCanonicalName());
//
//        job.setInputFormatClass(TextInputFormat.class);
//
//        TextOutputFormat.setCompressOutput(job, true);
//        TextOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//        TextOutputFormat.setOutputPath(job, new Path(output));
//
//        MultipleOutputs.addNamedOutput(job, SIMILAR_BY_LINKS_TAG, TextOutputFormat.class,
//                Text.class, Text.class);
//        MultipleOutputs.addNamedOutput(job, SIMILAR_BY_TEXT_TAG, TextOutputFormat.class,
//                Text.class, Text.class);
//
//        TextInputFormat.addInputPath(job, new Path(input));
//
//        job.setMapperClass(QueryDistancesMapper.class);
//        job.setReducerClass(QueryDistancesReducer.class);
//
//        job.setGroupingComparatorClass(TextFloatPair.mGrouper.class);
//        job.setSortComparatorClass(TextFloatPair.mKeyComparator.class);
//        job.setPartitionerClass(TextFloatPair.mPartitioner.class);
//
//
//        job.setMapOutputKeyClass(TextFloatPair.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        job.setNumReduceTasks(1);
//
//        return job;
//    }
//
//    static boolean deleteDirectory(File directoryToBeDeleted) {
//        File[] allContents = directoryToBeDeleted.listFiles();
//        if (allContents != null) {
//            for (File file : allContents) {
//                deleteDirectory(file);
//            }
//        }
//        return directoryToBeDeleted.delete();
//    }
//
//    public static void main(String[] args) throws Exception {
//        System.out.println("QueryDistancesJob Started!");
//
////        deleteDirectory(new File(args[1]));
//
//        int exitCode = ToolRunner.run(new QueryDistancesJob(), args);
//        System.exit(exitCode);
//    }
//}