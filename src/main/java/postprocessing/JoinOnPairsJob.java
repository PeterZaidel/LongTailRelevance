package postprocessing;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;



class JoinOnPairsJob extends Configured implements Tool {

    private static int QD_FEATURE_COUNT = 8;
    private static int URL_FEATURE_COUNT = 15;
    private static int QUERY_FEATURE_COUNT = 4;

    private static final String DELIMETER = "\t";
    private static final String PAIRS_FILE_TAG = "<PFTAG>";
    private static final String INPUT_FILE_TAG = "<INPUT_TAG>";
    private static final String TAG_DELIMETER = "<DELIM_TAG>";
    private static final String SVM_DELIMETER = " ";

    private static String pairs_filename = "";
    private static final String PAIRS_FILENAME_TAG = "pairs_filename";

    private static String queries_filename = "";
    private static String url_data_filename = "";


    private static String query_features_dir = "";
    private static String url_features_dir = "";

    private static final String QUERIES_FILENAME_TAG = "queries_filename";
    private static final String URLS_FILENAME_TAG = "urls_filename";

    private static final String QUERY_FEATURES_FILENAME_TAG = "query_features_file";
    private static final String URL_FEATURES_FILENAME_TAG = "url_features_file";

    private static final boolean QueryToId = true;

    public static class JoinOnPairsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text one = new Text("1");

        public void setup(Mapper.Context context) throws IOException {
            pairs_filename = context.getConfiguration().get(PAIRS_FILENAME_TAG);
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String filePathString = context.getInputSplit().toString();
            if(filePathString.contains(pairs_filename))
            {
                String val = value.toString();

                int query_len = val.indexOf(DELIMETER);
                String query = val.substring(0, query_len);

                val = val.substring(query_len+1);

                int url_len = val.indexOf(DELIMETER);
                String url = val.substring(0, url_len);

                String data = val.substring(url_len+1);
                int mark = Integer.parseInt(data);

                String outKey = query + DELIMETER + url;
                String outValue = PAIRS_FILE_TAG + TAG_DELIMETER + Integer.toString(mark);

                context.write(new Text(outKey), new Text(outValue));
            }
            else
            {

                String val = value.toString();

                int query_len = val.indexOf(DELIMETER);
                String query = val.substring(0, query_len);

                val = val.substring(query_len+1);

                int url_len = val.indexOf(DELIMETER);
                String url = val.substring(0, url_len);

                String data = val.substring(url_len+1);

                String outKey = query + DELIMETER + url;
                String outValue = INPUT_FILE_TAG + TAG_DELIMETER + data;

                context.write(new Text(outKey), new Text(outValue));
            }
        }
    }

    public static class JoinOnPairsReducer extends Reducer<Text, Text, Text, Text>
    {
        Text emp  = new Text("");

        private HashMap<String, Integer> queries_map = new HashMap<>();
        private HashMap<String, Integer> urls_map = new HashMap<>();

        private HashSet<String> saved_pairs = new HashSet<>();


        private HashMap<String, String> url_features = new HashMap<>();
        private HashMap<String, String> query_features = new HashMap<>();

        private void readToHashMap(BufferedReader br, HashMap<String, Integer> map) throws IOException {
            String line = br.readLine();
            while (line != null){
                String[] args = line.split("\t");

                map.put(args[1], Integer.parseInt(args[0]));

                line=br.readLine();
            }
        }


        private void readUrlFeatures() throws IOException, CompressorException {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            url_features = new HashMap<>();

            for(FileStatus fs: fileSystem.listStatus(new Path(url_features_dir))) {
                BufferedReader br = utils.Utils.getBufferedReaderForCompressedFile(fs.getPath().toUri().getPath());
                String line = br.readLine();
                while (line != null) {
                    String[] args = line.split("\t");

                    url_features.put(args[0], args[1]);

                    line = br.readLine();
                }
            }

        }

        private void readQueryFeatures() throws IOException, CompressorException {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            query_features = new HashMap<>();

            for(FileStatus fs: fileSystem.listStatus(new Path(query_features_dir))) {
                BufferedReader br = utils.Utils.getBufferedReaderForCompressedFile(fs.getPath().toUri().getPath());
                String line = br.readLine();
                while (line != null) {
                    String[] args = line.split("\t");

                    query_features.put(args[0], args[1]);

                    line = br.readLine();
                }
            }
        }

        public void setup(Reducer.Context context) throws IOException, IOException {
//            FileSystem fs = FileSystem.get(new Configuration());

            queries_filename = context.getConfiguration().get(QUERIES_FILENAME_TAG);
            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);

            url_features_dir = context.getConfiguration().get(URL_FEATURES_FILENAME_TAG);
            query_features_dir = context.getConfiguration().get(QUERY_FEATURES_FILENAME_TAG);

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
            Utils.readToHashMap(context.getConfiguration(), url_data_filename, urls_map, null, false);

            try {
                readUrlFeatures();
            } catch (FileNotFoundException e) {
            } catch (CompressorException e) {}


            try {
                readQueryFeatures();
            } catch (Exception e) {}
        }


        private String addFeaturesSVML(String data, String new_data, int start_idx)
        {
            String res = data;
//            int k = 0;
//            for(String v : data.split(SVM_DELIMETER))
//            {
//                k = Integer.parseInt(v.split(":")[1]);
//            }
//            k += 1;

            for (String v : new_data.split(SVM_DELIMETER))
            {
                String[] args = v.split(":");
                int new_idx = Integer.parseInt(args[0]);
                String new_val = args[1];

                new_idx = start_idx + new_idx;
                res += SVM_DELIMETER + Integer.toString(new_idx) + ":" + new_val;
            }

            return res;
        }

        private String preprocQueryFeatures(String data)
        {
            return data;
//            String[] args = data.split(SVM_DELIMETER);
//            HashSet<Integer> featureIdxs = new HashSet<>(Arrays.asList(0,1, 2, 3));
//            String res  = "";
//            for(String v: args)
//            {
//                int fidx = Integer.parseInt(v.split(":")[0]);
//                String value = v.split(":")[1];
//                if(featureIdxs.contains(fidx) && Double.parseDouble(value) > 0)
//                {
//                    res += Integer.toString(fidx) + ":" + value + SVM_DELIMETER;
//                }
//            }
//
//            return res;
        }

        private String preprocUrlFeatures(String data)
        {
            return data;
//            String[] args = data.split(SVM_DELIMETER);
//            HashSet<Integer> featureIdxs = new HashSet<>(Arrays.asList(0,13));
//            String res  = "";
//            for(String v: args)
//            {
//                int fidx = Integer.parseInt(v.split(":")[0]);
//                String value = v.split(":")[1];
//                if(featureIdxs.contains(fidx))
//                {
//                    res += Integer.toString(fidx) + ":" + value + SVM_DELIMETER;
//                }
//            }
//
//            return res;
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           try {
               String mark = "";
               String data = "";

               int values_count = 0;


               for (Text v : values) {
                   values_count += 1;
                   String[] args = v.toString().split(TAG_DELIMETER);
                   if (args[0].equals(INPUT_FILE_TAG)) {
                       data = args[1];
                   }
                   if (args[0].equals(PAIRS_FILE_TAG)) {
                       mark = args[1];
                   }
               }


               String[] key_args = key.toString().split(DELIMETER);

               if(key_args.length < 2)
               {
                  System.out.println("ARGS ERROR: " + key.toString());
                  return;
               }

               String query = key_args[0];
               String url = key_args[1];


               if(!queries_map.containsKey(query) || !urls_map.containsKey(url))
               {
                   return;
               }


               if (url_features.containsKey(url)) {
                   String url_data = url_features.get(url);
                   url_data = preprocUrlFeatures(url_data);
                   if(!url_data.isEmpty()) {
                       data = addFeaturesSVML(data, url_data, QD_FEATURE_COUNT);
                   }
               }

               if (query_features.containsKey(query)) {
                   String query_data = query_features.get(query);
                   query_data = preprocQueryFeatures(query_data);
                   if(!query_data.isEmpty()) {
                       data = addFeaturesSVML(data, query_data, QD_FEATURE_COUNT + URL_FEATURE_COUNT);
                   }
               }

               if (mark.isEmpty() || data.isEmpty()) {
                   int qid = queries_map.get(query);
                   int uid = urls_map.get(url);

                   boolean containQF = query_features.containsKey(query);
                   boolean containUF = url_features.containsKey(url);
                   boolean inMarks = mark.isEmpty();

                   if(!mark.isEmpty()) {
                       System.out.println("EMPTY PAIR: " + "QU: " + qid + "," + uid
                               + "   QF: " + containQF + "  UF: " + containUF
                               + " MARK: " + (!mark.isEmpty()) + " DATA: " + (!data.isEmpty()));
                       context.getCounter("JOP", "ZERO_PAIRS").increment(1);
                   }

                   return;
               }


               if (QueryToId) {
                   try {
                       query = Integer.toString(queries_map.get(query));
                       url = Integer.toString(urls_map.get(url));
                   } catch (Exception e) {
                       System.out.println("EMPTY PAIR: " + "Q: " + query + "," + url);
                       throw new InterruptedException(e.getMessage());
                   }
               }




               if (saved_pairs.contains(query + DELIMETER + url)) {
                   return;
               } else {
                   saved_pairs.add(query + DELIMETER + url);
               }


               String outValue = mark + SVM_DELIMETER + "qid:" + query + SVM_DELIMETER;
               outValue += data;

               String[] features = data.split(" ");
               HashSet<Integer> uniqFeatureIdxs = new HashSet<>();
               for (String fs : features)
               {
                   if(fs.isEmpty())
                       continue;

                   int fidx = Integer.parseInt(fs.split(":")[0]);
                   if(uniqFeatureIdxs.contains(fidx))
                   {
                       System.out.println("Error in feature indexes: " + url + "    " + outValue);
                       return;
                   }else
                   {
                       uniqFeatureIdxs.add(fidx);
                   }

               }

               try {

                   context.write(new Text(url), new Text(outValue));
               } catch (Exception e) {
                   throw new InterruptedException(e.getMessage());
               }
           }
           catch (Exception e)
           {
               throw new InterruptedException(e.getMessage());
           }
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
        String input_file = args[0];
        String pairs_file = args[1];

        String output_file = args[2];
        Utils.deleteDirectory(new File(output_file));

        String queries_filename = args[3];
        String urls_filename = args[4];

        String query_features_dir = args[5];
        String url_features_dir = args[6];

        Configuration conf = getConf();


        conf.set(QUERIES_FILENAME_TAG, queries_filename);
        conf.set(URLS_FILENAME_TAG, urls_filename);
        conf.set(PAIRS_FILENAME_TAG, pairs_file);

        conf.set(URL_FEATURES_FILENAME_TAG, url_features_dir);
        conf.set(QUERY_FEATURES_FILENAME_TAG, query_features_dir);

        Job job = GetJobConf(conf,input_file, pairs_file, output_file );
        boolean res = job.waitForCompletion(true);

        long zero_pairs  = job.getCounters().findCounter("JOP", "ZERO_PAIRS").getValue();
        System.out.println("ZERO PAIRS: " + zero_pairs);

        return  res ? 0 : 1;
    }


    private static Job GetJobConf(Configuration conf, String input, String pairsPath, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(JoinOnPairsJob.class);
        job.setJobName(JoinOnPairsJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));

        MultipleInputs.addInputPath(job,new Path(input),TextInputFormat.class, JoinOnPairsMapper.class);
        MultipleInputs.addInputPath(job,new Path(pairsPath),TextInputFormat.class, JoinOnPairsMapper.class);
//        TextInputFormat.addInputPath(job, new Path(input));
//        TextInputFormat.addInputPath(job, new Path(pairsPath));

        job.setMapperClass(JoinOnPairsMapper.class);
        job.setReducerClass(JoinOnPairsReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        return job;
    }


    public static void main(String[] args) throws Exception {
        System.out.println("JoinOnPairs Started!");

//        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new JoinOnPairsJob(), args);
        System.exit(exitCode);
    }
}