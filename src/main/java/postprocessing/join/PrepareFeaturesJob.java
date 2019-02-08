package postprocessing.join;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import utils.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;


class PrepareFeaturesJob extends Configured implements Tool {

    static class NamedParameter
    {
        public String Name;
        public String Value;

        public NamedParameter(String name)
        {
            this.Name = name;
        }

        public NamedParameter(String name, String val)
        {
            this.Name = name;
            this.Value = val;
        }

        public void getValue(Configuration conf)
        {
            this.Value = conf.get(this.Name);
        }

        public void setValue(Configuration conf)
        {
            conf.set(this.Name, this.Value);
        }

        public void setValue(Configuration conf, String value)
        {
            this.Value = value;
            this.setValue(conf);
        }
    }



    private static final String DELIMETER = "\t";
    private static final String SVM_DELIMETER = " ";
    private static final String FEATURE_DELIMETER = ":";
    private static final String FEATURE_HEADER_DELIMETER = "_";

    private static final String QD_FEATURE_HEADER = "qd";
    private static final String QUERY_FEATURE_HEADER = "query";
    private static final String URL_FEATURE_HEADER = "url";

    private static final String QD_TAG = "QD";
    private static final String QUERY_TAG = "QUERY";
    private static final String URL_TAG = "URL";
    private static final String TAG_DELIMETER = "<TAG>";



    private static final NamedParameter queries_filename = new NamedParameter("queries_filename");
    private static final NamedParameter url_data_filename = new NamedParameter("urls_filename");

//    private static String queries_filename = "";
//    private static String url_data_filename = "";
//    private static final String QUERIES_FILENAME_TAG = "queries_filename";
//    private static final String URLS_FILENAME_TAG = "urls_filename";

    private static final NamedParameter qd_dir = new NamedParameter("qd_dir");
    private static final NamedParameter query_dir = new NamedParameter("query_dir");
    private static final NamedParameter url_dir = new NamedParameter("url_dir");

    private static final NamedParameter out_qd_dir = new NamedParameter("out_qd_dir", "QD/part-");
    private static final NamedParameter out_query_dir = new NamedParameter("out_query_dir", "QUERY/part-");
    private static final NamedParameter out_url_dir = new NamedParameter("out_url_dir", "URL/part-");

//    private static final String qd_dir = "";
//    private static final String QD_DIR_TAG = "qd_dir";
//
//    private static final String query_dir = "";
//    private static final String url_dir = "";
//
//    private static final String out_qd_dir = "";
//    private static final String out_query_dir = "";
//    private static final String out_url_dir = "";

    public static class PrepareFeaturesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private HashMap<String, Integer> queries_map = new HashMap<>();
        private HashMap<String, Integer> urls_map = new HashMap<>();

        public void setup(Mapper.Context context) throws IOException {
            qd_dir.getValue(context.getConfiguration());
            query_dir.getValue(context.getConfiguration());
            url_dir.getValue(context.getConfiguration());

            queries_filename.getValue(context.getConfiguration());
            url_data_filename.getValue(context.getConfiguration());

//            queries_filename = context.getConfiguration().get(QUERIES_FILENAME_TAG);
//            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);

            queries_map = new HashMap<>();
            Utils.readToHashMap(context.getConfiguration(), queries_filename.Value, queries_map, null, true);

            urls_map = new HashMap<>();
            Utils.readToHashMap(context.getConfiguration(), url_data_filename.Value, urls_map, null, false);

        }

        private List<String> transform_features(String features_str, String header)
        {
            String[] features = features_str.split(SVM_DELIMETER);
            List<String> res = new ArrayList<>();
            for(int i =0; i <  features.length; i++)
            {
                String[] feature_args = features[i].split(FEATURE_DELIMETER);
                String new_feature = header +  FEATURE_HEADER_DELIMETER + feature_args[0]
                         + FEATURE_DELIMETER + feature_args[1];
                res.add(new_feature);
            }
            return res;
        }


        private void map_qd(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] args = value.toString().split(DELIMETER);
            String query = args[0];
            String url = args[1];
            String features_str = args[2];

            String qid = Integer.toString(queries_map.get(query));
            String urlId = Integer.toString(urls_map.get(url));

            List<String> features = transform_features(features_str, QD_FEATURE_HEADER);

            String outKey = QD_TAG + TAG_DELIMETER + qid + DELIMETER + urlId;
            String outValue = Utils.listToString(features, SVM_DELIMETER);

            context.write(new Text(outKey), new Text(outValue));
        }

        private void map_query(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] args = value.toString().split(DELIMETER);
            String query = args[0];
            String features_str = args[1];

            String qid = Integer.toString(queries_map.get(query));

            List<String> features = transform_features(features_str, QUERY_FEATURE_HEADER);

            String outKey = QUERY_TAG + TAG_DELIMETER + qid;
            String outValue = Utils.listToString(features, SVM_DELIMETER);

            context.write(new Text(outKey), new Text(outValue));
        }

        private void map_url(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] args = value.toString().split(DELIMETER);
            String url = args[0];
            String features_str = args[1];

            String urlId = Integer.toString(urls_map.get(url));

            List<String> features = transform_features(features_str, URL_FEATURE_HEADER);

            String outKey =  URL_TAG + TAG_DELIMETER + urlId;
            String outValue = Utils.listToString(features, SVM_DELIMETER);

            context.write(new Text(outKey), new Text(outValue));
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String filePathString = context.getInputSplit().toString();
            if(filePathString.contains(qd_dir.Value))
            {
                map_qd(key, value, context);
            }

            if(filePathString.contains(query_dir.Value))
            {
                map_query(key, value, context);
            }


            if(filePathString.contains(url_dir.Value)) {
              map_url(key, value, context);
            }

        }
    }

    public static class PrepareFeaturesReducer extends Reducer<Text, Text, Text, Text>
    {
        private MultipleOutputs output;

        public void setup(Reducer.Context context) throws IOException, IOException {
            output = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] key_args = key.toString().split(TAG_DELIMETER);
            String tag = key_args[0];
            String key_str = key_args[1];

            if(tag.equals(QD_TAG))
            {
                for (Text v: values) {
                    output.write(QD_TAG, new Text(key_str), v, out_qd_dir.Value);
                }
            }

            if(tag.equals(QUERY_TAG))
            {
                for (Text v: values) {
                    output.write(QUERY_TAG, new Text(key_str), v, out_query_dir.Value);
                }
            }

            if(tag.equals(URL_TAG))
            {
                for (Text v: values) {
                    output.write(URL_TAG, new Text(key_str), v, out_url_dir.Value);
                }
            }

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            output.close();
        }

    }

    @Override
    public int run(String[] args) throws Exception
    {
        int idx = 0;
        String qd_features_file = args[idx++];
        String url_feature_file = args[idx++];
        String query_feature_file = args[idx++];

        String queries_filename_str = args[idx++];
        String urls_filename_str = args[idx++];

        String output_dir = args[idx++];

        Utils.deleteDirectory(new File(output_dir));
        Configuration conf = getConf();

        queries_filename.setValue(conf, queries_filename_str);
        url_data_filename.setValue(conf, urls_filename_str);

        qd_dir.setValue(conf, qd_features_file.replace("/part-*", ""));
        query_dir.setValue(conf, query_feature_file.replace("/part-*", ""));
        url_dir.setValue(conf, url_feature_file.replace("/part-*", ""));


//        conf.set(QUERIES_FILENAME_TAG, queries_filename);
//        conf.set(URLS_FILENAME_TAG, urls_filename);


        Job job = GetJobConf(conf,qd_features_file, query_feature_file, url_feature_file , output_dir );
        boolean res = job.waitForCompletion(true);

        return  res ? 0 : 1;
    }


    private static Job GetJobConf(Configuration conf,
                                  String qd_features_file,
                                  String query_features_file,
                                  String url_features_file,
                                  String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(PrepareFeaturesJob.class);
        job.setJobName(PrepareFeaturesJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);

        MultipleOutputs.addNamedOutput(job, QD_TAG, TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QUERY_TAG, TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, URL_TAG, TextOutputFormat.class,
                Text.class, Text.class);


        TextOutputFormat.setOutputPath(job, new Path(output));

        MultipleInputs.addInputPath(job,new Path(qd_features_file),TextInputFormat.class, PrepareFeaturesMapper.class);
        MultipleInputs.addInputPath(job,new Path(query_features_file),TextInputFormat.class, PrepareFeaturesMapper.class);
        MultipleInputs.addInputPath(job,new Path(url_features_file),TextInputFormat.class, PrepareFeaturesMapper.class);

        job.setMapperClass(PrepareFeaturesMapper.class);
        job.setReducerClass(PrepareFeaturesReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        return job;
    }


    public static void main(String[] args) throws Exception {
        System.out.println("PrepareFeaturesJob Started!");

        int exitCode = ToolRunner.run(new PrepareFeaturesJob(), args);
        System.exit(exitCode);
    }
}