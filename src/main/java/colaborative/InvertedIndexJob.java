package colaborative;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;



public class InvertedIndexJob extends Configured implements Tool {

    private static final String DELIMETER = "\t";
    private static final String PAIRS_FILE_TAG = "<PFTAG>";
    private static final String INPUT_FILE_TAG = "<INPUT_TAG>";
    private static final String TAG_DELIMETER = "<DELIM_TAG>";
    private static final String SVM_DELIMETER = " ";


    private static String queries_filename = "";
    private static String url_data_filename = "";
    private static String known_queries_dir = "";

    private static  final String SIMILAR_BY_TEXT_TAG = "TEXT";
    private static final String SIMILAR_BY_LINKS_TAG = "LINKS";

    private static final String TEXT_DIST_TAG = "<TEXT>";
    private static final String LINK_DIST_TAG = "<LINK>";

    private static final String QUERIES_FILENAME_TAG = "queries_filename";
    private static final String URLS_FILENAME_TAG = "urls_filename";
    private static final String KNOWN_QUERIES_DIR = "known_queries_dir";

    private static final String KNOWN_QUERY_GROUP = "KNOWNQ";
    private static final String ALL_QUERY_GROUP = "ALLQ";

    private static int MAX_SIMILAR = 20;




    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text one = new Text("1");
        private HashMap<String, Integer> urls_map = new HashMap<>();

        private HashMap<String, QueryFeatures> knownQueriesFeatures = new HashMap<>();
        private HashSet<String> wordsVocab = new HashSet<>();

        private void readKnownQueries(String path, HashMap<String, QueryFeatures> res, Mapper.Context context) throws IOException, CompressorException {
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());

//            Path inPath = new Path(path);
//
//            FileStatus[] _fss = fileSystem.listStatus(new Path(path));
//            String strFs = "";
//            for(FileStatus _fs : _fss)
//            {
//                strFs += _fs + "|";
//            }
//            if(strFs.length() > 0) {
//                throw new IOException("LITS STATUS: " + strFs);
//            }

            for(FileStatus fileStatus: fileSystem.listStatus(new Path(path))) {

                Path unique_file = fileStatus.getPath();
                //BufferedReader br =Utils.getTextFileReader(fs.getPath().toUri().getPath(), context.getConfiguration()); //utils.Utils.getBufferedReaderForCompressedFile(fs.getPath().toUri().getPath());
                FileSystem fs = unique_file.getFileSystem(context.getConfiguration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(unique_file), StandardCharsets.UTF_8));

                String line = br.readLine();
                while (line != null) {
                    String[] args = line.split("\t", 2);

                    QueryFeatures qf = new QueryFeatures();
                    qf.parseKeyString(args[0]);
                    qf.parseValueString(args[1]);

                    res.put(args[0], qf);

                    line = br.readLine();
                }
            }
        }

        private void readKnownQueries2(String path, HashMap<String, QueryFeatures> res, Mapper.Context context) throws Exception {
            List<String> lines = Utils.readLines(new Path(path), context.getConfiguration());
            for(String line : lines)
            {
                String[] args = line.split("\t", 2);

                QueryFeatures qf = new QueryFeatures();
                qf.parseKeyString(args[0]);
                qf.parseValueString(args[1]);

                res.put(args[0], qf);
            }
        }

        private static HashSet<String> createVacab(HashMap<String, QueryFeatures> knownQueriesFeatures)
        {
            HashSet<String> vocab = new HashSet<>();
            for(QueryFeatures qf : knownQueriesFeatures.values())
            {
                String[] words = qf.query.split("\\W+");
                for(String w : words)
                {
                    vocab.add(w);
                }
            }
            return vocab;
        }

        public void setup(Mapper.Context context) throws IOException, IOException {

            url_data_filename = context.getConfiguration().get(URLS_FILENAME_TAG);
            known_queries_dir = context.getConfiguration().get(KNOWN_QUERIES_DIR);


            urls_map = new HashMap<>();
            Utils.readToHashMap(context.getConfiguration(), url_data_filename, urls_map, null, false);

            knownQueriesFeatures = new HashMap<>();
            try {
                readKnownQueries2(known_queries_dir, knownQueriesFeatures, context);
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }

            wordsVocab = createVacab(knownQueriesFeatures);
//
//            throw  new IOException("KNOWN QUERIES: "+ knownQueriesFeatures.size());


        }

        private boolean filterQuery(QueryFeatures qf)
        {
            int countUrls = 0;
            for(String url : qf.clickedLinks)
            {
                if(urls_map.containsKey(url))
                {
                    countUrls += 1;
                }
            }

            if( (double)countUrls/(double) qf.clickedLinks.size()  < 2.0/5.0)
            {
                return false;
            }


            String[] words = qf.query.split("\\W+");
            int contains_words = 0;
            for(String w : words)
            {
                if(wordsVocab.contains(w))
                {
                    contains_words += 1;
                }
            }

            if((double)contains_words/ (double)words.length < 1.0 / 3.0)
            {
                return false;
            }
            return true;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String path = ((FileSplit) context.getInputSplit()).getPath().toString();

            String[] args = value.toString().split("\t", 2);

            QueryFeatures qf = new QueryFeatures();
            qf.parseKeyString(args[0]);
            qf.parseValueString(args[1]);

//            if(!filterQuery(qf))
//            {
//                return;
//            }

            context.getCounter("InvIndexJob", "FILTERED_Q").increment(1);

            for(String w : qf.query.split("\\W+"))
            {
                context.write(new Text(w), new Text(qf.query));
            }


        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
    {


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder resStrBuilder = new StringBuilder();
            for(Text v : values)
            {
                resStrBuilder.append(v).append(DELIMETER);
            }

            String resValue = resStrBuilder.toString();
            resValue = resValue.substring(0, resValue.length()-DELIMETER.length());

            context.write(key, new Text(resValue));

        }

    }

    @Override
    public int run(String[] args) throws Exception
    {
        String input_file = args[0];

        String output_file = args[1];
        deleteDirectory(new File(output_file));

        String queries_filename = args[2];
        String urls_filename = args[3];

        String known_queries_dir = args[4];

        Configuration conf = getConf();

        conf.set(QUERIES_FILENAME_TAG, queries_filename);
        conf.set(URLS_FILENAME_TAG, urls_filename);
        conf.set(KNOWN_QUERIES_DIR, known_queries_dir);

        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");


        Job job = GetJobConf(conf,input_file,  output_file );
        return job.waitForCompletion(true) ? 0 : 1;
    }


    private static Job GetJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(InvertedIndexJob.class);
        job.setJobName(InvertedIndexJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);

        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        TextOutputFormat.setOutputPath(job, new Path(output));


        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(30);

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
        System.out.println("InvertedIndexJob Started!");

//        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new InvertedIndexJob(), args);
        System.exit(exitCode);
    }
}