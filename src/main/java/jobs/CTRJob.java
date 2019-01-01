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


public class CTRJob extends Configured implements Tool {

    public enum CtrType
    {
        QDOC,
        GLOBAL,
        RANK
    }

    private static CtrType CTR_TYPE = CtrType.QDOC;
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

    public static class CTRMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static LongWritable one = new LongWritable(1);

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



        private void emit_qdoc(QRecord record, Context context) throws IOException, InterruptedException {
            for(String url : record.shownLinks)
            {
                if(!urls_map.containsKey(url) && FILTER_URl)
                    continue;

                context.write(new Text(record.query + QURL_PAIR_DELIMETER + url),
                        new Text(SHOWN_TAG));
            }

            for(String url : record.clickedLinks)
            {
                if(!urls_map.containsKey(url) && FILTER_URl)
                    continue;

                context.write(new Text(record.query + QURL_PAIR_DELIMETER + url),
                        new Text(CLICK_TAG));
            }
        }

        private void emit_global(QRecord record, Context context) throws IOException, InterruptedException {
            for(String url : record.shownLinks)
            {
                if(!urls_map.containsKey(url) && FILTER_URl)
                    continue;

                context.write(new Text(url), new Text(SHOWN_TAG));
            }

            for(String url : record.clickedLinks)
            {
                if(!urls_map.containsKey(url) && FILTER_URl)
                    continue;

                context.write(new Text(url), new Text(CLICK_TAG));
            }
        }

        private void emit_rank(QRecord record, Context context) throws IOException, InterruptedException {
            for(int  i =0; i< record.shownLinks.size(); i++)
            {
                String url = record.shownLinks.get(i);
                if(!urls_map.containsKey(url) && FILTER_URl)
                    continue;

                context.write(new Text(url), new Text(SHOWN_TAG + TAG_DELIMETER + Integer.toString(i)));
            }

            for(int  i =0; i< record.clickedLinks.size(); i++)
            {
                String url = record.clickedLinks.get(i);
                if(!urls_map.containsKey(url) && FILTER_URl)
                    continue;

                context.write(new Text(url), new Text(CLICK_TAG + TAG_DELIMETER + Integer.toString(i)));
            }
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//            String filename = (context.getInputSplit()).toString();
//            filename = filename.substring(filename.lastIndexOf("/")+1);
//            filename = filename.substring(0, filename.indexOf(':'));

            QRecord record = new QRecord();
            record.parseString(value.toString());


            if(!queries_map.containsKey(record.query) && FILTER_QUERY)
            {
                return;
            }



            switch (CTR_TYPE)
            {
                case QDOC:
                    emit_qdoc(record, context);
                    break;
                case GLOBAL:
                    emit_global(record, context);
                    break;
                case RANK:
                    emit_rank(record, context);
                    break;
            }


        }
    }

    public static class CTRReducer extends Reducer<Text, Text, Text, Text>
    {

        private void emit_global(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long clicks = 0;
            long shows = 0;

            String url = key.toString();
            for(Text v : values)
            {
                String tag = v.toString();
                if(tag.equals(SHOWN_TAG))
                {
                    shows += 1;
                }

                if(tag.equals(CLICK_TAG))
                {
                    clicks +=1;
                }
            }

            if (clicks == 0)
                return;


            context.write(new Text(url), new Text(Double.toString((double) clicks/(double) shows)));
        }

        private void emit_rank(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<Integer, Long> clicks = new HashMap<Integer, Long>();
            HashMap<Integer, Long> shows = new HashMap<Integer, Long>();

            String url = key.toString();
            for(Text v : values)
            {
                String[] args = v.toString().split(TAG_DELIMETER);
                String tag = args[0];
                int rank = Integer.parseInt(args[1]);

                if(tag.equals(SHOWN_TAG))
                {
                    shows.put(rank, shows.getOrDefault(rank, 0L) + 1);
                }

                if(tag.equals(CLICK_TAG))
                {
                    clicks.put(rank, clicks.getOrDefault(rank, 0L) + 1);
                }
            }

            StringBuilder rank_ctr_builder = new StringBuilder();
            for (int r : clicks.keySet())
            {
                if(!shows.containsKey(r))
                    continue;
                double rank_shows = (double)shows.get(r);
                double rank_clicks = (double)clicks.get(r);
                rank_ctr_builder.append(Integer.toString(r)).append(FIELD_DELIMETER).append(Double.toString(rank_clicks / rank_shows)).append(LIST_DELIMETER);
            }

            String rank_ctr = rank_ctr_builder.toString();
            rank_ctr = rank_ctr.substring(0, rank_ctr.length() -1);

            context.write(new Text(url), new Text(rank_ctr));
        }

        private void emit_qdoc(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long clicks = 0;
            long shows = 0;

            String[] key_args = key.toString().split(QURL_PAIR_DELIMETER);

            if(key_args.length < 2)
            {
                System.out.println("REDUCER KEY ERROR: {key_args.length < 2} "
                        + "key: " + key_args.toString() + " ||  DELIMETER: " + QURL_PAIR_DELIMETER);
                return;
            }

            String query = key_args[0];
            String url = key_args[1];

            for (Text v : values)
            {
                String tag = v.toString();
                if(tag.equals(SHOWN_TAG))
                {
                    shows += 1;
                }

                if(tag.equals(CLICK_TAG))
                {
                    clicks +=1;
                }
            }

            if(clicks == 0) return;

            context.write(new Text(query), new Text(url+ DELIMETER + Double.toString((double) clicks/(double) shows)));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            switch (CTR_TYPE)
            {
                case QDOC:
                    emit_qdoc(key,  values, context);
                    break;
                case GLOBAL:
                    emit_global(key,  values, context);
                    break;
                case RANK:
                    emit_rank(key,  values, context);
            }
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
        job.setJarByClass(CTRJob.class);
        job.setJobName(CTRJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));

        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(CTRMapper.class);
        job.setReducerClass(CTRReducer.class);


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
        System.out.println("CTR Started!");

        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new CTRJob(), args);
        System.exit(exitCode);
    }
}