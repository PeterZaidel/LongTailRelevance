import com.sun.jersey.core.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.print.DocFlavor;
import javax.xml.soap.Node;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.ZipException;




public class PageRankJob extends Configured implements Tool {

    public static double alpha = 0.1;
    public static long N = 36866 + 5646; //57441; //4086514; ////
    public static long END_NODES = 36866;//3538126;
    public static int Iterations = 2;
    public static int Reducers = 5;

    public static int CUR_ITER = 0;

    private static final String HEADER_NODE = "<<NODE>>";
    private static final String HEADER_NODE_OUT = "<NODE_OUT>";
    private static final String HEADER_REC = "<<REC>>";

    public static final String PR_GROUP = "PageRank_GROUP";
    public static final String PR_END_SUM = "PR_END_SUM";

    protected static String EnSumFile = "";


    public static class CountEndNodesPRMapper extends Mapper<LongWritable, Text, Text, Text>{

        Text end_node_head = new Text("<END_NODE>");
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input_text = value.toString();

            int split_index = input_text.indexOf("\t");
            String header_url = input_text.substring(0, split_index);
            input_text = input_text.substring(split_index+1);

            try {
                Record rec = new Record();
                rec.parseString(input_text);

                if(rec.out_nodes.size() == 0)
                {
                    context.write(end_node_head, new Text(rec.head.toString()));
                }
            }
            catch (Exception e)
            {
                return;
            }


        }
    }

    public static class CountEndNodesPRReducer  extends  Reducer<Text,Text, Text, Text>{
        @Override
        protected void reduce(Text head, Iterable<Text> nodes_text, Context context) throws IOException, InterruptedException {
            double sum_end_nodes_pr = 0;
            for(Text t : nodes_text)
            {
                LinkNode n = new LinkNode();
                n.parseString(t.toString());

                sum_end_nodes_pr += n.pr;
            }

            context.write(new Text(""), new Text(Double.toString(sum_end_nodes_pr)));
        }
    }




    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        double enSumPR = 0.0;

        public void setup(Mapper.Context context) throws IOException
        {
            System.out.println("CURRENT_ITER: " + Integer.toString(CUR_ITER));

            try {
                FileSystem fs = FileSystem.get(new Configuration());
                Path p = new Path(EnSumFile + "/part-*");
                FileStatus[] statuses = fs.globStatus(p);
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath())) );
                    String line;
                    line=br.readLine();
                    while (line != null){
                        line = line.replace("\t", "");
                        double val = Double.parseDouble(line);
                        enSumPR += val;
                        line=br.readLine();
                    }
                }
            }
            catch (Exception e)
            { }
            finally
            { }


            System.out.println("enSumPR = "+Double.toString(enSumPR));

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input_text = value.toString();

            int split_index = input_text.indexOf("\t");
            String header_url = input_text.substring(0, split_index);
            input_text = input_text.substring(split_index+1);

            Record rec = new Record();
            rec.parseString(input_text);
//
            if(CUR_ITER == 0)
            {
                context.getCounter(PR_GROUP, "DEFAULT_PR").increment(1);
                rec.head.pr = 1.0/N;
            }

            //rec.head.pr += enSumPR/N;

            context.write(new Text(header_url), new Text(HEADER_REC + rec.toString()));


            if(rec.out_nodes.size() == 0)
            {
                context.getCounter(PR_GROUP, PR_END_SUM).increment(1);
            }

            long out_size = 0;
            for(LinkNode n: rec.out_nodes)
            {
                if(n.link.length() > 0)
                {
                    out_size++;
                }
            }

            if(out_size == 0)
            {
                return;
            }


            double to_pr = rec.head.pr/out_size;
            for(LinkNode n_to : rec.out_nodes)
            {
                if(n_to.link.length() == 0)
                {
                    continue;
                }

                LinkNode from = new LinkNode();
                from.link = rec.head.link;
                from.pr = to_pr;

                context.write(new Text(n_to.getLink()), new Text(HEADER_NODE + Double.toString(to_pr)));
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text,Text, Text, Text>
    {

        double enSumPR = 0.0;

        public void setup(Reducer.Context context) throws IOException
        {

            try {
                FileSystem fs = FileSystem.get(new Configuration());
                Path p = new Path(EnSumFile + "/part-*");
                FileStatus[] statuses = fs.globStatus(p);
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath())) );
                    String line;
                    line=br.readLine();
                    while (line != null){
                        line = line.replace("\t", "");
                        double val = Double.parseDouble(line);
                        enSumPR += val;
                        line=br.readLine();
                    }
                }
            }
            catch (Exception e)
            { }
            finally
            { }


            System.out.println("enSumPR = "+Double.toString(enSumPR));

        }

        @Override
        protected void reduce(Text node_url, Iterable<Text> nodes_text, Context context) throws IOException, InterruptedException {

            Record rec = new Record(node_url.toString());
            double rank = 0;

            boolean loaded_rec = false;

            List<LinkNode> in_nodes = new LinkedList<>();
            List<LinkNode> out_nodes = new LinkedList<>();
            for (Text tt : nodes_text)
            {
                String text_data = tt.toString();
                if(text_data.contains(HEADER_NODE))
                {
                    text_data = text_data.replace(HEADER_NODE, "");
                    double pr = Double.parseDouble(text_data);
                    rank += pr;
//                    LinkNode n = new LinkNode();
//                    n.parseString(text_data);
//
////                    in_nodes.add(n);
//
//                    rank += n.pr;
                }

                if(text_data.contains(HEADER_REC))
                {
                    loaded_rec = true;
                    text_data = text_data.replace(HEADER_REC, "");
                    rec.parseString(text_data);
                }
            }

//            rec.in_nodes = in_nodes;

//            if(rec.out_nodes.size() == 0) {
//                rank =  (alpha) / N + (1.0 - alpha) * rank;
//                //rec.head.pr = rank;
//            }
//            else {
//                rank =  (alpha) / N + (1.0 - alpha) * (rank + enSumPR / (N-END_NODES));
//                //rec.head.pr = rank;
//            }


            rank = 0.1 * 1.0/ N + (1- 0.1) *(rank+enSumPR/N);
            //rank = rank + enSumPR/N;
            if(Math.abs(rank - rec.head.pr) < 1E-4)
            {
                context.getCounter(PR_GROUP, "STAB").increment(1);
            }
            //rec.head.pr =  rank;
            rec.head.pr =  rank;


            context.write(new Text(node_url), new Text(rec.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
//        N = Integer.parseInt(System.getProperty("N", "5"));
//        alpha = Double.parseDouble(System.getProperty("alpha", "0.1"));
//        Iterations = Integer.parseInt(System.getProperty("iter", "2"));
        System.out.println("PAGE RANK JOB!!!");
        System.out.println("Iterations: " + Integer.toString(Iterations));


        int iterations = Iterations;//Integer.parseInt(args[0]);
        String input_file = args[0];
        String output = args[1];
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(new Configuration());

        String inputFormat  = "%s/it%02d/part-r-*";
        String outputFormat = "%s/it%02d/";
        String en_sumOutputFormat = "%s/en_sum/";

        String inputStep = "", outputStep = "";
        String ensumInputStep = "",ensumOutputStep = "";

        ArrayList<Job> steps = new ArrayList<>();

        ensumOutputStep = String.format(en_sumOutputFormat, output, 1);
        fs.delete(new Path(ensumOutputStep), true);
        PageRankJob.EnSumFile = ensumOutputStep;


        Job enSumJob = GetCountEndNodesConf(conf, input_file, ensumOutputStep );
        if(!enSumJob.waitForCompletion(true))
        {
            System.out.println("Error in enSumJob! iter: 0");
            return 1;
        }

        System.out.println("PageRank iter: 0");
        outputStep = String.format(outputFormat, output, 1);
        CUR_ITER = 0;
        Job prJob = GetJobConf(conf, input_file, outputStep, 1);
        steps.add(prJob);
        if(!steps.get(0).waitForCompletion(true))
        {
            System.out.println("Error in PRJob! iter: 0");
            return 1;
        }
        double stabilizedPercent = steps.get(0).getCounters().findCounter(PR_GROUP, "STAB").getValue()/ (double)(N);
        System.out.println("STABILIZATION: " + stabilizedPercent);
        System.out.println("SET_DEF_PR: " + steps.get(0).getCounters().findCounter(PR_GROUP, "DEFAULT_PR").getValue());



        for (int i = 1; i <10; i++) {

            System.out.println("PageRank iter: " + Integer.toString(i));
            inputStep  = String.format(inputFormat,  output, i);
            outputStep = String.format(outputFormat, output, i + 1);

            ensumInputStep = String.format(inputFormat,  output, i);
            ensumOutputStep = String.format(en_sumOutputFormat, output);
            fs.delete(new Path(ensumOutputStep), true);

            enSumJob = GetCountEndNodesConf(conf, ensumInputStep, ensumOutputStep );
            if(!enSumJob.waitForCompletion(true))
            {
                System.out.println("Error in enSumJob! iter: " + Integer.toString(i));
                return 1;
            }

            CUR_ITER = i;
            prJob = GetJobConf(conf, inputStep, outputStep, i + 1);
            steps.add(prJob);
            if(!steps.get(i).waitForCompletion(true))
            {
                System.out.println("Error in PRJob! iter: " + Integer.toString(i));
                return 1;
            }

            stabilizedPercent = steps.get(i).getCounters().findCounter(PR_GROUP, "STAB").getValue()/ (double)(N);
            System.out.println("STABILIZATION: " + stabilizedPercent);
            System.out.println("SET_DEF_PR: " + steps.get(i).getCounters().findCounter(PR_GROUP, "DEFAULT_PR").getValue());

            if(1.0 - stabilizedPercent < 0.1)
            {
                //break;
            }
        }


        return 0;
    }


    private static Job GetJobConf(Configuration conf, String input, String output, int currentIteration) throws IOException {
        Job job = Job.getInstance(conf);

//        job.getCounters().findCounter(PR_GROUP, PR_END_SUM).setValue(0);

        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

//        FileSystem fs = new Path("/").getFileSystem(conf);

        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(Reducers);

        return job;
    }


    private static Job GetCountEndNodesConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);

        job.setJarByClass(PageRankJob.class);
        job.setJobName("END Nodes Count");

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(CountEndNodesPRMapper.class);
        job.setReducerClass(CountEndNodesPRReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

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

        System.out.println("PageRank Started! ");

//        //TODO: TEST
        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new PageRankJob(), args);
        System.exit(exitCode);
    }
}