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
    public static long N = 100000;
    public static int Iterations = 3;

    private static final String HEADER_NODE = "<NODE>";
    private static final String HEADER_REC = "<REC>";




    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input_text = value.toString();

            int split_index = input_text.indexOf("\t");
            String header_url = input_text.substring(0, split_index);
            input_text = input_text.substring(split_index+1);

            Record rec = new Record();
            rec.parseString(input_text);

            context.write(new Text(rec.head.getLink()), new Text(HEADER_REC + rec.toString()));

            if(rec.out_nodes.size() == 0)
            {
                return;
            }

            double to_pr = rec.head.getPR()/rec.out_nodes.size();
            for(LinkNode n : rec.out_nodes)
            {
                if (n.getLink().length() == 0)
                {
                    continue;
                }

                LinkNode to = new LinkNode();
                to.link = rec.head.link;
                to.pr = to_pr;
//                n.pr  = to_pr;
//
//                Record node_rec = new Record();
//                node_rec.head = rec.head;

                context.write(new Text(n.getLink()), new Text(HEADER_NODE + to.toString()));
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text,Text, Text, Text>
    {
        @Override
        protected void reduce(Text node_url, Iterable<Text> nodes_text, Context context) throws IOException, InterruptedException {

            Record rec = new Record();
            double rank = 0;

            for (Text tt : nodes_text) {
                String text_data = tt.toString();
                if(text_data.contains(HEADER_NODE))
                {
                    text_data = text_data.replace(HEADER_NODE, "");
                    LinkNode n = new LinkNode();
                    n.parseString(text_data);
                    rank += n.pr;
                    continue;
                }

                if(text_data.contains(HEADER_REC))
                {
                    text_data = text_data.replace(HEADER_REC, "");
                    rec.parseString(text_data);
                }
            }

            rank =  (alpha) / N + (1.0 - alpha) * rank;
            rec.head.pr = rank;

            context.write(new Text(node_url), new Text(rec.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
//        N = Integer.parseInt(System.getProperty("N", "5"));
//        alpha = Double.parseDouble(System.getProperty("alpha", "0.1"));
//        Iterations = Integer.parseInt(System.getProperty("iter", "2"));

        int iterations = Iterations;//Integer.parseInt(args[0]);
        String input_file = args[0];
        String output = args[1];
        Configuration conf = getConf();

        String inputFormat  = "%s/it%02d/part-r-*";
        String outputFormat = "%s/it%02d/";

        String inputStep = "", outputStep = "";
        Job[] steps = new Job[iterations];

        outputStep = String.format(outputFormat, output, 1);
        steps[0] = GetJobConf(conf, input_file, outputStep, 1);
        boolean job_res = steps[0].waitForCompletion(true);
        if(!job_res)
        {
            return 1;
        }

        for (int i = 1; i < iterations; i++) {
            inputStep  = String.format(inputFormat,  output, i);
            outputStep = String.format(outputFormat, output, i + 1);
            steps[i] = GetJobConf(conf, inputStep, outputStep, i + 1);
            job_res = steps[i].waitForCompletion(true);
            if(!job_res)
            {
                return 1;
            }
        }
        return 0;
    }


    private static Job GetJobConf(Configuration conf, String input, String output, int currentIteration) throws IOException {
        Job job = Job.getInstance(conf);
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

//        //TODO: TEST
//        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new PageRankJob(), args);
        System.exit(exitCode);
    }
}