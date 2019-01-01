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
import javax.ws.rs.HEAD;
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




public class HITSJob extends Configured implements Tool {

    public static double alpha = 0.1;
    public static long N = 100000;
    public static int Iterations = 3;

    private static String OUT_HEADER = "<OUT>";
    private static String IN_HEADER = "<IN>";
    private static String HEAD_HEADER = "<HEAD>";



    public static class HITSMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input_text = value.toString();

            int split_index = input_text.indexOf("\t");
            String header_url = input_text.substring(0, split_index);
            input_text = input_text.substring(split_index+1);

            Record rec = new Record();
            rec.parseString(input_text);

            rec.head.a = 0.0;
            for(LinkNode n : rec.out_nodes)
            {
                rec.head.a += n.h;
            }

            rec.head.h = 0.0;
            for(LinkNode n : rec.out_nodes)
            {
                rec.head.h += n.a;
            }

            context.write(new Text(rec.head.getLink()), new Text(HEAD_HEADER + rec.head.toString()));

            for(LinkNode n: rec.out_nodes)
            {
                context.write(new Text(n.getLink()), new Text(OUT_HEADER + rec.head.toString()));
            }

            for(LinkNode n: rec.in_nodes)
            {
                context.write(new Text(n.getLink()), new Text(IN_HEADER + rec.head.toString()));
            }


//            context.write(new Text(rec.head.getLink()), new Text(rec.toString()));

//            if(rec.out_nodes.size() == 0)
//            {
//                return;
//            }
//
//            for(LinkNode n : rec.out_nodes)
//            {
//                Record node_rec = new Record(rec.head);
////                node_rec.head = rec.head;
//
//                context.write(new Text(n.getLink()), new Text(node_rec.toString()));
//            }
        }
    }

    public static class HITSReducer extends Reducer<Text,Text, Text, Text>
    {
        @Override
        protected void reduce(Text head_node_data, Iterable<Text> nodes_data, Context context) throws IOException, InterruptedException {
            Record rec = new Record();

            for(Text t : nodes_data)
            {
                String data = t.toString();
                if(data.contains(IN_HEADER)) {
                    data = data.replace(IN_HEADER, "");

                }

                if(data.contains(OUT_HEADER)) {
                    data = data.replace(OUT_HEADER, "");
                    LinkNode n = new LinkNode();
                    n.parseString(data);

                    rec.out_nodes.add(n);
                }

                if(data.contains(HEAD_HEADER)) {
                    data = data.replace(HEAD_HEADER, "");
                    rec.head.parseString(data);
                }
            }

            context.write(new Text(rec.head.getLink()), new Text(rec.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
/*
N = Integer.parseInt(System.getProperty("N"));
alpha = Double.parseDouble(System.getProperty("alpha", "0.1"));
Iterations = Integer.parseInt(System.getProperty("iter", "5"));
Job job =  GetJobConf(getConf(), args[0], args[1], 1);
//        if (System.getProperty("mapreduce.input.indexedgz.bytespermap") != null) {
//            throw new Exception("Property = " + System.getProperty("mapreduce.input.indexedgz.bytespermap"));
//        }
return job.waitForCompletion(true) ? 0 : 1;
*/

        int iterations = Iterations;//Integer.parseInt(args[0]);
        System.out.println("Iters: " + Integer.toString(iterations));
        String input_file = args[0];
        String output = args[1];
        Configuration conf = getConf();

        String outputFormat = "%s/it%02d/";
        String inputFormat  = "%s/it%02d/part-r-*";


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

            System.out.println("iter: " + Integer.toString(i));

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

        FileSystem fs = new Path("/").getFileSystem(conf);

        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(HITSMapper.class);
        job.setReducerClass(HITSReducer.class);


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

        int exitCode = ToolRunner.run(new HITSJob(), args);
        System.exit(exitCode);
    }
}