import com.sun.jersey.core.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import javax.print.DocFlavor;
import javax.ws.rs.HEAD;
import javax.xml.soap.Node;
import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.ZipException;




public class SortingJob extends Configured implements Tool {

    public enum SortingVal {
        PR,
        HITS_A,
        HITS_H
    }


    public static SortingVal SORT_VAL = SortingVal.PR;


    public static class DoubleComparator extends WritableComparator {

        public DoubleComparator() {
            super(DoubleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            Double v1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
            Double v2 = ByteBuffer.wrap(b2, s2, l2).getDouble();

            return v1.compareTo(v2) * (-1);
        }
    }


    public static class SortingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input_text = value.toString();

            int split_index = input_text.indexOf("\t");
            String header_url = input_text.substring(0, split_index);
            input_text = input_text.substring(split_index+1);

            Record rec = new Record();
            rec.parseString(input_text);

            LinkNode head_node = rec.head;
            double val = head_node.pr;
            switch (SORT_VAL) {
                case PR:
                    val = head_node.pr;
//                    if(rec.out_nodes.size() == 0)
//                    {
//                        val = 0;
//                    }

                    break;
                case HITS_A:
                    val = head_node.a;
                    break;
                case HITS_H:
                    val = head_node.h;
                    break;
            }

            String rec_sizes = "OUT: " + Integer.toString(rec.out_nodes.size()) + " IN: " + Integer.toString(rec.in_nodes.size());
            context.write(new DoubleWritable(val), new Text(head_node.toString() + "| " + rec_sizes));
        }
    }

    public static class SortingReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text>
    {
        @Override
        protected void reduce(DoubleWritable val, Iterable<Text> nodes_data, Context context) throws IOException, InterruptedException {

            for(Text t : nodes_data)
            {
                context.write(val, new Text(t.toString()) );
            }
        }
    }

    private  String getMaxIterFiles(String path) throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        FileStatus[] fileStatus = fs.listStatus(new Path(path));

        String max_iter_path = "";
        int max_iter_num = 0;
        for(FileStatus status : fileStatus)
        {
            String itName = status.getPath().toString();
            itName = itName.split("/")[itName.split("/").length -1];
            itName = itName.replace("it","");
            try {
                int iter = Integer.parseInt(itName);

                if(iter >= max_iter_num)
                {
                    max_iter_num = iter;
                    max_iter_path = status.getPath().toString();
                }
            }
            catch (Exception e){
            }

        }
        return max_iter_path + "/part-r-*";
    }

    @Override
    public int run(String[] args) throws Exception
    {

        String res_path = args[2];
        try {
            SORT_VAL = SortingVal.PR;
            Job job = GetJobConf(getConf(), getMaxIterFiles(args[0]), res_path + "pr/");
            int res = job.waitForCompletion(true) ? 0 : 1;
        }
        catch (Exception e)
        {}

        try {
            SORT_VAL = SortingVal.HITS_A;
            Job job = GetJobConf(getConf(), getMaxIterFiles(args[1]), res_path + "hits_a/");
            int res = job.waitForCompletion(true) ? 0 : 1;
        }
        catch (Exception e)
        {}


        try {
            SORT_VAL = SortingVal.HITS_H;
            Job job = GetJobConf(getConf(), getMaxIterFiles(args[1]), res_path + "hits_h/");
            int res = job.waitForCompletion(true) ? 0 : 1;
        }
        catch (Exception e)
        {}

        return 0;
    }


    private static Job GetJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(SortingJob.class);
        job.setJobName(SortingJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(SortingMapper.class);
        job.setReducerClass(SortingReducer.class);
        job.setNumReduceTasks(1);
        job.setSortComparatorClass(DoubleComparator.class);

        job.setOutputKeyClass(DoubleWritable.class);
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

        System.out.println("Started sorting!");

//        //TODO: TEST
        deleteDirectory(new File(args[2]));

        int exitCode = ToolRunner.run(new SortingJob(), args);
        System.exit(exitCode);
    }
}