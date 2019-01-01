import com.sun.jersey.core.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class UniqLinksJob extends Configured implements Tool {

    public static final String UNIQ_LINKS_GROUP = "UNIQ_LINKS_GROUP";
    public static final String UNIQ_LINKS_COUNTER = "UNIQ_LINKS_COUNTER";


    protected static Path index_urls_path = new Path("/");

    public static class LinkGraphMapper extends Mapper<LongWritable, Text, Text, Text> {




//
//        static final IntWritable one = new IntWritable(1);

        static final String Header = "<HEAD>";

        // Pattern for recognizing a URL, based off RFC 3986
        private static final Pattern urlPattern = Pattern.compile(
                "(?:^|[\\W])((ht|f)tp(s?):\\/\\/|www\\.)"
                        + "(([\\w\\-]+\\.){1,}?([\\w\\-.~]+\\/?)*"
                        + "[\\p{Alnum}.,%_=?&#\\-+()\\[\\]\\*$~@!:/{};']*)",
                Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);


        private String decompress(String base64) throws IOException, DataFormatException {
            String encoded = base64;
            byte[] compressed = Base64.decode(encoded);

            try {
                // Decompress the bytes
                Inflater decompresser = new Inflater();
                decompresser.setInput(compressed, 0, compressed.length);

                byte[] result = new byte[compressed.length];
                StringBuilder outputString = new StringBuilder();
                int resultLength = decompresser.inflate(result);
                while (resultLength  > 0)
                {
                    outputString.append(new String(result, 0, resultLength, "UTF-8"));
                    resultLength = decompresser.inflate(result);
                }

                decompresser.end();

                // Decode the bytes into a String

                return outputString.toString();
            }
            catch (DataFormatException e)
            {
                return "";
            }

        }

        public static String getDomainName(String url) {
            try {
                URI uri = new URI(url);
                String domain = uri.getHost();
                return domain.startsWith("www.") ? domain.substring(4) : domain;
            }catch (Exception e)
            {
                return "";
            }
        }

        private List<String> find_urls(String text) {
            List<String> res = new ArrayList<>();
            Matcher matcher = urlPattern.matcher(text);
            while (matcher.find()) {
                int matchStart = matcher.start(1);
                int matchEnd = matcher.end();
                // now you have the offsets of a URL match

                String url_text = text.substring(matchStart, matchEnd);

                if(getDomainName(url_text).contains("lenta.ru"))
                {
                    res.add(url_text);
                }
            }
            return res;
        }

        private Text one = new Text("1");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted_data = value.toString().split("\t");
            int doc_id = Integer.decode(splitted_data[0]);
            String raw_text = splitted_data[1];


            String html_text = null;
            try {
                html_text = decompress(raw_text);
                List<String> links = find_urls(html_text);

                for(String url : links )
                {
//                    if(urls_map.containsKey(url)) {
//                        context.write(new Text(url), one);
//                        continue;
//                    }
//
//                    long new_idx = context.getCounter(UNIQ_LINKS_GROUP, UNIQ_LINKS_COUNTER).getValue();
                    context.write(new Text(url), new Text("w"));
//                    context.getCounter(UNIQ_LINKS_GROUP, UNIQ_LINKS_COUNTER).increment(1);
                }

            } catch (Exception e) {
                System.out.println("ddd");
            }


        }
    }

    public static class LongComparator extends WritableComparator {

        public LongComparator() {
            super(LongWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            Long v1 = ByteBuffer.wrap(b1, s1, l1).getLong();
            Long v2 = ByteBuffer.wrap(b2, s2, l2).getLong();

            return v1.compareTo(v2) * (-1);
        }
    }

    public static class LinkGraphReducer extends Reducer<Text, Text, LongWritable, Text>
    {

        private Map<String, Integer> urls_map = new HashMap<>();

        public void setup(Reducer.Context context) throws IOException
        {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(index_urls_path)));
                String line;
                line=br.readLine();

                int max_id = 0;

                while (line != null){
                    String[] args = line.split("\t");
                    int id = Integer.parseInt(args[0]);
                    String url = args[1];

                    urls_map.put(url, id);

                    if(id > max_id)
                    {
                        max_id = id;
                    }

                    line=br.readLine();
                }

                if(context.getCounter(UNIQ_LINKS_GROUP, UNIQ_LINKS_COUNTER).getValue() == 0) {
                    context.getCounter(UNIQ_LINKS_GROUP, UNIQ_LINKS_COUNTER).setValue(max_id+1);
                }
            }
            catch (Exception e)
            { }
            finally
            { }

        }

        @Override
        protected void reduce(Text url, Iterable<Text> data, Context context) throws IOException, InterruptedException {

            if(urls_map.containsKey(url.toString()))
            {
                context.write(new LongWritable(urls_map.get(url.toString())), url);
            }
            else {
                long new_idx = context.getCounter(UNIQ_LINKS_GROUP, UNIQ_LINKS_COUNTER).getValue();
                context.write(new LongWritable(new_idx), url);
                context.getCounter(UNIQ_LINKS_GROUP, UNIQ_LINKS_COUNTER).increment(1);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static Job GetJobConf(Configuration conf, final String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(UniqLinksJob.class);
        job.setJobName(UniqLinksJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(LinkGraphMapper.class);
        job.setReducerClass(LinkGraphReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(LongComparator.class);

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

        UniqLinksJob.index_urls_path = new Path(args[2]);

        int exitCode = ToolRunner.run(new UniqLinksJob(), args);
        System.exit(exitCode);
    }
}