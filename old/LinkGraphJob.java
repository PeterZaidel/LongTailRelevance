import com.sun.jersey.core.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class LinkGraphJob extends Configured implements Tool {

    public static final String LINK_GRAPH_GROUP = "LINK_GRAPH_GROUP";
    public static String ALL_LINKS_COUNTER = "ALL_LINKS_COUNTER";
    public static String END_LINKS_COUNTER = "END_LINKS_COUNTER";


    protected static Path index_urls_path = new Path("/data/infopoisk/hits_pagerank/urls.*");
    protected static Path raw_index_path = new Path("/user/p.zaydel/ir-hw3/uniq_links/part-*");

    public static class LinkGraphMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Map<String, Integer> urls_map = new HashMap<>();
        private Map<Integer, String> inv_urls_map = new HashMap<>();

        public void setup(Mapper.Context context) throws IOException
        {
            System.out.println("LOADING URLS.TXT : " + index_urls_path.toString());
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] statuses = fs.globStatus(index_urls_path);

                int urls_count = 0;
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath())) );
                    String line;
                    line=br.readLine();
                    while (line != null){
                        String[] args = line.split("\t");
                        int id = Integer.parseInt(args[0]);
                        String url = args[1];

                        urls_map.put(url, id);
                        inv_urls_map.put(id, url);
                        urls_count += 1;

                        line=br.readLine();
                    }
                }
                System.out.println("ALL URLS: " + urls_count);
            }
            catch (Exception e)
            { }
            finally
            { }

        }


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

                if(getDomainName(url_text).contains("lenta.ru")
                        && !getDomainName(url_text).contains("icdn.lenta.ru")
                        && !url_text.endsWith(".js")
                        && !url_text.endsWith(".jpg")
                        && !url_text.endsWith(".png")
                        && !url_text.endsWith(".js")
                        && !url_text.endsWith(".ico")
                        && !url_text.endsWith(".js")
                        && !url_text.endsWith(".css")
                        )
                {
                    res.add(url_text);
                }
                else
                {
//                    if(getDomainName(url_text).contains("lenta.ru"))
//                        matchEnd++;
                }
            }
            return res;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String[] splitted_data = value.toString().split("\t");
             int doc_id = Integer.decode(splitted_data[0]);
             String raw_text = splitted_data[1];

             if(!inv_urls_map.containsKey(doc_id))
             {
                 System.out.println("MAP LOST DOC_ID: " + doc_id);
                 return;
             }

             String doc_url = inv_urls_map.get(doc_id);

             String html_text = null;
             try
             {
                html_text = decompress(raw_text);
                List<String> out_links = find_urls(html_text);

//                if(out_links.size() == 0)
//                {
//                    context.getCounter(LINK_GRAPH_GROUP, END_LINKS_COUNTER).increment(1);
//                    return;
//                }


                StringBuilder out_str = new StringBuilder("<OUT>");
                for(String url: out_links)
                {
                    if (url.length() > 0) {
                        out_str.append(url).append("\t");
                    }
                }

                if(doc_url == null)
                {
                    System.out.println("MAP NULL EXCEPTION DOC_URL");
                    return;
                }


                context.write(new Text(doc_url), new Text(out_str.toString()));

                for(String url: out_links)
                {
                    context.write(new Text(url), new Text(doc_url));
                }



             } catch (Exception e) {
                System.out.println("MAP_EXCEPTION: " + e.getMessage());
             }


        }
    }

    public static class LinkGraphReducer extends Reducer<Text, Text, Text, Text>
    {
        static final String Header = "<HEAD>";
        static final String OUT_HEAD = "<OUT>";

//        private Map<String, Integer> urls_map = new HashMap<>();
//        private Map<Integer, String> inv_urls_map = new HashMap<>();
//
//        public void setup(Reducer.Context context) throws IOException
//        {
//            try {
//                FileSystem fs = FileSystem.get(new Configuration());
//                FileStatus[] statuses = fs.globStatus(raw_index_path);
//                for (FileStatus status : statuses)
//                {
//                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath())) );
//                    String line;
//                    line=br.readLine();
//                    while (line != null){
//                        String[] args = line.split("\t");
//                        int id = Integer.parseInt(args[0]);
//                        String url = args[1];
//
//                        urls_map.put(url, id);
//                        inv_urls_map.put(id, url);
//
//                        line=br.readLine();
//                    }
//                }
//            }
//            catch (Exception e)
//            { }
//            finally
//            { }
//
//        }

        @Override
        protected void reduce(Text url_t, Iterable<Text> data, Context context) throws IOException, InterruptedException {

            List<String> out_links = new ArrayList<>();
            List<String> in_links = new ArrayList<>();
            String header_url = url_t.toString();
//
//            String header_url = inv_urls_map.get(Integer.parseInt(url_t.toString()));

            try {


                for (Text t : data)
                {
                    String str_t = t.toString();

                    if (str_t.contains(OUT_HEAD))
                    {
                        str_t = str_t.replace(OUT_HEAD, "");
                        if (str_t.length() > 0)
                        {
                            out_links.addAll(Arrays.asList(str_t.split("\t")));
                        }
                    }
                    else {
                        in_links.add(str_t);
                    }

                }
            }
            catch (Exception e)
            {
                System.out.println("REDUCER_EXC_1: " + e.getMessage());
            }


            try {
                Record rec = new Record(header_url, out_links, in_links);
                if (rec.out_nodes.size() == 0) {
                    context.getCounter(LINK_GRAPH_GROUP, END_LINKS_COUNTER).increment(1);
                }
                if(rec.in_nodes.size() == 0)
                {
                    context.getCounter(LINK_GRAPH_GROUP, "IN_ZERO").increment(1);
                }
                if(rec.in_nodes.size() == 0 && rec.out_nodes.size() == 0)
                {
                    context.getCounter(LINK_GRAPH_GROUP, "ALL_ZERO").increment(1);
                }

                context.write(new Text(header_url), new Text(rec.toString()));
            }
            catch (Exception e)
            {
                System.out.println("REDUCER_EXC_2: " + e.getMessage());
            }

        }
    }

    @Override
    public int run(String[] args) throws Exception {

        System.out.println("GRAPH JOB START!!!!!");
        Job job = GetJobConf(getConf(), args[0], args[1]);
        int res = job.waitForCompletion(true) ? 0 : 1;

        System.out.println("END_NODES: " + job.getCounters().findCounter(LINK_GRAPH_GROUP, END_LINKS_COUNTER).getValue());
        System.out.println("IN_ZERO_COUNTER: " + job.getCounters().findCounter(LINK_GRAPH_GROUP, "IN_ZERO").getValue());
        System.out.println("ALL_ZERO_COUNTER: " + job.getCounters().findCounter(LINK_GRAPH_GROUP, "ALL_ZERO").getValue());

        return res;
    }

    private static Job GetJobConf(Configuration conf, final String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(LinkGraphJob.class);
        job.setJobName(LinkGraphJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(LinkGraphMapper.class);
        job.setReducerClass(LinkGraphReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
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

////        //TODO: TEST
        deleteDirectory(new File(args[1]));
//
////        System.out.println(args[2]);
//
        LinkGraphJob.index_urls_path = new Path(args[3]);
        LinkGraphJob.raw_index_path = new Path(args[3]);

        int exitCode = ToolRunner.run(new LinkGraphJob(), args);
        System.exit(exitCode);
    }
}