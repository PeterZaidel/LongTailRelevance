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


    protected static Path index_urls_path = new Path("/");

    public static class LinkGraphMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private Map<String, Integer> urls_map = new HashMap<>();
        private Map<Integer, String> inv_urls_map = new HashMap<>();

        public void setup(Context context) throws IOException
        {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] statuses = fs.globStatus(index_urls_path);
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

                        line=br.readLine();
                    }
                }
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

                if(urls_map.containsKey(url_text)) {
                    res.add(urls_map.get(url_text).toString());
                }
            }
            return res;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String[] splitted_data = value.toString().split("\t");
             int doc_id = Integer.decode(splitted_data[0]);
             String raw_text = splitted_data[1];

//             String doc_url = inv_urls_map.get(doc_id);

//             if (!Base64.isBase64(raw_text))
//             {
//                 // if string is not base64, it is string from urls.txt
//                 // find url and add Header to it
//                 List<String> urls = find_urls(raw_text);
//                 context.write(new LongWritable(doc_id), new Text(Header + urls.get(0)));
//                 return;
//             }


            String html_text = null;
            try {
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
                    if (url.length() > 0)
                        out_str.append(url).append("\t");
                }

                context.write(new LongWritable(doc_id), new Text(out_str.toString()));

                for(String url: out_links)
                {
                    context.write(new LongWritable(Integer.parseInt(url)),
                            new Text(Integer.toString(doc_id)));
                }

//                for(String url : find_urls(html_text))
//                {
//                    context.write(new LongWritable(doc_id), new Text(url));
//                }
            } catch (DataFormatException e) {
                return;
            }


        }
    }

    public static class LinkGraphReducer extends Reducer<LongWritable, Text, Text, Text>
    {
        static final String Header = "<HEAD>";
        static final String OUT_HEAD = "<OUT>";

        @Override
        protected void reduce(LongWritable url_idx, Iterable<Text> data, Context context) throws IOException, InterruptedException {

            List<String> out_links = new ArrayList<>();
            List<String> in_links = new ArrayList<>();
            String header_link = url_idx.toString();

            for(Text t: data)
            {
                String str_t = t.toString();
                if(str_t.contains(OUT_HEAD))
                {
                    str_t = str_t.replace(OUT_HEAD, "");
                    if(str_t.length() > 0)
                        out_links.addAll(Arrays.asList(str_t.split("\t")));
                }
                else
                {
                    in_links.add(str_t);
                }

            }



            Record rec = new Record(header_link, out_links, in_links);
            if(rec.out_nodes.size() == 0)
            {
                System.out.println("end_link_id: " + url_idx);
                context.getCounter(LINK_GRAPH_GROUP, END_LINKS_COUNTER).increment(1);
            }

//            Record rec = new Record();
//            rec.head = new LinkNode(header_link);
//            rec.out_nodes = new ArrayList<>();
//            for(String s : node_links)
//            {
//                rec.out_nodes.add(new LinkNode(s));
//            }

            context.write(new Text(header_link), new Text(rec.toString()));



//            List<String> node_links = new ArrayList<>();
//
//            String header_link = "";
//            for(Text link: links)
//            {
//                String link_str = link.toString();
//                if(link_str.contains(Header)) {
//                    header_link = link_str.replaceAll(Header, "");
//                }
//                else {
//
//                    node_links.add(link_str);
//                }
//            }
//
//            Record rec = new Record();
//            rec.head = new LinkNode(header_link);
//            rec.out_nodes = new ArrayList<>();
//            for(String s : node_links)
//            {
//                rec.out_nodes.add(new LinkNode(s));
//            }
//
//            context.write(new Text(header_link), new Text(rec.toString()));


            //StringBuilder output = new StringBuilder();

 //           double rank = 0.0;

//
//            String header_link = "";
//            for(Text link: links)
//            {
//                String link_str = link.toString();
//                if(link_str.contains(Header)) {
//                    header_link = link_str.replaceAll(Header, "");
//                }
//                else {
//
//                    output.append(link.toString()).append("\t");
//                }
//            }
//
//
//            String res = "";
////            res += url_idx.toString() + "\t";
//            res += rank.toString() + "\t";
////            res += header_link + "\t";
//            res += output.toString();
//            res += "\n";

    //        context.write(new Text(header_link), new NodeWritable(header_link, rank, node_links));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
//        if (System.getProperty("mapreduce.input.indexedgz.bytespermap") != null) {
//            throw new Exception("Property = " + System.getProperty("mapreduce.input.indexedgz.bytespermap"));
//        }
        int res = job.waitForCompletion(true) ? 0 : 1;


        System.out.println(END_LINKS_COUNTER + ": " + job.getCounters().findCounter(LINK_GRAPH_GROUP, END_LINKS_COUNTER).getValue());


        return res;
    }

    private static Job GetJobConf(Configuration conf, final String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(LinkGraphJob.class);
        job.setJobName(LinkGraphJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        TextInputFormat.addInputPath(job, new Path(input));

//        FileSystem fs = new Path("/").getFileSystem(conf);
//
//        RemoteIterator<LocatedFileStatus> fileListItr = fs.listFiles(new Path(input), false);
//
//        while (fileListItr != null && fileListItr.hasNext()) {
//            LocatedFileStatus file = fileListItr.next();
//            if (file.toString().contains("txt")) {
//                TextInputFormat.addInputPath(job, file.getPath());
//            }
//        }

        job.setMapperClass(LinkGraphMapper.class);
//        job.setCombinerClass(LinkGraphReducer.class);
        job.setReducerClass(LinkGraphReducer.class);

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

        LinkGraphJob.index_urls_path = new Path(args[2]);

        int exitCode = ToolRunner.run(new LinkGraphJob(), args);
        System.exit(exitCode);
    }
}