package utils;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Utils {

    private static Set<String> TopDomains = new HashSet<String>(Arrays.asList(".moe" , ".properties" , ".support" , ".kg" , ".press" , ".center" , ".kr" , ".global" , ".bid" , ".capital" , ".kz" , ".photo" , ".la" , ".pet" , ".li" ,
            ".mobi" , ".wtf" , ".city" , ".chat" , ".biz" , ".lt" , ".menu" , ".lu" , ".lv" , ".aero" , ".ly" , ".rip" , ".click" , ".ma" , ".md" , ".me" , ".football" , ".ml" , ".lol" , ".mn" , ".ms" , ".mu" , ".mx" , ".my" , ".mz" ,
            ".wiki" , ".credit" , ".audio" , ".ovh" , ".guru" , ".ng" , ".ni" , ".date" , ".nl" , ".host" , ".no" , ".np" , ".top" , ".plus" , ".nu" , ".christmas" , ".nz" , ".xn--d1acj3b" , ".accountant" , ".com" , ".life" , ".xxx" ,
            ".space" , ".live" , ".vet" , ".xn--j1amh" , ".ph" , ".pk" , ".pl" , ".review" , ".pn" , ".pt" , ".xn--80asehdb" , ".pw" , ".buzz" , ".py" , ".net" , ".news" , ".xyz" , ".science" , ".cab" , ".today" , ".site" , ".ae" ,
            ".xn--p1acf" , ".ag" , ".ai" , ".help" , ".academy" , ".cash" , ".tips" , ".media" , ".am" , ".faith" , ".edu" , ".ar" , ".trade" , ".at" , ".re" , ".au" , ".az" , ".ro" , ".ba" , ".ink" , ".rs" , ".ru" , ".be" , ".bg" ,
            ".xn--c1avg" , ".win" , ".bo" , ".sa" , ".yandex" , ".social" , ".br" , ".ngo" , ".work" , ".sd" , ".se" , ".market" , ".name" , ".sh" , ".si" , ".by" , ".bz" , ".sk" , ".online" , ".so" , ".ca" , ".tube" , ".agency" ,
            ".travel" , ".cc" , ".st" , ".cd" , ".su" , ".cf" , ".ch" , ".sx" , ".porn" , ".cl" , ".int" , ".cm" , ".cn" , ".co" , ".nyc" , ".th" , ".cx" , ".tj" , ".cy" , ".tk" , ".cz" , ".tl" , ".tm" , ".tn" , ".to" , ".tr" , ".de" ,
            ".tv" , ".tw" , ".dj" , ".tz" , ".team" , ".dk" , ".moscow" , ".do" , ".ua" , ".tel" , ".uk" , ".report" , ".ec" , ".us" , ".ee" , ".asia" , ".club" , ".uz" , ".works" , ".vc" , ".es" , ".eu" , ".expert" , ".vg" , ".world" ,
            ".info" , ".vn" , ".church" , ".vu" , ".one" , ".sex" , ".fi" , ".fm" , ".tools" , ".tech" , ".fr" , ".xn--80adxhks" , ".webcam" , ".ga" , ".ws" , ".ge" , ".gg" , ".racing" , ".link" , ".xn--90ais" , ".gq" , ".gr" , ".gs" ,
            ".gy" , ".ninja" , ".land" , ".diamonds" , ".gallery" , ".pro" , ".hk" , ".studio" , ".red" , ".gov" , ".hr" , ".ht" , ".hu" , ".website" , ".sale" , ".id" , ".ie" , ".download" , ".il" , ".im" , ".xn--p1ai" , ".in" , ".io" ,
            ".za" , ".ir" , ".is" , ".it" , ".house" , ".org" , ".legal" , ".zw" , ".jp" ));

    public static boolean isValidURL(String url)
    {
        url = Utils.prepareUrl(url);
        url = url.split("/")[0];
        int idx1 = url.lastIndexOf(".");
        if(idx1 < 0)
        {
            return false;
        }

        String topDomain = url.substring(idx1);
        if(TopDomains.contains(topDomain))
        {
            return true;
        }
        return false;


//        if(url.contains("."))
//        {
//            return true;
//        }
//        else
//        {
//            return false;
//        }
//        if(url.startsWith("http://")
//                || url.startsWith("https://")
//                || url.startsWith("www."))
//        {
//            return true;
//        }
//        return false;
    }

    public static String[] splitLinks(String in, String delimeter)
    {

        ArrayList<String> res = new ArrayList<>();
        String[] buf = in.split(delimeter, -1);//Splitter.on(delimeter).splitToList(in).toArray(new String[0]);//in.split(LIST_DELIMETER);

        int res_idx = 0;
        for(int i = 0; i<buf.length; i++)
        {
            if(isValidURL(buf[i]))
            {
                res.add(buf[i]);
                res_idx++;
            }
            else if(res_idx > 0)
            {
                String prev = res.get(res_idx-1);
                res.remove(res_idx-1);
                res.add(prev + delimeter + buf[i]);
            }
        }

        return res.toArray(new String[0]);
    }

    public static String prepareQuery(String query)
    {
        return query.toLowerCase().trim();
    }

    private static String[] replace_header = {"http://", "https://", "www."};
    public static String prepareUrl(String url)
    {
         String inputUrl = url;
         url = url.trim();
         for(String to_replace : replace_header)
         {
             if(url.indexOf(to_replace) == 0)
             {
                 url = url.substring(to_replace.length());
             }
         }
         return url;
//        return url.replace("http://", "").replace("https://", "").trim();
//        int protocol_pos = url.indexOf("://");
//        if (protocol_pos != -1) {
//            url = url.substring(protocol_pos+3);
//        }
//
//        return url.trim();
    }


    public static void readToHashMap(Configuration conf, String filepath, HashMap<String, Integer> map,
                                      HashMap<Integer, String> inv_map,
                                      boolean isQuery) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(filepath)),
                StandardCharsets.UTF_8) );

        String line = br.readLine();
        while (line != null){
            String[] args = line.split("\t");

            String val = args[1];
            if(isQuery)
            {
                val = Utils.prepareQuery(val);
            }
            else
            {
                val = Utils.prepareUrl(val);
            }

            map.put(val, Integer.parseInt(args[0]));
            if(inv_map != null) {
                inv_map.put(Integer.parseInt(args[0]), val);
            }

            line=br.readLine();
        }
    }

    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public static List<String> readLines(Path location, Configuration conf) throws IOException {

        FileSystem fileSystem = FileSystem.get(conf);

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        FileStatus[] items = fileSystem.listStatus(location);

        //throw new IOException("FILE STATUS: " + location.toString() + "|||\n|||" + items.length);

        if (items == null) {
            return new ArrayList<String>();
        }

        List<String> results = new ArrayList<String>();
        for(FileStatus item: items) {

            // ignoring files like _SUCCESS
            if(item.getPath().getName().startsWith("_")) {
                continue;
            }

            if(item.isDirectory()) {
                continue;
            }


            CompressionCodec codec = null;

            try {
                codec = factory.getCodec(item.getPath());
            }catch (Exception e)
            {
                throw  new IOException("CODEC ERROR" + e.getMessage());
            }
            InputStream stream = null;

            // check if we have a compression codec we need to use
            if (codec != null) {
                try {
                    stream = codec.createInputStream(fileSystem.open(item.getPath()));
                }
                catch (Exception e)
                {
                    throw  new IOException("codec.createInputStream ERROR" + e.getMessage()
                            + "||\n||" + item.getPath().toString());
                }

            }
            else {
                try {
                    stream = fileSystem.open(item.getPath());
                }catch (Exception e)
                {
                    throw  new IOException("fileSystem.open(item.getPath()); ERROR" + e.getMessage()
                            + "||\n||" + item.getPath().toString());
                }
            }

            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer, "UTF-8");
            String raw = writer.toString();
            String[] resulting = raw.split("\n");
            for(String str: raw.split("\n")) {
                results.add(str);
            }
        }
        return results;
    }


    public static BufferedReader getBufferedReaderForCompressedFile(String fileIn) throws FileNotFoundException, CompressorException {
        FileInputStream fin = new FileInputStream(fileIn);
        BufferedInputStream bis = new BufferedInputStream(fin);
        CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
        BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
        return br2;
    }

    public static BufferedReader getBufferedReaderGZip(String filename) throws IOException {
        InputStream fileStream = new FileInputStream(filename);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
        BufferedReader buffered = new BufferedReader(decoder);
        return buffered;
    }

//    public static BufferedReader getHDFSBufferedReaderBZ2(String filename, Configuration conf) throws IOException, CompressorException {
//        FileSystem fs = FileSystem.get(conf);
//
//        BufferedReader br = new BufferedReader();
//
//
//        FileInputStream fin = new FileInputStream(String.valueOf(fs.open(new Path(filename))));
//        BufferedInputStream bis = new BufferedInputStream(fin);
//
//        CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(inputStreamReader);
//        BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
//        return br2;
//    }

    public static BufferedReader getTextFileReader(String filename, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(filename)),
                StandardCharsets.UTF_8));
        return br;
    }

    public static String listToString(List<String> list, String delimeter)
    {
        StringBuilder res = new StringBuilder();
        for (String s : list)
        {
            res.append(s).append(delimeter);
        }
        res = new StringBuilder(res.substring(0, res.length() - delimeter.length()));
        return res.toString();
    }

    public static List<String> stringToList(String in, String delimeter)
    {
        ArrayList<String> res = new ArrayList<>();
        for(String v : in.split(delimeter))
        {
            if(!v.isEmpty())
            {
                res.add(v);
            }
        }
        return res;
    }
}
