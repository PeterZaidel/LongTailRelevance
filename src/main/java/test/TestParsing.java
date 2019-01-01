package test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
//import utils.Utils;
import utils.Utils;
import writables.QRecord;

import java.io.*;
import java.util.*;


public class TestParsing {

    @Test
    public void testUrlsListParse() throws IOException {
        String filename = "/media/peter/DATA1/Data/InfoSearch/hw5/data/url.data";
        HashMap<String, Integer> urls_map = new HashMap<>();
        Utils.readToHashMap(new Configuration(), filename, urls_map,null, false);

        List<String> badUrls = new ArrayList<>();
        Set<String> topDomains = new HashSet<>();
        for(String url : urls_map.keySet())
        {
            badUrls.add(url);
            if(url.contains(","))
            {
                String out = "";
                for(String ss : url.split(","))
                {
                    out+= ss + "  ,  ";
                }
//                System.out.println(out);
            }
            String url2 = Utils.prepareUrl(url);//url.replace("www.","").replace("http://","").replace("https://","");
            url2 = url2.split("/")[0];
            int idx1 = url2.lastIndexOf(".");
            if(idx1 < 0)
            {
                System.out.println(url);
            }else {
                String topDomain = url2.substring(idx1);
                topDomains.add(topDomain);
            }
        }

        String query = "пăхма çăмăл пÿрт урлă@188";
        String time = "1493133331000";

        String topDomainsStr = "{";
        for(String dd : topDomains)
        {
            topDomainsStr +="\""+dd +"\""+ " , ";
        }
        topDomainsStr += "}";

        int to_test = 5;
        for (int i =0; i< badUrls.size()-to_test; i+=to_test)
        {

            HashSet<String> currentUrlsSet = new HashSet<>();
            for (int j =0; j< to_test; j++)
            {
                currentUrlsSet.add(badUrls.get(i+j));
            }

            List<String> testUrlsToParsing1 = new ArrayList<>();
            List<String> testUrlsToParsing2 = new ArrayList<>();

            String data = query + "\t";
            for(int j = 0; j < to_test; j++)
            {
                data += badUrls.get(i+j) + ",";
                testUrlsToParsing1.add(badUrls.get(i+j));
            }
            data = data.substring(0, data.length() -1) + "\t";

            for(int j = 0; j < to_test; j++)
            {
                data += badUrls.get(i+j) + ",";
                testUrlsToParsing2.add(badUrls.get(i+j));
            }
            data = data.substring(0, data.length() -1) + "\t";

            for(int j = 0; j < to_test; j++)
            {
                data += time + ",";
            }
            data = data.substring(0, data.length() -1);

            for(String u: testUrlsToParsing1)
            {
                assertTrue(urls_map.containsKey(u));
            }

            QRecord record = new QRecord();
            record.parseString(data, urls_map);

            if(record.shownLinks.size() != to_test)
            {
                System.out.println(data);
            }

            assertTrue(record.shownLinks.size() == to_test);
            assertTrue(record.clickedLinks.size() == to_test);

            for(String url : record.shownLinks)
            {
                if(!currentUrlsSet.contains(url))
                {
                    System.out.println(url);
                }

                assertTrue(currentUrlsSet.contains(url));
                assertTrue(urls_map.containsKey(url));
            }


        }


//        String data = "пăхма çăмăл пÿрт урлă@188\thttps://A_,C,http://B_D,www.C.D\thttp://A_,C\t1493133331000";
//        QRecord record = new QRecord();
//        record.parseString(data);
//
//        System.out.println("aaa");
    }
}
