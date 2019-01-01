package writables;

import com.google.common.base.Splitter;
import org.apache.hadoop.io.Writable;
import utils.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;


public class QRecord implements Writable {
    private static String DELIMETER = "\t";
    private static String LIST_DELIMETER = ",";

    private static String QUERY_GEO_DELIMETER = "@";

    public String query = "";
    public int queryGeo = -1;
    public List<String> shownLinks = new ArrayList<>();
    public List<String> clickedLinks = new ArrayList<>();
    public List<Long> timestamps = new ArrayList<>();

    public QRecord(){}
    public QRecord(String query, int queryGeo, List<String> shownLinks, List<String> clickedLinks){
        this.query = query;
        this.queryGeo = queryGeo;
        this.shownLinks = shownLinks;
        this.clickedLinks = clickedLinks;
    }

    public QRecord(String query,int queryGeo, List<String> shownLinks, List<String> clickedLinks, List<Long> timestamps){
        this.query = query;
        this.queryGeo = queryGeo;
        this.shownLinks = shownLinks;
        this.clickedLinks = clickedLinks;
        this.timestamps = timestamps;
    }

    public QRecord(String data_string)
    {
        this.parseString(data_string);
    }



    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(query);
        out.writeInt(queryGeo);

        out.writeInt(shownLinks.size());
        for (int i =0; i< shownLinks.size(); i++)
        {
            out.writeUTF(shownLinks.get(i));
        }

        out.writeInt(clickedLinks.size());
        for (int i =0; i< clickedLinks.size(); i++)
        {
            out.writeUTF(clickedLinks.get(i));
        }

        out.writeInt(timestamps.size());
        for (int i =0; i< timestamps.size(); i++)
        {
            out.writeLong(timestamps.get(i));
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        query = in.readUTF();
        queryGeo = in.readInt();

        shownLinks = new ArrayList<>();
        int shownLinksSize =  in.readInt();
        for (int i =0; i< shownLinksSize; i++)
        {
            shownLinks.add(in.readUTF());
        }

        clickedLinks = new ArrayList<>();
        int clickedLinksSize =  in.readInt();
        for (int i =0; i< clickedLinksSize; i++)
        {
            clickedLinks.add(in.readUTF());
        }

        timestamps = new ArrayList<>();
        int timestampsSize =  in.readInt();
        for (int i =0; i< timestampsSize; i++)
        {
            timestamps.add(in.readLong());
        }
    }

    //private static final Pattern urlPattern = Pattern.compile("(http://|https://)?(www.)");/* Returns true if url is valid */




    public void parseString(String in)
    {
        parseString(in, null);
//        int idx = 0;
//        String[] args = in.split(DELIMETER);
//
//        String[] query_args = args[idx++].split(QUERY_GEO_DELIMETER);
//
//        query = Utils.prepareQuery(query_args[0]);
//        queryGeo = Integer.parseInt(query_args[1]);
//
//        shownLinks = new ArrayList<>();
//        String[] shownLinks_args = Utils.splitLinks(args[idx++], LIST_DELIMETER);//.split(LIST_DELIMETER);
//        for (String url : shownLinks_args)
//        {
//            shownLinks.add(Utils.prepareUrl(url));
//        }
//
//        clickedLinks = new ArrayList<>();
//        String[] clickedLinks_args = Utils.splitLinks(args[idx++], LIST_DELIMETER);//.split(LIST_DELIMETER);
//        for (String url : clickedLinks_args)
//        {
//            clickedLinks.add(Utils.prepareUrl(url));
//        }
//
//        timestamps = new ArrayList<>();
//        String[] timestamps_args = args[idx++].split(LIST_DELIMETER);
//        for (String timestamp : timestamps_args)
//        {
//            timestamps.add(Long.parseLong(timestamp));
//        }
//
//        query = Utils.prepareQuery(query);
    }

    public void parseString(String in, HashMap<String, Integer> urls_filter)
    {
        int idx = 0;
        String[] args = in.split(DELIMETER);

        String[] query_args = args[idx++].split(QUERY_GEO_DELIMETER);

        query = Utils.prepareQuery(query_args[0]);
        queryGeo = Integer.parseInt(query_args[1]);

        shownLinks = new ArrayList<>();
        String[] shownLinks_args = Utils.splitLinks(args[1], LIST_DELIMETER);//args[idx++].split(LIST_DELIMETER);
        idx++;
        for (String url : shownLinks_args)
        {
            url = Utils.prepareUrl(url);
            if(urls_filter == null || (urls_filter!= null && urls_filter.containsKey(url))) {
                shownLinks.add(url);
            }
        }

        clickedLinks = new ArrayList<>();
        String[] clickedLinks_args = Utils.splitLinks(args[idx++], LIST_DELIMETER);//args[idx++].split(LIST_DELIMETER);
        for (String url : clickedLinks_args)
        {
            url = Utils.prepareUrl(url);
            if(urls_filter == null || (urls_filter!= null && urls_filter.containsKey(url))) {
                clickedLinks.add(url);
            }
        }

        timestamps = new ArrayList<>();
        String[] timestamps_args = args[idx++].split(LIST_DELIMETER);
        for (String timestamp : timestamps_args)
        {
            timestamps.add(Long.parseLong(timestamp));
        }

        query = Utils.prepareQuery(query);
    }


    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(query + QUERY_GEO_DELIMETER + Long.toString(queryGeo));
        stringBuilder.append(DELIMETER);

        for(int i =0; i< shownLinks.size(); i++)
        {
            if(i<shownLinks.size()-1)
                stringBuilder.append(shownLinks.get(i) + LIST_DELIMETER);
            else
                stringBuilder.append(shownLinks.get(i));
        }

        for(int i =0; i< clickedLinks.size(); i++)
        {
            if(i<clickedLinks.size()-1)
                stringBuilder.append(clickedLinks.get(i) + LIST_DELIMETER);
            else
                stringBuilder.append(clickedLinks.get(i));
        }

        for(int i =0; i< timestamps.size(); i++)
        {
            if(i<timestamps.size()-1)
                stringBuilder.append(Long.toString(timestamps.get(i)) + LIST_DELIMETER);
            else
                stringBuilder.append(Long.toString(timestamps.get(i)));
        }

        return stringBuilder.toString();
    }
}
