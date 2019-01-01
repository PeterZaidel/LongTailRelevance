package colaborative;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import utils.Utils;
import writables.QRecord;

import java.util.*;

public class QueryFeatures {

    public static final double MIN_DISTANCE = 0.1;
    public static final double EPS = 0.001;

    private static final String DELIMETER = "\t";
    private static final String LIST_DELIMETER = "<:LD:>";
    public static int NGRAM = 3;

    public static double SHOW_COEFF = 1;
    public static double CLICK_COEFF = 5;
    public static double TEXT_COEFF = 1;

    public static double KNOWN_URLS_COUNT = 100;
    public static double KNOWN_GRAMS_COUNT = 100;

    public String query;
    public List<String> shownLinks = new ArrayList<>();
    public List<String> clickedLinks = new ArrayList<>();

    public QueryFeatures()
    {}

    public QueryFeatures(QRecord record)
    {
        query = record.query;

        HashSet<String> shownSet = new HashSet<>(record.shownLinks);
        HashSet<String> clickedSet = new HashSet<>(record.clickedLinks);

        shownLinks = new ArrayList<>(shownSet);
        clickedLinks = new ArrayList<>(clickedSet);
    }

    public String getKey()
    {
        return query;
    }

    public String getValue()
    {
        String res = "";
        res += Utils.listToString(shownLinks, LIST_DELIMETER) +DELIMETER;
        res += Utils.listToString(clickedLinks, LIST_DELIMETER);
        return res;
    }

    public void parseValueString(String valStr)
    {
        String[] args = valStr.split(DELIMETER);
        if(args.length == 1)
        {
            shownLinks.addAll(Arrays.asList(Utils.splitLinks(args[0], LIST_DELIMETER)));
        }

        if(args.length == 2) {
            shownLinks.addAll(Arrays.asList(Utils.splitLinks(args[0], LIST_DELIMETER)));
            clickedLinks.addAll(Arrays.asList(Utils.splitLinks(args[1], LIST_DELIMETER)));
        }

        HashSet<String> shownSet = new HashSet<>(shownLinks);
        HashSet<String> clickedSet = new HashSet<>(clickedLinks);

        shownLinks = new ArrayList<>(shownSet);
        clickedLinks = new ArrayList<>(clickedSet);
    }

    public void parseKeyString(String keyStr)
    {
        query = Utils.prepareQuery(keyStr);
    }


    public double dist(QueryFeatures q2)
    {
        return CalculateDistance(this, q2);
    }

    private static double cosineList(List<String> l1, List<String> l2)
    {
        double cosine = 0;
        double abs1 = l1.size();
        double abs2 = l2.size();
        HashMap<String, Integer> countShownUrls = new HashMap<>();
        for (String u1 : l1) {
            countShownUrls.put(u1, 0);
        }
        for (String u2 : l2) {
            if (countShownUrls.containsKey(u2)) {
                countShownUrls.put(u2, 1);
            }
        }
        for(Integer flag : countShownUrls.values())
        {
            cosine += flag;
        }
        cosine = cosine/(abs1 * abs2);
        return cosine;
    }

    public static List<String> generateNCharGrams(String in, int ngram)
    {
        in = in.trim().replaceAll(" +", " ");
        List<String> res = new ArrayList<>();
        for(int i =0; i< in.length(); i++)
        {
            if(i + ngram < in.length()) {
                res.add(in.substring(i, i + ngram));
            }else
            {
                res.add(in.substring(i));
            }
        }
        return res;
    }

    public static List<String> generateWords(String in)
    {
        return Arrays.asList(in.split("\\W+"));
    }

    public static double CalculateDistance(QueryFeatures q1, QueryFeatures q2)
    {
        double cosine_shown = cosineList(q1.shownLinks, q2.shownLinks);
        double cosine_click = cosineList(q1.clickedLinks, q2.clickedLinks);
        double cosine_text = cosineList(generateNCharGrams(q1.query, NGRAM),
                generateNCharGrams(q2.query, NGRAM) );

        double dist = SHOW_COEFF * cosine_shown + CLICK_COEFF*cosine_click ;//+ TEXT_COEFF*cosine_text;
        if(!Double.isFinite(dist))
        {
            return 0;
        }
        return dist;
    }

    public static double CalculateDistanceText(QueryFeatures q1, QueryFeatures q2)
    {
//        double cosine_shown = cosineList(q1.shownLinks, q2.shownLinks);
//        double cosine_click = cosineList(q1.clickedLinks, q2.clickedLinks);
        double cosine_text = cosineList(generateWords(q1.query),
                generateWords(q2.query) );

        double dist = TEXT_COEFF*cosine_text;
        if(!Double.isFinite(dist))
        {
            return 0;
        }
        return dist;
    }


}
