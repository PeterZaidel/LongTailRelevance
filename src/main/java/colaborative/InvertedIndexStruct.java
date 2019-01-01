package colaborative;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class InvertedIndexStruct {
    public enum TextTokenType
    {
        NGRAM,
        WORDS,
        BIGRAMS,
        NONE
    }

    public static final String SHOWED_LINKS_GROUP = "SHOWLINK";
    public static final String CLICKED_LINKS_GROUP = "CLICKLINK";
    public static final String TEXT_GROUP = "TEXT";


    private HashMap<String, HashMap<String, List<QueryFeatures> >> groupIndex = new HashMap<>();
    public InvertedIndexStruct()
    {
        groupIndex.put(SHOWED_LINKS_GROUP, new HashMap<>());
        groupIndex.put(CLICKED_LINKS_GROUP, new HashMap<>());
        groupIndex.put(TEXT_GROUP, new HashMap<>());
    }

    public QueryFeatures get(String group, String key)
    {
        return null;
    }


    private void addToList(String group, String key, QueryFeatures qf)
    {
        if(!groupIndex.get(group).containsKey(key)) {
            groupIndex.get(group).put(key, new ArrayList<>());
        }

        groupIndex.get(group).get(key).add(qf);
    }



    public void add(QueryFeatures qf, TextTokenType tokenType, int ngram)
    {
        for(String url: qf.shownLinks)
        {
           addToList(SHOWED_LINKS_GROUP, url, qf);
        }

        for(String url: qf.clickedLinks)
        {
            addToList(CLICKED_LINKS_GROUP, url, qf);
        }

        if(tokenType == TextTokenType.WORDS)
        {
            List<String> words = QueryFeatures.generateWords(qf.query);
            for(String w: words)
            {
                addToList(TEXT_GROUP, w, qf);
            }
        }

        if(tokenType == TextTokenType.NGRAM)
        {
            List<String> words = QueryFeatures.generateNCharGrams(qf.query, ngram);
            for(String w: words)
            {
                addToList(TEXT_GROUP, w, qf);
            }
        }



    }
}
