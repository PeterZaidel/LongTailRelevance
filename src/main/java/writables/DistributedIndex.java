package writables;

public class DistributedIndex {

    public static final String QUERY_TAG = "<QQ>";
    public static final String URL_TAG = "<UU>";
    //public static final String TAG_DELIMETER = "<TAG_DEL>";



    public static class QueryUrlIndex
    {

        public static final String DELIMETER = "\t";
        public String query;
        public String url;

        public QueryUrlIndex(){}

        public QueryUrlIndex(String query, String url)
        {
            this.query = query;
            this.url = url;
        }

        public String toString()
        {
            return QUERY_TAG + query + DELIMETER + URL_TAG  + url;
        }

        public String toStringNoTag()
        {
            return  query + DELIMETER + url;
        }

        public void parseString(String in)
        {
            String[] args = in.split(DELIMETER);
            query = args[0].replace(QUERY_TAG , "");
            url = args[1].replace(URL_TAG , "");
        }

        public static boolean checkParse(String in)
        {
            String[] args = in.split(DELIMETER);
            if(args.length == 2)
            {
                if(args[0].contains(QUERY_TAG) && args[1].contains(URL_TAG))
                    return true;
            }

            return false;
        }
    }

    public static class QueryIndex
    {

        public static final String DELIMETER = "\t";
        public String query;

        public QueryIndex(){}

        public QueryIndex(String query)
        {
            this.query = query;
        }

        public String toString()
        {
            return QUERY_TAG + query;
        }

        public String toStringNoTag()
        {
            return query;
        }

        public void parseString(String in)
        {
            query = in.replace(QUERY_TAG, "");
        }

        public static boolean checkParse(String in)
        {
            if(in.contains(DELIMETER))
            {
                return false;
            }
            else
            {
                return in.contains(QUERY_TAG) && !in.contains(URL_TAG);
            }
        }
    }

    public static class UrlIndex
    {

        public static final String DELIMETER = "\t";
        public String url;

        public UrlIndex(){}
        public UrlIndex(String url)
        {
            this.url = url;
        }

        public String toString()
        {
            return URL_TAG + url;
        }

        public String toStringNoTag()
        {
            return url;
        }

        public void parseString(String in)
        {
            url = in.replace(URL_TAG, "");
        }

        public static boolean checkParse(String in)
        {
            if(in.contains(DELIMETER))
            {
                return false;
            }
            else
            {
                return !in.contains(QUERY_TAG) && in.contains(URL_TAG);
            }
        }
    }
}
