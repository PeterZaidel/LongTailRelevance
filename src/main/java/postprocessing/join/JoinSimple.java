package postprocessing.join;

import utils.Utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class JoinSimple {




    public static void main(String[] args) throws Exception
    {
        int idx = 0;
        List<String> qd_feature_files = new ArrayList<>();
        List<String> query_feature_files = new ArrayList<>();
        List<String> url_feature_files = new ArrayList<>();

        String output_file = "";

        for(String s : args)
        {
            String[] ss = s.split("=");
            String tag = ss[0];
            String val = ss[1];

            if(tag.equals("out"))
            {
                output_file = val;
            }

            if(tag.equals("qd"))
            {
                qd_feature_files.add(val);
            }

            if(tag.equals("q"))
            {
                query_feature_files.add(val);
            }

            if(tag.equals("url"))
            {
                url_feature_files.add(val);
            }
        }


        String input_file = args[0];
        String pairs_file = args[1];

        output_file = args[2];
        Utils.deleteDirectory(new File(output_file));

        String queries_filename = args[3];
        String urls_filename = args[4];

        String query_features_dir = args[5];
        String url_features_dir = args[6];
    }
}

///media/peter/DATA1/Data/InfoSearch/hw5/FULL/features2/QD/*
///media/peter/DATA1/Data/InfoSearch/hw5/data/test.txt
///media/peter/DATA1/Data/InfoSearch/hw5/xgb/xgb_test/
///media/peter/DATA1/Data/InfoSearch/hw5/data/queries.tsv
///media/peter/DATA1/Data/InfoSearch/hw5/data/url.data
///media/peter/DATA1/Data/InfoSearch/hw5/FULL/features2/QUERY/
///media/peter/DATA1/Data/InfoSearch/hw5/FULL/features2/URL/