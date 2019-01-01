package features;

import jobs.FeaturesJob;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import writables.DistributedIndex;
import writables.DistributedParameter;
import writables.QRecord;
import writables.ReduceResult;

import java.io.IOException;
import java.util.*;
import java.util.function.DoubleUnaryOperator;

public class SDBNFeatureExtractor implements FeatureExtractor {

    public static final String FIELD_DELIMETER = ":";
    public static final String LIST_DELIMETER = ",";
    public static final String DELIMETER = "\t";

    public static class SDBNParameters
    {
        public static final String ALG_NAME = "SDBN";

        public static final String A_N = "A_N";
        public static final String A_D = "A_D";
        public static final String S_N = "S_N";
        public static final String S_D = "S_D";

        public static double ALPHA_A = 0.1;
        public static double ALPHA_S = 0.1;
        public static double BETA_A = 0.1;
        public static double BETA_S = 0.1;

        public double aN = 0;
        public double aD = 0;
        public double sN = 0;
        public double sD = 0;

    }

    SDBNParameters parameters = new SDBNParameters();
    private HashMap<String, Integer> queries_map = null;
    private HashMap<String, Integer> urls_map = null;

    public boolean FILTER_ZERO_VALS = false;

    @Override
    public void setQueriesMap(HashMap<String, Integer> queriesMap) {
        this.queries_map = queriesMap;
    }

    @Override
    public void setUrlsMap(HashMap<String, Integer> urlsMap) {
        this.urls_map = urlsMap;
    }

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        QRecord record = new QRecord();
        record.parseString(value.toString(), urls_map);

        if(record.clickedLinks.size() == 0)
            return;

        if(queries_map != null && !queries_map.containsKey(record.query))
            return;

        String satisfaction_url = record.clickedLinks.get(record.clickedLinks.size()-1);

        Set<String> clicked_set = new HashSet<>();
        clicked_set.addAll(record.clickedLinks);

        String last_clicked_url = record.clickedLinks.get(0);
        for(String url : record.shownLinks)
        {
            if(clicked_set.contains(url))
            {
                last_clicked_url = url;
            }
        }

        //S_N parameter update

        {
            DistributedParameter param = new DistributedParameter();
            param.paramName = SDBNParameters.S_N;
            param.paramGroup = SDBNParameters.ALG_NAME;

            DistributedIndex.QueryUrlIndex qu_index = new DistributedIndex.QueryUrlIndex(record.query,
                    satisfaction_url);
            param.paramIndex = qu_index.toString();
            param.paramValue = Integer.toString(1);

            context.write(param.extractKey(), param.extractValue());
        }

        for(String url : record.shownLinks)
        {
            DistributedParameter param = new DistributedParameter();
            param.paramName = SDBNParameters.A_D;
            param.paramGroup = SDBNParameters.ALG_NAME;

            DistributedIndex.QueryUrlIndex qu_index = new DistributedIndex.QueryUrlIndex(record.query, url);
            param.paramIndex = qu_index.toString();
            param.paramValue = Integer.toString(1);

            context.write(param.extractKey(), param.extractValue());


            if(url.equals(last_clicked_url))
            {
                break;
            }
        }

        for(String url : record.clickedLinks)
        {
            DistributedIndex.QueryUrlIndex qu_index = new DistributedIndex.QueryUrlIndex(record.query, url);

            // A_N param update
            {
                DistributedParameter param_an = new DistributedParameter();
                param_an.paramName = SDBNParameters.A_N;
                param_an.paramGroup = SDBNParameters.ALG_NAME;
                param_an.paramIndex = qu_index.toString();

                param_an.paramValue = Integer.toString(1);
                context.write(param_an.extractKey(), param_an.extractValue());
            }

            // S_D param update
            {
                DistributedParameter param_sd = new DistributedParameter();
                param_sd.paramName = SDBNParameters.S_D;
                param_sd.paramGroup = SDBNParameters.ALG_NAME;
                param_sd.paramIndex = qu_index.toString();

                param_sd.paramValue = Integer.toString(1);
                context.write(param_sd.extractKey(), param_sd.extractValue());
            }

        }
    }

    @Override
    public boolean filter_reduce(Text key, Text value) {
        DistributedParameter param = new DistributedParameter();
        param.parseKeyValue(key.toString(), value.toString());

        return param.paramGroup.equals(SDBNParameters.ALG_NAME);
    }

    @Override
    public void combiner_update(Text key, Text value, Reducer.Context context) {
        reduce_update(key, value, context);
    }

    @Override
    public void combiner_final(Text key, Reducer.Context context) throws IOException, InterruptedException {
        {
            if(parameters.sN > 0 || !FILTER_ZERO_VALS) {
                DistributedParameter param = new DistributedParameter();
                param.paramName = SDBNParameters.S_N;
                param.paramGroup = SDBNParameters.ALG_NAME;

                param.paramValue = Double.toString(parameters.sN);

                context.write(key, param.extractValue());
            }
        }

        {
            if(parameters.sD> 0 || !FILTER_ZERO_VALS) {
                DistributedParameter param = new DistributedParameter();
                param.paramName = SDBNParameters.S_D;
                param.paramGroup = SDBNParameters.ALG_NAME;

                param.paramValue = Double.toString(parameters.sD);

                context.write(key, param.extractValue());
            }
        }

        {
            if(parameters.aD > 0 || !FILTER_ZERO_VALS) {
                DistributedParameter param = new DistributedParameter();
                param.paramName = SDBNParameters.A_D;
                param.paramGroup = SDBNParameters.ALG_NAME;

                param.paramValue = Double.toString(parameters.aD);

                context.write(key, param.extractValue());
            }
        }

        {
            if(parameters.aN > 0 || !FILTER_ZERO_VALS) {
                DistributedParameter param = new DistributedParameter();
                param.paramName = SDBNParameters.A_N;
                param.paramGroup = SDBNParameters.ALG_NAME;

                param.paramValue = Double.toString(parameters.aN);

                context.write(key, param.extractValue());
            }
        }

    }

    @Override
    public void reduce_update(Text key, Text value, Reducer.Context context) {
        DistributedParameter param = new DistributedParameter();
        param.parseKey(key.toString());
        param.parseValue(value.toString());

        double val = Double.parseDouble(param.paramValue);
        switch (param.paramName)
        {
            case SDBNParameters.A_D:
                parameters.aD += val;
                break;
            case SDBNParameters.A_N:
                parameters.aN += val;
                break;

            case SDBNParameters.S_N:
                parameters.sN += val;
                break;
            case SDBNParameters.S_D:
                parameters.sD += val;
                break;
        }
    }

    private String doubleToStr(double v)
    {
        if(!Double.isFinite(v))
        {
            return null;
        }

        return Double.toString(v);
    }

    @Override
    public ReduceResult reduce_final(Text key, Reducer.Context context) throws IOException, InterruptedException {

        if(!DistributedIndex.QueryUrlIndex.checkParse(key.toString()))
        {
            return null;
        }

        DistributedIndex.QueryUrlIndex index = new DistributedIndex.QueryUrlIndex();
        index.parseString(key.toString());

        double A_U = (parameters.aN + SDBNParameters.ALPHA_A)/(parameters.aD + SDBNParameters.ALPHA_A + SDBNParameters.BETA_A);
        double S_U = (parameters.sN + SDBNParameters.ALPHA_S)/(parameters.sD + SDBNParameters.ALPHA_S + SDBNParameters.BETA_S);
        double R_U = A_U * S_U;

//        String value = "";
//        value += Double.toString(A_U)
//                + DELIMETER + Double.toString(S_U)
//                + DELIMETER + Double.toString(R_U);


        String[] outValues = {doubleToStr(parameters.aN), doubleToStr(parameters.aD),
                doubleToStr(parameters.sN), doubleToStr(parameters.sD),
                doubleToStr(A_U),
                doubleToStr(S_U),
                doubleToStr(R_U)};

        String[] outFeatureNames = {"aN", "aD", "sN", "sD", "AU", "SU", "RU"};


        List<ReduceResult.IdxFeaturePair> outPairs = ReduceResult.toIdxFeaturePairs(
                Arrays.asList(outValues),
                Arrays.asList(outFeatureNames));



        ReduceResult res = new ReduceResult(FeaturesJob.QD_GROUP_NAME,
                index.toStringNoTag(), outPairs);

        if(parameters.aN == 0 && parameters.sN == 0)
        {
            res.isNotZero = false;
        }

        return res;
    }
}
