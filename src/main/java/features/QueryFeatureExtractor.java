package features;

import jobs.FeaturesJob;
import writables.DistributedIndex;
import writables.DistributedParameter;
import writables.QRecord;
import writables.ReduceResult;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class QueryFeatureExtractor implements FeatureExtractor {


    public static final String FIELD_DELIMETER = ":";
    public static final String LIST_DELIMETER = ",";
    public static final String DELIMETER = "\t";

    public static class QueryFeatureParameters
    {
        public static final String ALG_NAME = "QUERY_FEATURE";

        public static final String SESSIONS_COUNT = "SESSIONS_COUNT";
        public long sessions_count = 0;

        public static final String MEAN_CLICK_SUM = "MEAN_CLICK_SUM";
        public long mean_click_sum = 0;

        public static final String MEAN_SHOW_CLICK_SUM = "MEAN_SHOW_CLICK_SUM";
        public long mean_show_click_sum = 0;
//
//        public static final String MEAN_TIME = "MEAN_TIME";
//        public long mean_time = 0;

        public static final String MEAN_FIRST_POS = "MEAN_FIRST_POS";
        public double mean_first_pos = 0;

        public static final String MEAN_LAST_POS = "MEAN_LAST_POS";
        public double mean_last_pos = 0;


    }

    QueryFeatureParameters parameters = new QueryFeatureParameters();
    private HashMap<String, Integer> queries_map = null;
    private HashMap<String, Integer> urls_map = null;

    private void map_sessions_count(QRecord record, Mapper.Context context) throws IOException, InterruptedException {
        DistributedParameter param = new DistributedParameter();
        param.paramName = QueryFeatureParameters.SESSIONS_COUNT;
        param.paramGroup = QueryFeatureParameters.ALG_NAME;

        DistributedIndex.QueryIndex q_index = new DistributedIndex.QueryIndex(record.query);
        param.paramIndex = q_index.toString();
        param.paramValue = Integer.toString(1);

        context.write(param.extractKey(), param.extractValue());
    }

    private void map_mean_click(QRecord record, Mapper.Context context) throws IOException, InterruptedException {
        int clicks_count = record.clickedLinks.size();
        DistributedParameter param = new DistributedParameter();
        param.paramName = QueryFeatureParameters.MEAN_CLICK_SUM;
        param.paramGroup = QueryFeatureParameters.ALG_NAME;

        DistributedIndex.QueryIndex q_index = new DistributedIndex.QueryIndex(record.query);
        param.paramIndex = q_index.toString();
        param.paramValue = Integer.toString(clicks_count);

        context.write(param.extractKey(), param.extractValue());
    }

    private void map_show_click(QRecord record, Mapper.Context context) throws IOException, InterruptedException {
        int clicks_count = record.clickedLinks.size();
        int show_count = record.shownLinks.size();

        if(show_count == 0)
            return;

        double show_click = clicks_count/show_count;

        DistributedParameter param = new DistributedParameter();
        param.paramName = QueryFeatureParameters.MEAN_SHOW_CLICK_SUM;
        param.paramGroup = QueryFeatureParameters.ALG_NAME;

        DistributedIndex.QueryIndex q_index = new DistributedIndex.QueryIndex(record.query);
        param.paramIndex = q_index.toString();
        param.paramValue = Double.toString(show_click);

        context.write(param.extractKey(), param.extractValue());
    }

    private void map_mean_pos(QRecord record, Mapper.Context context) throws IOException, InterruptedException {
        int first_pos = -1;
        int last_pos = -1;
        if(record.clickedLinks.size() == 0)
            return;

        String firstClickUrl = record.clickedLinks.get(0);
        String lastClickUrl = record.clickedLinks.get(record.clickedLinks.size()-1);

        int iRank = 0;
        for(String s : record.shownLinks)
        {
            if(s.equals(firstClickUrl) && first_pos == -1)
            {
                first_pos =iRank;
            }
            if(s.equals(lastClickUrl) && last_pos == -1)
            {
                last_pos = iRank;
            }

            iRank++;
        }

        if(first_pos >= 0) {
            DistributedParameter param = new DistributedParameter();
            param.paramName = QueryFeatureParameters.MEAN_FIRST_POS;
            param.paramGroup = QueryFeatureParameters.ALG_NAME;

            DistributedIndex.QueryIndex q_index = new DistributedIndex.QueryIndex(record.query);
            param.paramIndex = q_index.toString();
            param.paramValue = Double.toString((double) first_pos);

            context.write(param.extractKey(), param.extractValue());
        }

        if(last_pos >= 0)
        {
            DistributedParameter param = new DistributedParameter();
            param.paramName = QueryFeatureParameters.MEAN_LAST_POS;
            param.paramGroup = QueryFeatureParameters.ALG_NAME;

            DistributedIndex.QueryIndex q_index = new DistributedIndex.QueryIndex(record.query);
            param.paramIndex = q_index.toString();
            param.paramValue = Double.toString((double) last_pos);

            context.write(param.extractKey(), param.extractValue());
        }
    }

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
        record.parseString(value.toString());

        if(queries_map != null && !queries_map.containsKey(record.query))
            return;

        map_mean_click(record, context);
        map_show_click(record, context);
        map_sessions_count(record, context);
        map_mean_pos(record, context);
    }

    @Override
    public boolean filter_reduce(Text key, Text value) {
        DistributedParameter param = new DistributedParameter();
        param.parseKeyValue(key.toString(), value.toString());

        return param.paramGroup.equals(QueryFeatureParameters.ALG_NAME);
    }

    @Override
    public void combiner_update(Text key, Text value, Reducer.Context context) {

    }

    @Override
    public void combiner_final(Text key, Reducer.Context context) throws IOException, InterruptedException {

    }


    private void reduce_update_sessions_count(DistributedParameter param)
    {
        if(param.paramName.equals(QueryFeatureParameters.SESSIONS_COUNT))
        {
            parameters.sessions_count += 1;
        }
    }

    private void reduce_update_mean_click(DistributedParameter param)
    {
        if(param.paramName.equals(QueryFeatureParameters.MEAN_CLICK_SUM))
        {
            parameters.mean_click_sum += Integer.parseInt(param.paramValue);
        }
    }

    private void reduce_update_show_click(DistributedParameter param)
    {
        if(param.paramName.equals(QueryFeatureParameters.MEAN_SHOW_CLICK_SUM))
        {
            parameters.mean_show_click_sum += Double.parseDouble(param.paramValue);
        }
    }

    private void reduce_update_mean_pos(DistributedParameter param)
    {
        if(param.paramName.equals(QueryFeatureParameters.MEAN_FIRST_POS))
        {
            parameters.mean_first_pos += Double.parseDouble(param.paramValue);
        }

        if(param.paramName.equals(QueryFeatureParameters.MEAN_LAST_POS))
        {
            parameters.mean_last_pos += Double.parseDouble(param.paramValue);
        }
    }

    @Override
    public void reduce_update(Text key, Text value, Reducer.Context context) {
        DistributedParameter param = new DistributedParameter();
        param.parseKeyValue(key.toString(), value.toString());

        if(!param.paramGroup.equals(QueryFeatureParameters.ALG_NAME))
            return;

        reduce_update_sessions_count(param);
        reduce_update_mean_click(param);
        reduce_update_show_click(param);
        reduce_update_mean_pos(param);
    }


    private String reduce_final_mean_click(MutableBoolean isNotZero)
    {
        double mean_click = (double) parameters.mean_click_sum/ (double) parameters.sessions_count;
        if(!Double.isFinite(mean_click))
        {
            isNotZero.setValue(isNotZero.booleanValue() || false);
            return null;
        }

        if(mean_click == 0)
        {
            isNotZero.setValue(isNotZero.booleanValue() || false);
        }

        return Double.toString(mean_click);
    }

    private String reduce_final_show_click(MutableBoolean isNotZero)
    {
        double mean_show_click = (double) parameters.mean_show_click_sum/ (double) parameters.sessions_count;



        if(!Double.isFinite(mean_show_click))
        {
            isNotZero.setValue(isNotZero.booleanValue() || false);
            return null;
        }

        if(mean_show_click == 0)
        {
            isNotZero.setValue(isNotZero.booleanValue() || false);
        }

        return Double.toString(mean_show_click);
    }

    private String reduce_final_mean_last_pos(MutableBoolean isNotZero)
    {
        double mean_last_pos = parameters.mean_last_pos/ (double) parameters.sessions_count;

        if(!Double.isFinite(mean_last_pos))
        {
            mean_last_pos = -1;
        }

        if(mean_last_pos == -1)
        {
            isNotZero.setValue(isNotZero.booleanValue() || false);
            return null;
        }

        return Double.toString(mean_last_pos);
    }

    private String reduce_final_mean_first_pos(MutableBoolean isNotZero)
    {
        double mean_fisrt_pos = parameters.mean_first_pos/ (double) parameters.sessions_count;


        if(!Double.isFinite(mean_fisrt_pos))
        {
            mean_fisrt_pos = -1;
        }

        if(mean_fisrt_pos == -1)
        {
            isNotZero.setValue(isNotZero.booleanValue() || false);
            return null;
        }

        return Double.toString(mean_fisrt_pos);

    }


    @Override
    public ReduceResult reduce_final(Text key, Reducer.Context context) throws IOException, InterruptedException {
        if(!DistributedIndex.QueryIndex.checkParse(key.toString()))
        {
            return null;
        }

        DistributedIndex.QueryIndex index = new DistributedIndex.QueryIndex();
        index.parseString(key.toString());

        MutableBoolean isNotZero = new MutableBoolean(false);

        String mean_click = reduce_final_mean_click(isNotZero);
        String mean_show_click = reduce_final_show_click(isNotZero);
        String mean_first_pos = reduce_final_mean_first_pos(isNotZero);
        String mean_last_pos = reduce_final_mean_last_pos(isNotZero);

        String[] outValues = {mean_click, mean_show_click, mean_first_pos, mean_last_pos};
        String[] outFeatureNames = {"mean_click", "mean_show_click",
                "mean_first_pos", "mean_last_pos"};
        List<ReduceResult.IdxFeaturePair> outPairs = ReduceResult.toIdxFeaturePairs(
                Arrays.asList(outValues),
                Arrays.asList(outFeatureNames));

//        String value = mean_click + DELIMETER + mean_show_click;

        ReduceResult res = new ReduceResult(FeaturesJob.QUERY_GROUP_NAME,
                index.toStringNoTag(), outPairs);

        res.isNotZero = isNotZero.isTrue();

        return res;
    }
}
