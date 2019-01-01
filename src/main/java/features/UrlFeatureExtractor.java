package features;

import jobs.FeaturesJob;
import org.apache.commons.lang.mutable.MutableBoolean;
import writables.DistributedIndex;
import writables.DistributedParameter;
import writables.QRecord;
import writables.ReduceResult;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class UrlFeatureExtractor implements FeatureExtractor {


    public static final String FIELD_DELIMETER = ":";
    public static final String LIST_DELIMETER = ",";
    public static final String DELIMETER = "\t";

    public static class UrlFeatureParameters
    {
        public static final String ALG_NAME = "URL_FEATURE";

        public static final String CLICK_COUNT = "CLICK_COUNT";
        public int click_count = 0;

        public static final String SHOW_COUNT = "SHOW_COUNT";
        public int show_count = 0;

//        public static final String MEAN_CLICK_TIME = "MEAN_CLICK_TIME";
//        public double mean_click_time = 0;

        public static final String MEAN_CLICK_POS = "MEAN_CLICK_POS";
        public int mean_click_pos = 0;

        public static final String MEAN_SHOW_POS = "MEAN_SHOW_POS";
        public int mean_show_pos = 0;


    }

    UrlFeatureParameters parameters = new UrlFeatureParameters();
    private HashMap<String, Integer> queries_map = null;
    private HashMap<String, Integer> urls_map = null;


    private void map_count(QRecord record, Mapper.Context context) throws IOException, InterruptedException {
        for(String url: record.shownLinks)
        {
            if(urls_map != null && !urls_map.containsKey(url))
                continue;

            DistributedParameter param = new DistributedParameter();
            param.paramName = UrlFeatureParameters.SHOW_COUNT;
            param.paramGroup = UrlFeatureParameters.ALG_NAME;

            DistributedIndex.UrlIndex index = new DistributedIndex.UrlIndex(url);
            param.paramIndex = index.toString();
            param.paramValue = Integer.toString(1);

            context.write(param.extractKey(), param.extractValue());
        }

        for(String url: record.clickedLinks)
        {

            if(urls_map != null && !urls_map.containsKey(url))
                continue;

            DistributedParameter param = new DistributedParameter();
            param.paramName = UrlFeatureParameters.CLICK_COUNT;
            param.paramGroup = UrlFeatureParameters.ALG_NAME;

            DistributedIndex.UrlIndex index = new DistributedIndex.UrlIndex(url);
            param.paramIndex = index.toString();
            param.paramValue = Integer.toString(1);

            context.write(param.extractKey(), param.extractValue());
        }
    }

    private void map_mean_click_pos(QRecord record, Mapper.Context context) throws IOException, InterruptedException {
        HashSet<String> clickedUrls = new HashSet<>(record.clickedLinks);

        int pos = 0;
        for(String url : record.shownLinks)
        {
            if(urls_map != null && !urls_map.containsKey(url))
                continue;

            if(clickedUrls.contains(url))
            {
                DistributedParameter param = new DistributedParameter();
                param.paramName = UrlFeatureParameters.MEAN_CLICK_POS;
                param.paramGroup = UrlFeatureParameters.ALG_NAME;

                DistributedIndex.UrlIndex index = new DistributedIndex.UrlIndex(url);
                param.paramIndex = index.toString();
                param.paramValue = Integer.toString(pos);

                context.write(param.extractKey(), param.extractValue());
            }
            pos++;
        }
    }

    private void map_mean_show_pos(QRecord record, Mapper.Context context) throws IOException, InterruptedException {

        int pos = 0;
        for(String url : record.shownLinks)
        {
            if(urls_map != null && !urls_map.containsKey(url))
                continue;


            DistributedParameter param = new DistributedParameter();
            param.paramName = UrlFeatureParameters.MEAN_SHOW_POS;
            param.paramGroup = UrlFeatureParameters.ALG_NAME;

            DistributedIndex.UrlIndex index = new DistributedIndex.UrlIndex(url);
            param.paramIndex = index.toString();
            param.paramValue = Integer.toString(pos);

            context.write(param.extractKey(), param.extractValue());

            pos++;
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

        map_count(record, context);
        map_mean_click_pos(record, context);
        map_mean_show_pos(record, context);
    }

    @Override
    public boolean filter_reduce(Text key, Text value) {
        DistributedParameter param = new DistributedParameter();
        param.parseKeyValue(key.toString(), value.toString());

        return param.paramGroup.equals(UrlFeatureParameters.ALG_NAME);
    }

    @Override
    public void combiner_update(Text key, Text value, Reducer.Context context) {

    }

    @Override
    public void combiner_final(Text key, Reducer.Context context) throws IOException, InterruptedException {

    }


    private void reducer_update_count(DistributedParameter param)
    {
        if(param.paramName.equals(UrlFeatureParameters.CLICK_COUNT))
        {
            parameters.click_count += Integer.parseInt(param.paramValue);
        }

        if(param.paramName.equals(UrlFeatureParameters.SHOW_COUNT))
        {
            parameters.show_count += Integer.parseInt(param.paramValue);
        }
    }

    private void reducer_update_mean_pos(DistributedParameter param)
    {
        if(param.paramName.equals(UrlFeatureParameters.MEAN_CLICK_POS))
        {
            parameters.mean_click_pos += Integer.parseInt(param.paramValue);
        }

        if(param.paramName.equals(UrlFeatureParameters.MEAN_SHOW_POS))
        {
            parameters.mean_show_pos += Integer.parseInt(param.paramValue);
        }
    }



    @Override
    public void reduce_update(Text key, Text value, Reducer.Context context) {
        DistributedParameter param = new DistributedParameter();
        param.parseKeyValue(key.toString(), value.toString());


        reducer_update_count(param);
        reducer_update_mean_pos(param);
    }


    private String reducer_final_count_shows(MutableBoolean isNotZero)
    {
        return Integer.toString(parameters.show_count);
    }

    private String reducer_final_count_clicks(MutableBoolean isNotZero)
    {
        return Integer.toString(parameters.click_count);
    }

    private String reducer_final_mean_click_pos(MutableBoolean isNotZero)
    {
        int click_count = parameters.click_count;
        if(click_count == 0)
        {
            isNotZero.setValue(isNotZero.booleanValue() || false);
            return null;
        }

        return Double.toString(parameters.mean_click_pos/ (double)click_count);
    }

    private String reducer_final_mean_show_pos(MutableBoolean isNotZero)
    {
        int show_count = parameters.show_count;
        if(show_count == 0)
        {
            isNotZero.setValue(isNotZero.booleanValue() || false);
            return null;
        }

        return Double.toString(parameters.mean_show_pos/ (double)show_count);
    }



    @Override
    public ReduceResult reduce_final(Text key, Reducer.Context context) throws IOException, InterruptedException {
        if(!DistributedIndex.UrlIndex.checkParse(key.toString()))
        {
            return null;
        }

        DistributedIndex.UrlIndex index = new DistributedIndex.UrlIndex();
        index.parseString(key.toString());

        MutableBoolean isNotZero = new MutableBoolean(false);

        String click_count = reducer_final_count_clicks(isNotZero);
        String shows_count = reducer_final_count_shows(isNotZero);
        String mean_click_pos = reducer_final_mean_click_pos(isNotZero);
        String mean_show_pos = reducer_final_mean_show_pos(isNotZero);

        String[] outValues = {click_count, shows_count, mean_click_pos, mean_show_pos};
        String[] outFeaturesNames = {"click_count", "shows_count", "mean_click_pos", "mean_show_pos"};
        List<ReduceResult.IdxFeaturePair> outPairs = ReduceResult.toIdxFeaturePairs(
                Arrays.asList(outValues),
                Arrays.asList(outFeaturesNames));

//        String value = mean_click + DELIMETER + mean_show_click;

        ReduceResult res = new ReduceResult(FeaturesJob.DOC_GROUP_NAME,
                index.toStringNoTag(), outPairs);

        res.isNotZero = isNotZero.isTrue();

        return res;
    }
}
