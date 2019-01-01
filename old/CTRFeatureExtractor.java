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

public class CTRFeatureExtractor implements FeatureExtractor {

    public static final String FIELD_DELIMETER = ":";
    public static final String LIST_DELIMETER = ",";
    public static final String DELIMETER = "\t";

    private class CTRParameters
    {
        public static final String ALG_NAME = "CTR";


        public static final String QD_SHOW = "QD_SHOW";
        public static final String QD_CLICK = "QD_CLICK";

        public static final String GLOBAL_SHOW = "GLOBAL_SHOW";
        public static final String GLOBAL_CLICK = "GLOBAL_CLICK";

        public static final String RANK_SHOW = "RANK_SHOW";
        public static final String RANK_CLICK = "RANK_CLICK";

        public static final int MAX_RANK = 10;


        public long qd_shows = 0;
        public long qd_clicks = 0;

        public long global_shows = 0;
        public long global_clicks = 0;

        public long[] rank_clicks = new long[MAX_RANK];
        public long[] rank_shows = new long[MAX_RANK];

    }

    private CTRParameters parameters = new CTRParameters();

    public boolean FILTER_ZERO_VALS = false;

    private void map_qdoc(QRecord record, Mapper.Context context) throws IOException, InterruptedException
    {
        for(String url : record.shownLinks)
        {
            DistributedParameter param = new DistributedParameter();
            param.paramName = CTRParameters.QD_SHOW;
            param.paramGroup = CTRParameters.ALG_NAME;

            DistributedIndex.QueryUrlIndex index = new DistributedIndex.QueryUrlIndex(record.query,
                    url);
            param.paramIndex = index.toString();
            param.paramValue = Integer.toString(1);

            context.write(param.extractKey(), param.extractValue());
        }

        for(String url : record.clickedLinks)
        {

            DistributedParameter param = new DistributedParameter();
            param.paramName = CTRParameters.QD_CLICK;
            param.paramGroup = CTRParameters.ALG_NAME;

            DistributedIndex.QueryUrlIndex index = new DistributedIndex.QueryUrlIndex(record.query,
                    url);
            param.paramIndex = index.toString();
            param.paramValue = Integer.toString(1);

            context.write(param.extractKey(), param.extractValue());
        }
    }

    private void map_global(QRecord record, Mapper.Context context) throws IOException, InterruptedException {
        for(String url : record.shownLinks)
        {
            DistributedParameter param = new DistributedParameter();
            param.paramName = CTRParameters.GLOBAL_SHOW;
            param.paramGroup = CTRParameters.ALG_NAME;

            DistributedIndex.UrlIndex index = new DistributedIndex.UrlIndex(url);
            param.paramIndex = index.toString();
            param.paramValue = Integer.toString(1);

            context.write(param.extractKey(), param.extractValue());
        }

        for(String url : record.clickedLinks)
        {
            DistributedParameter param = new DistributedParameter();
            param.paramName = CTRParameters.GLOBAL_CLICK;
            param.paramGroup = CTRParameters.ALG_NAME;

            DistributedIndex.UrlIndex index = new DistributedIndex.UrlIndex(url);
            param.paramIndex = index.toString();
            param.paramValue = Integer.toString(1);

            context.write(param.extractKey(), param.extractValue());
        }
    }

    private void map_rank(QRecord record, Mapper.Context context) throws IOException, InterruptedException {
        for(int  i =0; i< record.shownLinks.size(); i++)
        {
            String url = record.shownLinks.get(i);

            DistributedParameter param = new DistributedParameter();
            param.paramName = CTRParameters.RANK_SHOW;
            param.paramGroup = CTRParameters.ALG_NAME;

            DistributedIndex.UrlIndex index = new DistributedIndex.UrlIndex(url);
            param.paramIndex = index.toString();
            param.paramValue = Integer.toString(1) + LIST_DELIMETER + Integer.toString(i);

            context.write(param.extractKey(), param.extractValue());
        }

        for(int  i =0; i< record.clickedLinks.size(); i++)
        {
            String url = record.clickedLinks.get(i);

            DistributedParameter param = new DistributedParameter();
            param.paramName = CTRParameters.RANK_CLICK;
            param.paramGroup = CTRParameters.ALG_NAME;

            DistributedIndex.UrlIndex index = new DistributedIndex.UrlIndex(url);
            param.paramIndex = index.toString();
            param.paramValue = Integer.toString(1) + LIST_DELIMETER + Integer.toString(i);

            context.write(param.extractKey(), param.extractValue());
        }
    }

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        QRecord record = new QRecord();
        record.parseString(value.toString());

        map_global(record, context);
        map_qdoc(record, context);
        //map_rank(record, context);

    }

    @Override
    public boolean filter_reduce(Text key, Text value) {
        DistributedParameter param = new DistributedParameter();
        param.parseKeyValue(key.toString(), value.toString());

        return param.paramGroup.equals(CTRParameters.ALG_NAME);
    }

    @Override
    public void combiner_update(Text key, Text value, Reducer.Context context) {
        reduce_update(key, value, context);
    }


    public void combiner_final_global(Text key, Reducer.Context context) throws IOException, InterruptedException {
        if(!DistributedIndex.UrlIndex.checkParse(key.toString()))
            return;

        {
            if(parameters.global_shows > 0 || !FILTER_ZERO_VALS) {
                DistributedParameter param = new DistributedParameter();
                param.paramName = CTRParameters.GLOBAL_SHOW;
                param.paramGroup = CTRParameters.ALG_NAME;

                param.paramValue = Integer.toString((int) parameters.global_shows);

                context.write(key, param.extractValue());
            }
        }

        {
            if(parameters.global_clicks > 0 || !FILTER_ZERO_VALS) {
                DistributedParameter param = new DistributedParameter();
                param.paramName = CTRParameters.GLOBAL_CLICK;
                param.paramGroup = CTRParameters.ALG_NAME;

                param.paramValue = Integer.toString((int) parameters.global_clicks);

                context.write(key, param.extractValue());
            }
        }

    }

    public void combiner_final_qdoc(Text key, Reducer.Context context) throws IOException, InterruptedException {
        if(!DistributedIndex.QueryUrlIndex.checkParse(key.toString()))
            return;

        {
            if(parameters.qd_shows > 0 || !FILTER_ZERO_VALS) {
                DistributedParameter param = new DistributedParameter();
                param.paramName = CTRParameters.QD_SHOW;
                param.paramGroup = CTRParameters.ALG_NAME;

                param.paramValue = Integer.toString((int) parameters.qd_shows);

                context.write(key, param.extractValue());
            }
        }

        {
            if(parameters.qd_clicks > 0 || !FILTER_ZERO_VALS) {
                DistributedParameter param = new DistributedParameter();
                param.paramName = CTRParameters.QD_CLICK;
                param.paramGroup = CTRParameters.ALG_NAME;

                param.paramValue = Integer.toString((int) parameters.qd_clicks);

                context.write(key, param.extractValue());
            }
        }
    }

    public void combiner_final_rank(Text key, Reducer.Context context) throws IOException, InterruptedException {
        if(!DistributedIndex.UrlIndex.checkParse(key.toString()))
            return;

        for(int rank =0; rank< CTRParameters.MAX_RANK; rank++)
        {
            long r_shows = parameters.rank_shows[rank];
            long r_clicks = parameters.rank_clicks[rank];

            {
                DistributedParameter param = new DistributedParameter();
                param.paramName = CTRParameters.RANK_SHOW;
                param.paramGroup = CTRParameters.ALG_NAME;

                param.paramValue = Long.toString(r_shows) + LIST_DELIMETER + Integer.toString(rank);

                context.write(key, param.extractValue());
            }

            {
                DistributedParameter param = new DistributedParameter();
                param.paramName = CTRParameters.RANK_CLICK;
                param.paramGroup = CTRParameters.ALG_NAME;

                param.paramValue = Long.toString(r_clicks) + LIST_DELIMETER + Integer.toString(rank);

                context.write(key, param.extractValue());
            }
        }

    }

    @Override
    public void combiner_final(Text key, Reducer.Context context) throws IOException, InterruptedException {
        combiner_final_global(key, context);
        combiner_final_qdoc(key, context);
        //combiner_final_rank(key, context);
    }

    private void reduce_update_global(DistributedParameter param)  {
        if(param.paramName.equals(CTRParameters.GLOBAL_CLICK))
        {
            parameters.global_clicks += Integer.parseInt(param.paramValue);
        }

        if(param.paramName.equals(CTRParameters.GLOBAL_SHOW))
        {
            parameters.global_shows += Integer.parseInt(param.paramValue);
        }
    }

    private void reduce_update_rank(DistributedParameter param) {
        if(param.paramName.equals(CTRParameters.RANK_CLICK))
        {
            String[] val_args = param.paramValue.split(LIST_DELIMETER);

            int rank = Integer.parseInt(val_args[1]);
            if(rank < CTRParameters.MAX_RANK && rank>=0)
            {
                parameters.rank_clicks[rank] += Integer.parseInt(val_args[0]);
//                parameters.rank_clicks.put(rank, parameters.rank_clicks.getOrDefault(rank, 0L) + 1);
            }
            if(rank >= CTRParameters.MAX_RANK)
            {
                parameters.rank_clicks[CTRParameters.MAX_RANK-1] += Integer.parseInt(val_args[0]);
            }
        }

        if(param.paramName.equals(CTRParameters.RANK_SHOW))
        {
            String[] val_args = param.paramValue.split(LIST_DELIMETER);
            int rank = Integer.parseInt(val_args[1]);
            if(rank < CTRParameters.MAX_RANK && rank>=0)
            {
                parameters.rank_shows[rank] += Integer.parseInt(val_args[0]);;
//              parameters.rank_shows.put(rank, parameters.rank_shows.getOrDefault(rank, 0L) + 1);
            }
            if(rank >= CTRParameters.MAX_RANK)
            {
                parameters.rank_shows[CTRParameters.MAX_RANK-1] += Integer.parseInt(val_args[0]);
            }

        }
    }

    private void reduce_update_qdoc(DistributedParameter param) {

        if(param.paramName.equals(CTRParameters.QD_SHOW))
        {
            parameters.qd_shows += Integer.parseInt(param.paramValue);
        }

        if(param.paramName.equals(CTRParameters.QD_CLICK))
        {
            parameters.qd_clicks += Integer.parseInt(param.paramValue);
        }
    }

    @Override
    public void reduce_update(Text key, Text value, Reducer.Context context) {
        DistributedParameter param = new DistributedParameter();
        param.parseKeyValue(key.toString(), value.toString());

        if(!param.paramGroup.equals(CTRParameters.ALG_NAME))
            return;

        reduce_update_global(param);
        reduce_update_qdoc(param);
        //reduce_update_rank(param);
    }

    private String reduce_final_global(MutableBoolean isNotZero)  {
        double global_ctr = (double)parameters.global_clicks/(double)parameters.global_shows;

        if(global_ctr == 0){
            isNotZero.setValue(isNotZero.booleanValue() || false);
            return null;
        }
            //return  "";

        String value = Double.toString(global_ctr);
        return value;
    }

    private String reduce_final_rank(MutableBoolean isNotZero) {
        boolean isPositive = false;
        StringBuilder rank_ctr_builder = new StringBuilder();
        for(int r = 0; r<CTRParameters.MAX_RANK; r++)
        {
            double rank_shows = (double)parameters.rank_shows[r];
            double rank_clicks = (double)parameters.rank_clicks[r];

            if(rank_shows == 0) {
                rank_shows = 1;
                rank_clicks = 0;
            }

            double rank_ctr = rank_clicks/(rank_shows);


            if(rank_clicks == 0)
                rank_ctr = 0;


            if(rank_ctr > 0)
                isPositive = true;

            rank_ctr_builder.append(Integer.toString(r)).append(FIELD_DELIMETER).append(
                    Double.toString(rank_ctr)).append(LIST_DELIMETER);
        }

        String rank_ctr = rank_ctr_builder.toString();
        rank_ctr = rank_ctr.substring(0, rank_ctr.length() -1);

        if(!isPositive)
            isNotZero.setValue(isNotZero.booleanValue() || false);
//            return "";

        return rank_ctr;
    }

    private String reduce_final_qdoc(MutableBoolean isNotZero) {
        double qdoc_ctr = (double)parameters.qd_clicks/(double)parameters.qd_shows;
        if(qdoc_ctr == 0) {
            isNotZero.setValue(false);
            return null;
        }

        String value = Double.toString(qdoc_ctr);
        return value;
    }

    @Override
    public ReduceResult reduce_final(Text key, Reducer.Context context) throws IOException, InterruptedException {
        if(DistributedIndex.UrlIndex.checkParse(key.toString()))
        {
            String out = "";
            DistributedIndex.UrlIndex index = new DistributedIndex.UrlIndex();
            index.parseString(key.toString());

            MutableBoolean isNotZero = new MutableBoolean(true);

            String global_ctr_out = reduce_final_global(isNotZero);
            //String rank_ctr_out = reduce_final_rank(isNotZero);

//            if(global_ctr_out.isEmpty() && rank_ctr_out.isEmpty())
//            {
//                return new writables.ReduceResult();
//            }

           // out = global_ctr_out + DELIMETER + rank_ctr_out;
            out = global_ctr_out;

            //TODO: emit with group
            ReduceResult res = new ReduceResult(FeaturesJob.DOC_GROUP_NAME,
                    index.toStringNoTag(), Arrays.asList(global_ctr_out));

            res.isNotZero = isNotZero.isTrue();

            return res;
//            context.write(new Text(index.url), new Text(out));
        }

        if(DistributedIndex.QueryUrlIndex.checkParse(key.toString()))
        {
            String out = "";
            DistributedIndex.QueryUrlIndex index = new DistributedIndex.QueryUrlIndex();
            index.parseString(key.toString());

            MutableBoolean isNotZero = new MutableBoolean(true);

            String qdoc_ctr_out = reduce_final_qdoc(isNotZero);
            out += qdoc_ctr_out;

//            if(out.isEmpty() || out.length() == 0)
//            {
//                return null;
//            }

            //TODO: emit with group
            //context.write(new Text(index.toString(), new Text(out));
            ReduceResult res = new ReduceResult(FeaturesJob.QD_GROUP_NAME,
                    index.toStringNoTag(),  Arrays.asList(qdoc_ctr_out));

            res.isNotZero = isNotZero.isTrue();

            return res;
        }

        return null;

    }
}
