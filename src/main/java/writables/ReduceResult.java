package writables;

import java.util.ArrayList;
import java.util.List;

public  class ReduceResult
{

    public static class IdxFeaturePair
    {
        public String name;
        public String value;
        public int idx = 0;
        public boolean isNotNull = false;

        public IdxFeaturePair(){}

        public IdxFeaturePair(String name, String value, int idx)
        {
            this.idx = idx;
            this.value = value;
        }
    }

    public static List<IdxFeaturePair> toIdxFeaturePairs(List<String> outValues, List<String> names)
    {
        List<ReduceResult.IdxFeaturePair> outPairs = new ArrayList<>();
        for (int i = 0; i < outValues.size(); i++) {
            String val = outValues.get(i);
            ReduceResult.IdxFeaturePair p = new ReduceResult.IdxFeaturePair();
            p.idx = i;
            p.value = val;
            p.name = names.get(i);

            if(val != null && val.length() > 0)
            {
                p.isNotNull = true;
            }
            else
            {
                p.isNotNull = false;
            }
            outPairs.add(p);
        }
        return outPairs;
    }

    public String Group;
    public String Key;
    public List<IdxFeaturePair> Value;
    public boolean isNotZero = true;

    public ReduceResult()
    {}

    public ReduceResult(String Group, String Key, List<IdxFeaturePair> Value)
    {
        this.Group = Group;
        this.Key = Key;
        this.Value = Value;
    }
}