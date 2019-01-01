package writables;

import org.apache.hadoop.io.Text;

public class DistributedParameter  {



    private static String DELIMETER = "\t";

    public String paramIndex;

    public String paramName;
    public String paramGroup;

    public String paramValue;

    public DistributedParameter(){}

    public DistributedParameter(String paramName, String paramGroup)
    {
        this.paramName = paramName;
        this.paramGroup = paramGroup;
    }

    public DistributedParameter(String paramName, String paramGroup, String paramIndex, String paramValue)
    {
        this.paramName = paramName;
        this.paramGroup = paramGroup;
        this.paramIndex  = paramIndex;
        this.paramValue = paramValue;
    }

//
//    @Override
//    public void write(DataOutput out) throws IOException {
//        out.writeUTF(paramGroup);
//        out.writeUTF(paramName);
//        out.writeUTF(paramIndex);
//        out.writeUTF(paramValue);
//
//    }
//
//    @Override
//    public void readFields(DataInput in) throws IOException {
//        paramGroup = in.readUTF();
//        paramName = in.readUTF();
//        paramIndex = in.readUTF();
//        paramValue = in.readUTF();
//    }

    public Text extractKey()
    {
        return new Text(paramIndex);
    }

    public Text extractValue()
    {
        String val = "";
        val += paramGroup + DELIMETER;
        val += paramName + DELIMETER + paramValue;
        return new Text(val);
    }

    public void parseKeyValue(String key, String value)
    {
        paramIndex = key;
        String[] args = value.split(DELIMETER);
        paramGroup = args[0];
        paramName = args[1];
        paramValue = args[2];
    }

    public void parseKey(String key)
    {
        paramIndex = key;
    }

    public void parseValue(String value)
    {
        String[] args = value.split(DELIMETER);
        paramGroup = args[0];
        paramName = args[1];
        paramValue = args[2];
    }



}