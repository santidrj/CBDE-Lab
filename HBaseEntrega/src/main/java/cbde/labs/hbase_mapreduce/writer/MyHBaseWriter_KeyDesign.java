package cbde.labs.hbase_mapreduce.writer;

public class MyHBaseWriter_KeyDesign extends MyHBaseWriter {

    protected String nextKey() {
        String type = data.get("type");
        String region = data.get("region");
        if ("type_1".equals(type) && "3".equals(region)) {
            return "A" + key;
        }
        return "Z" + key;
    }

}
