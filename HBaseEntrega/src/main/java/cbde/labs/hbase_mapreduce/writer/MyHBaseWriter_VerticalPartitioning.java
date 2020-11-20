package cbde.labs.hbase_mapreduce.writer;

public class MyHBaseWriter_VerticalPartitioning extends MyHBaseWriter {
    //create 'wines2', 'q1', 'others'

    protected String toFamily(String attribute) {
        if ("type".equals(attribute) || "region".equals(attribute) || "flav".equals(attribute)) {
            return "q1";
        }
        return "others";
    }

}
