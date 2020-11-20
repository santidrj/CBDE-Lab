package cbde.labs.hbase_mapreduce.reader;

public class MyHBaseReader_KeyDesign extends MyHBaseReader {

    protected String scanStart() {
        return "A";
    }

    protected String scanStop() {
        return "B";
    }

}
