package cbde.labs.hbase_mapreduce.reader;

public class MyHBaseReader_VerticalPartitioning extends MyHBaseReader {

    protected String[] scanFamilies() {
        //Ya que para la query solo necesitamos leer la familia Q1 retornamos la familía correspondiente
        return new String[]{"q1"};
    }

}

