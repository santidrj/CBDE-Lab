package cbde.labs.hbase_mapreduce.writer;

public class MyHBaseWriter_VerticalPartitioning extends MyHBaseWriter {
    //create 'wines2', 'q1', 'others'

    protected String toFamily(String attribute) {
        //Comprobamos si el atributo coincide con los de la familia Q1 i retornamos la familía adecuada para cada caso
        if ("type".equals(attribute) || "region".equals(attribute) || "flav".equals(attribute)) {
            return "q1";
        }
        return "others";
    }

}
