package cbde.labs.hbase_mapreduce.writer;

public class MyHBaseWriter_KeyDesign extends MyHBaseWriter {

    protected String nextKey() {
        /*Debido a que los indices se ordenan lexicogŕaficamente decidimos que a las tuplas que cumplen la condición
        de la query se les asigne una key tal que sea la concatenación del tipo, la región y el índice que genera
        MyHBaseWriter, de esta forma todas las tuplas de un mismo tipo y región quedarán agrupadas en la misma zona.
        */
        String type = data.get("type");
        String region = data.get("region");
        return type + "-" + region + "-" + key;
    }

}
