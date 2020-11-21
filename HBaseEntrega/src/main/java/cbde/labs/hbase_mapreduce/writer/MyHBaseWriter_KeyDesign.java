package cbde.labs.hbase_mapreduce.writer;

public class MyHBaseWriter_KeyDesign extends MyHBaseWriter {

    protected String nextKey() {
        /*Debido a que los indices se ordenan lexicogŕaficamente decidimos que a las tuplas que cumplen la condición
        de la query se les asigne una key A + id y a las que no la cumplen Z + id, de forma que todas las tuplas que
        cumplan la condicion esten todas en la A y el resto en la Z.
        */
        String type = data.get("type");
        String region = data.get("region");
        if ("type_3".equals(type) && "0".equals(region)) {
            return "A" + key;
        }
        return "Z" + key;
    }

}
