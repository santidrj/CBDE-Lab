package cbde.labs.hbase_mapreduce.reader;

public class MyHBaseReader_KeyDesign extends MyHBaseReader {

    protected String scanStart() {
        //Sabemos que el primer id será, como mínimo, "type_3-0-0", por tanto comenzamos el scan desde la key
        // "type_3-0-", de manera que todas las filas que nos interesan entrarán dentro del rango.
        return "type_3-0-";
    }

    protected String scanStop() {
        /*Sabemos que todas las tuplas que cumplan la condicion cumpliran la condición de que su key sea "type_3-0- +
         id", por tanto, sabemos que estaran antes que la "type-3-1-".
        */
        return "type_3-1-";
    }

}
