package cbde.labs.hbase_mapreduce.reader;

public class MyHBaseReader_KeyDesign extends MyHBaseReader {

    protected String scanStart() {
        //Sabemos que el primer id será, como mínimo, A + 0, por tanto comenzamos el scan desde la A
        return "A";
    }

    protected String scanStop() {
        /*Sabemos que todas las tuplas que cumplan la condicion cumpliran la condición de que su ket sea A + id,
        por tanto, sabemos que estaran antes que la B.
        */
        return "B";
    }

}
