package cbde.labs.hbase_mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Selection extends JobMapReduce {

    public static class SelectionMapper extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            /*
            Hacemos el fetch de los parametros del select y de la condición del where
             */
            String[] selection = context.getConfiguration().getStrings("selection");
            String type = context.getConfiguration().getStrings("type")[0];

            /*
            Separamos los campos del input, los quales sabemos que estan separados por comas .
             */
            String[] values = value.toString().split(",");

            /*
            Comprobamos que el atributo type de la fila cumpla la condicion del select, y en caso afirmativo iteramos
            sobre los valores y los escribimos en la salida.
             */
            if (Utils.getAttribute(values, "type").equals(type)) {
                String selectionValue = Utils.getAttribute(values, selection[0]);
                StringBuilder newValue = new StringBuilder(selectionValue);
                for (int i = 1; i < selection.length; i++) {
                    selectionValue = Utils.getAttribute(values, selection[i]);
                    newValue.append("," + selectionValue);
                }
                context.write(key, new Text(newValue.toString()));
            }
        }
    }

    public Selection() {
        this.input = null;
        this.output = null;
    }

    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        // Define the new job and the name it will be given
        Job job = Job.getInstance(configuration, "Selection");
        configureJob(job, this.input, this.output);
        // Let's run it!
        return job.waitForCompletion(true);
    }

    public static void configureJob(Job job, String pathIn, String pathOut)
        throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(Selection.class);


        //Definimos el mapper y el tipo de las keys y los valores de salida, que en este caso ambos son texto.
        job.setMapperClass(Selection.SelectionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        /*
        Ya que en esta caso únicamente estamos haciendo una seleccion, y por tanto no es necesario el reduce pasamos
        a definir los tipos de salida del job.
        */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /*
        Especificamos el formato de entrada, en este caso SequenceFileInputFormat, y a continuación los path de entrada
        y salida de los ficheros.
         */
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));

        /*
        Finalmente le pasamos los parámetros necesarios al mapper, en este caso primero le pasamos los parámetros a
        seleccionar, en este caso todos los atributos de la table y posteriormente el atributo con el que queremos
        hacer el where, en este caso el atributo type.
         */
        job.getConfiguration()
            .setStrings("selection", "type", "region", "alc", "m_acid", "ash", "alc_ash", "mgn", "t_phenols", "flav",
                "nonflav_phenols", "proant", "col", "hue", "od280od315", "proline");
        job.getConfiguration().setStrings("type", "type_1");
    }
}
