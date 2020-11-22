package cbde.labs.hbase_mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AggregationAvg extends JobMapReduce {

    public static class AggregationAvgMapper extends Mapper<Text, Text, Text, DoubleWritable> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            /*
            Hacemos el fetch de los parametros del group by y de la agregación
             */
            String groupBy = context.getConfiguration().getStrings("groupBy")[0];
            String agg = context.getConfiguration().getStrings("agg")[0];

            /*
            Separamos los campos del input, los quales sabemos que estan separados por comas.
             */
            String[] arrayValues = value.toString().split(",");

            /*
			Finalmente, obtenemos el valor por el que queremos agrupar y el valor del que queremos calcular la media y
			los escribimos.
			 */
            String groupByValue = Utils.getAttribute(arrayValues, groupBy);
            double aggValue = Double.parseDouble(Utils.getAttribute(arrayValues, agg));
            context.write(new Text(groupByValue), new DoubleWritable(aggValue));
        }
    }

    public static class AggregationAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
            /*
			Inicializamos el avg y un contador a 0, calculamos la media para todos los valores de un mismo grupo y la
			escribimos.
			 */
            double avg = 0;
            int counter = 0;
            for (DoubleWritable value : values) {
                avg += value.get();
                counter += 1;
            }
            avg /= counter;
            context.write(key, new DoubleWritable(avg));
        }
    }

    public AggregationAvg() {
        this.input = null;
        this.output = null;
    }

    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        // Define the new job and the name it will be given
        Job job = Job.getInstance(configuration, "AggregationAvg");
        AggregationAvg.configureJob(job, this.input, this.output);
        // Let's run it!
        return job.waitForCompletion(true);
    }

    public static void configureJob(Job job, String pathIn, String pathOut)
        throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(AggregationAvg.class);

        //Definimos el mapper y el tipo de las keys y los valores de salida, que en este caso son texto y double.
        job.setMapperClass(AggregationAvg.AggregationAvgMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        /*
		En este caso, si que necesitamos tanto combiner como reducer así que los definimos. En esta ocasión el
		reducer y el combiner serán el mismo, ya que el reducer es asociativo y commutativo.
		 */
        job.setCombinerClass(AggregationAvg.AggregationAvgReducer.class);
        job.setReducerClass(AggregationAvg.AggregationAvgReducer.class);

        /*
        Especificamos el formato de salida y de entrada del job.
         */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        /*
        Especificamos los path de entrada y salida de los ficheros.
         */
        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));

        /*
        Finalmente le pasamos los parámetros necesarios a la configuración, en este caso primero le pasamos los del
        group by, en este caso el atributo type, y los valores de la agregación, en este caso el atributo col.
         */
        job.getConfiguration().setStrings("groupBy", "type");
        job.getConfiguration().setStrings("agg", "col");
    }
}
