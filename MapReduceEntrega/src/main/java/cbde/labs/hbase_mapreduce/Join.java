package cbde.labs.hbase_mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class Join extends JobMapReduce {

	public static class JoinMapper extends Mapper<Text, Text, IntWritable, Text> {

		private static int N = 100;

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			/*
            Hacemos el fetch de los parametros del join, del external y del internal
             */
			String join = context.getConfiguration().getStrings("join")[0];
			String external = context.getConfiguration().getStrings("external")[0];
			String internal = context.getConfiguration().getStrings("internal")[0];

			/*
            Separamos los campos del input, los quales sabemos que estan separados por comas .
             */
			String[] arrayValues = value.toString().split(",");

			/*
			Obtenemos el valor del parametro join y si se cumplen las condiciónes generamos una key para separarlos.
			 */
			String joinValue = Utils.getAttribute(arrayValues, join);
			if (joinValue.equals(external)) {
				int newKey = (int)(Math.random()*N);
				context.write(new IntWritable(newKey), value);
			}
			else if (joinValue.equals(internal)) {
				for (int newKey = 0; newKey < N; newKey++) {
					context.write(new IntWritable(newKey), value);
				}
			}
		}

	}

	public static class JoinReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/*
            Hacemos el fetch de los parametros del join, del external y del region.
             */
			String join = context.getConfiguration().getStrings("join")[0];
			String external = context.getConfiguration().getStrings("external")[0];
			String region = context.getConfiguration().getStrings("regionValue")[0];

			//Separamos los sets entre internal y external
			ArrayList<String> externals = new ArrayList<String>();
			ArrayList<String> internals = new ArrayList<String>();
			for (Text value : values) {
				String[] arrayValues = value.toString().split(",");
				String joinValue = Utils.getAttribute(arrayValues, join);
				if (joinValue.equals(external)) {
					externals.add(value.toString());
				}
				else {
					internals.add(value.toString());
				}
			}
			/*
			Finalmente recorremos los sets comprobando que las regiones coincidan, y en caso afirmativo escribos los
			valores.
			 */
			for (int i = 0; i < externals.size(); i++) {
				for (int j = 0; j < internals.size(); j++) {
					String[] internalValues = internals.get(j).split(",");
					String[] externalValues = externals.get(i).split(",");
					if (Utils.getAttribute(externalValues, region).equals(Utils.getAttribute(internalValues, region))) {
						context.write(NullWritable.get(), new Text(externals.get(i)+"<->"+internals.get(j)));
					}
				}
			}
		}

	}

	public Join() {
		this.input = null;
		this.output = null;
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "Join");
		Join.configureJob(job, this.input, this.output);
	    // Let's run it!
	    return job.waitForCompletion(true);
	}

	public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(Join.class);

		//Definimos el mapper y el tipo de las keys y los valores de salida, que en este caso son intWritable y texto.
		job.setMapperClass(Join.JoinMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		/*
		En este caso, un reducer así que los definimos, pero no utilizamos un combiner porque en este caso el reducer
		no es commutativo.
		 */
		job.setReducerClass(Join.JoinReducer.class);

		/*
        Especificamos el formato de salida y de entrada del job.
         */
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		/*
        Especificamos los path de entraday salida de los ficheros.
         */
		FileInputFormat.addInputPath(job, new Path(pathIn));
		FileOutputFormat.setOutputPath(job, new Path(pathOut));

		/*
        Finalmente le pasamos los parámetros necesarios al mapper, en este caso primero le pasamos los del join,
        en este caso el atributo type, el valor del external, en este caso type_1, el valor del internal, en este caso
        type_2, y el atributo que utilezaremos para el reducer, en este caso region.
         */
		job.getConfiguration().setStrings("join", "type");
		job.getConfiguration().setStrings("regionValue", "region");
		job.getConfiguration().setStrings("external", "type_1");
		job.getConfiguration().setStrings("internal", "type_2");
    }
}
