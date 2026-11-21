import java.io.IOException;

import java.util.ArrayList;

import java.util.List;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ProductQuantityMapReduce {



    // DataCo CSV column positions (zero-based)

    private static final int QUANTITY_INDEX = 36;

    private static final int PRODUCT_ID_INDEX = 44;



    public static class QuantityMapper

            extends Mapper<LongWritable, Text, Text, IntWritable> {



        private final Text productId = new Text();

        private final IntWritable quantity = new IntWritable();



        private List<String> parseCSV(String line) {



            List<String> fields = new ArrayList<String>();

            StringBuilder field = new StringBuilder();

            boolean insideQuotes = false;



            for (int i = 0; i < line.length(); i++) {

                char c = line.charAt(i);



                if (c == '"') {

                    if (insideQuotes && i + 1 < line.length()

                            && line.charAt(i + 1) == '"') {

                        field.append('"');

                        i++;

                    } else {

                        insideQuotes = !insideQuotes;

                    }

                } else if (c == ',' && !insideQuotes) {

                    fields.add(field.toString());

                    field.setLength(0);

                } else {

                    field.append(c);

                }

            }



            fields.add(field.toString());

            return fields;

        }



        public void map(LongWritable key, Text value, Context context)

                throws IOException, InterruptedException {



            String line = value.toString();



            if (line.trim().isEmpty()) {

                return;

            }



            // Skip CSV header

            if (line.startsWith("Type,")) {

                return;

            }



            List<String> fields = parseCSV(line);



            if (fields.size() <= PRODUCT_ID_INDEX ||

                    fields.size() <= QUANTITY_INDEX) {

                return;

            }



            try {

                String id = fields.get(PRODUCT_ID_INDEX).trim();

                String qty = fields.get(QUANTITY_INDEX).trim();



                if (id.isEmpty() || qty.isEmpty()) {

                    return;

                }



                int quantityValue = Integer.parseInt(qty);



                if (quantityValue <= 0) {

                    return;

                }



                productId.set(id);

                quantity.set(quantityValue);



                context.write(productId, quantity);



            } catch (NumberFormatException e) {

                // Ignore invalid records

            }

        }

    }



    public static class QuantityReducer

            extends Reducer<Text, IntWritable, Text, LongWritable> {



        private final LongWritable total = new LongWritable();



        public void reduce(Text key, Iterable<IntWritable> values,

                Context context)

                throws IOException, InterruptedException {



            long sum = 0;



            for (IntWritable value : values) {

                sum += value.get();

            }



            total.set(sum);

            context.write(key, total);

        }

    }



    public static void main(String[] args) throws Exception {



        if (args.length != 2) {

            System.err.println(

                "Usage: ProductQuantityMapReduce <input> <output>");

            System.exit(2);

        }



        Configuration conf = new Configuration();



        Job job = Job.getInstance(conf,

                "Product Quantity Aggregation");



        job.setJarByClass(ProductQuantityMapReduce.class);



        job.setMapperClass(QuantityMapper.class);

        job.setReducerClass(QuantityReducer.class);



        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);



        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(LongWritable.class);



        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));



        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
