package dataflowdemonew;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class DataFlowDemo {
	
	public static void main(String[] args) {
		DataflowOptions options =
		        PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowOptions.class);
		 //PipelineOptions options = PipelineOptionsFactory.create();
         //options.setRunner(DataflowRunner.class);
         //options.setTempLocation("gs://erudite-scholar-262008/tmp/");
		Pipeline p =Pipeline.create(options);
		//p.apply(MongoDbIO.read().withUri("mongodb://localhost:27017").
		//		withDatabase("my-database").withCollection("my-collection"));
		
		p.apply(TextIO.read().from("gs://paresh_saraf/input/inputfile.txt")).apply("ParseAndConvertToKV", MapElements.via(
			    new SimpleFunction<String, KV<String, Integer>>() {
			        @Override
			        public KV<String, Integer> apply(String input) {
			            String[] split = input.split(",");
			            if (split.length < 4) {
			                return null;
			            }
			            String key = split[1];
			            Integer value = Integer.valueOf(split[3]);
			            return KV.of(key, value);
			        }
			    }
			)).apply(GroupByKey.<String, Integer>create()).apply
		    ("SumUpValuesByKey", ParDo.of(new DoFn<KV<String, Iterable<Integer>>, String>() {
			    @ProcessElement
			    public void processElement(ProcessContext context) {
			        Integer totalSales = 0;
			        String brand = context.element().getKey();
			        Iterable<Integer> sales = context.element().getValue();
			        for (Integer amount : sales) {
			            totalSales += amount;
			        }
			        context.output(brand + ": " + totalSales);
			    }
			})).apply(TextIO.write().to("gs://paresh_saraf/input/outputfile.txt").withoutSharding());

        p.run();



	}
	
	public interface DataflowOptions extends PipelineOptions {

	    /**
	     * By default, this example reads from a public dataset containing the text of King Lear. Set
	     * this option to choose a different input file or glob.
	     */
	    @Description("Path of the file to read from")
	    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
	    String getInputFile();

	    void setInputFile(String value);

	    /** Set this required option to specify where to write the output. */
	    @Description("Path of the file to write to")
	    @Required
	    String getOutput();

	    void setOutput(String value);
	  }

}
