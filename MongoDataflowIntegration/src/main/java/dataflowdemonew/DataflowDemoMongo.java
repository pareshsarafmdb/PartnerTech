package dataflowdemonew;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;

public class DataflowDemoMongo {
	public static void main(String[] args) {
		DataflowOptions options =
		        PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowOptions.class);
		Pipeline p =Pipeline.create(options);
		p.apply(MongoDbIO.read().
				withUri("mongodb+srv://main_user:pare1212@freetier-fhbg9.mongodb.net/test").
				withDatabase("test").withCollection("test"))
		
		.apply("ParseAndConvertToKV", MapElements.via(
			    new SimpleFunction<Document, KV<String, Double>>() {
			        @Override
			        public KV<String, Double> apply(Document document) {
			            
			            String key = document.getString("brand");
			            Double value = document.getDouble("sales");
			            return KV.of(key, value);
			        }
			    }
			)).apply(GroupByKey.<String, Double>create()).apply
		    ("SumUpValuesByKey", ParDo.of(new DoFn<KV<String, Iterable<Double>>, String>() {
			    @ProcessElement
			    public void processElement(ProcessContext context) {
			        Double totalSales = 0.0;
			        String brand = context.element().getKey();
			        Iterable<Double> sales = context.element().getValue();
			        for (Double amount : sales) {
			            totalSales += amount;
			        }
			        context.output(brand + ": " + totalSales);
			    }
			})).apply(TextIO.write().to("gs://paresh_saraf/input/outputfilemongo.txt").withoutSharding());

        p.run().waitUntilFinish();;
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
