package dataflowdemonew;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO.Read;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class DataflowMongoToBigQuery {
	  // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
	  private static final String WEATHER_SAMPLES_TABLE =
	      "clouddataflow-readonly:samples.weather_stations";
	  /**
	   * Options supported by {@link MaxPerKeyExamples}.
	   *
	   * <p>Inherits standard configuration options.
	   */
	  public interface Options extends PipelineOptions {
	    @Description("Table to read from, specified as " + "<project_id>:<dataset_id>.<table_id>")
	    @Default.String(WEATHER_SAMPLES_TABLE)
	    String getInput();

	    void setInput(String value);

	    @Description("Table to write to, specified as " + "<project_id>:<dataset_id>.<table_id>")
	    @Validation.Required
	    String getOutput();

	    void setOutput(String value);
	  }
	  

	  public static void main(String[] args) throws Exception {

	    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
	    Pipeline p = Pipeline.create(options);

	    // Build the table schema for sale
	    List<TableFieldSchema> saleFields = new ArrayList<>();
	    saleFields.add(new TableFieldSchema().setName("storeLocation").setType("STRING"));
	    saleFields.add(new TableFieldSchema().setName("couponUsed").setType("BOOL"));
	    saleFields.add(new TableFieldSchema().setName("purchaseMethod").setType("STRING"));
	    saleFields.add(new TableFieldSchema().setName("saleDate").setType("STRING"));
	    TableSchema saleSchema = new TableSchema().setFields(saleFields);
	    
	    //Build schema for customer
	    List<TableFieldSchema> customerFields = new ArrayList<>();
	    customerFields.add(new TableFieldSchema().setName("saleId").setType("STRING"));
	    customerFields.add(new TableFieldSchema().setName("gender").setType("STRING"));
	    customerFields.add(new TableFieldSchema().setName("age").setType("INT64"));
	    customerFields.add(new TableFieldSchema().setName("email").setType("STRING"));
	    customerFields.add(new TableFieldSchema().setName("satisfaction").setType("INT64"));	
	    TableSchema customerSchema = new TableSchema().setFields(customerFields);

	    //Build schema for saleItem
	    List<TableFieldSchema> saleItemFields = new ArrayList<>();
	    saleItemFields.add(new TableFieldSchema().setName("saleId").setType("STRING"));
	    saleItemFields.add(new TableFieldSchema().setName("name").setType("STRING"));
	    saleItemFields.add(new TableFieldSchema().setName("price").setType("FLOAT"));
	    saleItemFields.add(new TableFieldSchema().setName("quantity").setType("INT64"));
	    TableSchema saleItemSchema = new TableSchema().setFields(saleItemFields);
	    
	    Read read= MongoDbIO.read().
				withUri("mongodb+srv://murugabms:kirbms2006@murugacluster-fgped.mongodb.net/test").withBucketAuto(true).
				withDatabase("sample_supplies").withCollection("sales");
	    
	    PCollection<Document> lines= p.apply(read);
	    
	    TableReference table1 =  new TableReference();
	    table1.setProjectId("dev-fortress-263217");
	    table1.setDatasetId("sales");
	    table1.setTableId("saletable");
	    
	    TableReference table2 =  new TableReference();
	    table2.setProjectId("dev-fortress-263217");
	    table2.setDatasetId("sales");
	    table2.setTableId("customertable");
	    
	    TableReference table3 =  new TableReference();
	    table3.setProjectId("dev-fortress-263217");
	    table3.setDatasetId("sales");
	    table3.setTableId("saleitemtable");
	    
		lines.apply("salesData", MapElements.via(
			    new SimpleFunction<Document, TableRow>(){
			        @Override
			        public TableRow apply(Document document) {
			            
			            String storeLocation =   document.getString("storeLocation");
			            Boolean couponUsed = document.getBoolean("couponUsed");
			            String purchaseMethod = document.getString("purchaseMethod");
			            String saleDate = document.getDate("saleDate").toString();
			            
//			            DateFormat df = new SimpleDateFormat("yyyy-MM-dd-kk.mm.ss.SSS");
//			            Date saleDateFinal = null;
//						try {
//							saleDateFinal = df.parse(saleDate);
//						} catch (ParseException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}

			            		    
			            TableRow row =
			      	          new TableRow()
			      	              .set("storeLocation", storeLocation)
			      	              .set("couponUsed", couponUsed)
			      	              .set("purchaseMethod",purchaseMethod)
			      	              .set("saleDate", saleDate);
			            return row;
			           
			        }
			    }
			))
	        .apply(
	            BigQueryIO.writeTableRows()
	                .to(table1)
	                .withSchema(saleSchema)
	                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		lines.apply("customerData", MapElements.via(
			    new SimpleFunction<Document, TableRow>(){
			        @Override
			        public TableRow apply(Document document) {
			            
			        	Document doc =  (Document)document.get("customer");
			            
			            String gender =   doc.getString("gender");
			            int age = doc.getInteger("age");
			            String email =   doc.getString("gender");
			            int satisfaction = doc.getInteger("satisfaction");
			            
			            TableRow row1 =
			      	          new TableRow()
			      	          	.set("saleId", document.getObjectId("_id").toString())
			      	              .set("gender", gender)
			      	              .set("age", age)
			      	              .set("email", email)
			      	              .set("satisfaction", satisfaction);
			            return row1;
			           
			        }
			    }
			))
	        .apply(
	            BigQueryIO.writeTableRows()
	                .to(table2)
	                .withSchema(customerSchema)
	                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		
	    p.run().waitUntilFinish();
	  }
}
