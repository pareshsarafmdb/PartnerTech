package dataflowdemonew;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO.Read;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.bson.Document;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import dataflowdemonew.DataflowMongoToBigQuery.Options;

public class MongoDataFlowBigQuery {
	
	 // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
	  private static final String WEATHER_SAMPLES_TABLE =
	      "clouddataflow-readonly:samples.weather_stations";
	  /**
	   * Options supported by {@link MaxPerKeyExamples}.
	   *
	   * <p>Inherits standard configuration options.
	   */
	  public interface Options extends PipelineOptions, GcpOptions {
	    
	    @Description("Table to write to, specified as " + "<project_id>:<dataset_id>.<table_id>")
	    @Validation.Required
	    String getMongouri();

	    void setMongouri(String value);
	    
	  }
	  

	  public static void main(String[] args) throws Exception {
		    
		    
	    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
	    Pipeline p = Pipeline.create(options);
	  
	    System.out.println("mongoURI *********************" + options.getMongouri());
	    System.out.println("project *********************" + options.getProject());
	    
	    // Build the table schema for Customer

	    List<TableFieldSchema> customerFields = new ArrayList<>();
	    customerFields.add(new TableFieldSchema().setName("customerid").setType("STRING"));
	    customerFields.add(new TableFieldSchema().setName("region").setType("INT64"));
	    customerFields.add(new TableFieldSchema().setName("firstname").setType("STRING"));
	    customerFields.add(new TableFieldSchema().setName("lastname").setType("STRING"));
	    customerFields.add(new TableFieldSchema().setName("cell").setType("INT64"));
	    customerFields.add(new TableFieldSchema().setName("email").setType("STRING"));
	    customerFields.add(new TableFieldSchema().setName("yob").setType("STRING"));
	    customerFields.add(new TableFieldSchema().setName("gender").setType("STRING"));
	    TableSchema customerSchema = new TableSchema().setFields(customerFields);
	    
	    //Build schema for Address
	    List<TableFieldSchema> addressFields = new ArrayList<>();
	    addressFields.add(new TableFieldSchema().setName("customerid").setType("STRING"));
	    addressFields.add(new TableFieldSchema().setName("number").setType("INT64"));
	    addressFields.add(new TableFieldSchema().setName("street").setType("STRING"));
	    addressFields.add(new TableFieldSchema().setName("city").setType("STRING"));
	    addressFields.add(new TableFieldSchema().setName("state").setType("STRING"));
	    addressFields.add(new TableFieldSchema().setName("zip").setType("STRING"));	
	    TableSchema addressSchema = new TableSchema().setFields(addressFields);

	    //Build schema for Policy
	    List<TableFieldSchema> policyFields = new ArrayList<>();
	    policyFields.add(new TableFieldSchema().setName("customerid").setType("STRING"));
	    policyFields.add(new TableFieldSchema().setName("policyType").setType("STRING"));
	    policyFields.add(new TableFieldSchema().setName("policyNum").setType("STRING"));
	    policyFields.add(new TableFieldSchema().setName("nextRenewalDt").setType("STRING"));
	    policyFields.add(new TableFieldSchema().setName("model").setType("STRING"));
	    policyFields.add(new TableFieldSchema().setName("year").setType("INT64"));
	    policyFields.add(new TableFieldSchema().setName("value").setType("INT64"));

	    TableSchema policySchema = new TableSchema().setFields(policyFields);
	    
	    Read read= MongoDbIO.read().
				withUri(options.getMongouri()).withBucketAuto(true).
				withDatabase("policydata").withCollection("customers");
	    
	    PCollection<Document> lines= p.apply(read);
	    
	    TableReference table1 =  new TableReference();
	    
	    table1.setProjectId(options.getProject());
	    table1.setDatasetId("policydata");
	    table1.setTableId("customers");
	    
	    TableReference table2 =  new TableReference();
	    table2.setProjectId(options.getProject());
	    table2.setDatasetId("policydata");
	    table2.setTableId("addresses");
	    
	    TableReference table3 =  new TableReference();
	    table3.setProjectId(options.getProject());
	    table3.setDatasetId("policydata");
	    table3.setTableId("policies");
	    
		lines.apply("customerData", MapElements.via(
			    new SimpleFunction<Document, TableRow>(){
			        @Override
			        public TableRow apply(Document document) {
			   
			            String customerid =   document.getObjectId("_id").toString();
			            Integer region = document.getInteger("region");
			            String firstname = document.getString("firstname");
			            String lastname = document.getString("lastname");
			            Long cell = document.getLong("cell");
			            String email = document.getString("email");
			            String yob = document.getDate("yob").toString();
			            String gender = document.getString("gender");
			            
			            		    
			            TableRow row =
			      	          new TableRow()
			      	              .set("customerid", customerid)
			      	              .set("region", region)
			      	              .set("firstname",firstname)
			      	              .set("lastname", lastname)
			      	              .set("cell", cell)
			      	              .set("email", email)
			      	              .set("yob", yob)
			      	              .set("gender", gender);
			            return row;
			           
			        }
			    }
			))
	        .apply(
	            BigQueryIO.writeTableRows()
	                .to(table1)
	                .withSchema(customerSchema)
	                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		lines.apply("addressData", MapElements.via(
			    new SimpleFunction<Document, TableRow>(){
			        @Override
			        public TableRow apply(Document document) {
			            
			    	        String customerid =   document.getObjectId("_id").toString();
			            Document doc =  (Document)document.get("address");
			            
			            int number =   doc.getInteger("number");
			            String street =   doc.getString("street");
			            String city =   doc.getString("city");
			            String state =   doc.getString("state");
			            String zip = doc.getString("zip");
			            
			            TableRow row1 =
			      	          new TableRow()
			      	          	.set("customerid", customerid)
			      	              .set("number", number)
			      	              .set("street", street)
			      	              .set("city", city)
			      	              .set("state", state)
			      	              .set("zip", zip);
			            return row1;
			           
			        }
			    }
			))
	        .apply(
	            BigQueryIO.writeTableRows()
	                .to(table2)
	                .withSchema(addressSchema)
	                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		
		lines.apply("policyData", FlatMapElements.via(
			    new SimpleFunction<Document, ArrayList<TableRow>>(){
			        @Override
			        public ArrayList<TableRow> apply(Document document) {
			            
			    	        
			    	        List<Document> policies = (List<Document>) document.get("policies");
			    	        
			    		    ArrayList<TableRow> rows = new ArrayList<>();
			    		    
			            for (Document policy : policies) {
			            	    String customerid =   document.getObjectId("_id").toString();
				            String policyType =   policy.getString("policyType");
				            String policyNum =   policy.getString("policyNum");
				            String nextRenewalDt =   policy.getDate("nextRenewalDt").toString();
				            String model =   policy.getString("model");
				            int year = policy.getInteger("year");
				            int value = policy.getInteger("value");
				            TableRow row1 =
					      	          new TableRow()
					      	          	.set("customerid", customerid)
					      	              .set("policyType", policyType)
					      	              .set("policyNum", policyNum)
					      	              .set("nextRenewalDt", nextRenewalDt)
					      	              .set("model", model)
					      	              .set("year", year)
					      	              .set("value", value);
				            rows.add(row1);
			            } 
			            
			            return rows;
			           
			        }
			    }
			))
	        .apply(
	            BigQueryIO.writeTableRows()
	                .to(table3)
	                .withSchema(policySchema)
	                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
	    p.run().waitUntilFinish();
	  }

}
