package dataproc.demo.etl;
import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.google.gson.Gson;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;

import avro.shaded.com.google.common.collect.Lists;
import dataproc.demo.pojos.Account;
import dataproc.demo.pojos.Card;
import dataproc.demo.pojos.Client;
import dataproc.demo.pojos.Disp;
import dataproc.demo.pojos.District;
import dataproc.demo.pojos.Loan;
import dataproc.demo.pojos.Order;
import dataproc.demo.pojos.Transaction;
import scala.Tuple2;


public class ETLDemo {
	
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder()
			      //.master("local")
			      .appName("MongoSparkConnectorIntro")
			      .config("spark.mongodb.output.uri", "mongodb+srv://main_user:pare1212@freetier-fhbg9.mongodb.net/test.myCollection")
			      .config("spark.mongodb.input.uri", "mongodb+srv://main_user:pare1212@freetier-fhbg9.mongodb.net/test.myCollection")
			      .getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		
		String path = "gs://paresh_bucket/relationalData/fin_district.tsv";
		JavaRDD<String> lines = sc.textFile(path);
		JavaRDD<Document> districts = lines.map(x -> getDistrict(x));
		
		Map<String, String> writeOverrides1 = new HashMap<String, String>();
		writeOverrides1.put("database", "test");
	    writeOverrides1.put("collection", "districts");
	    writeOverrides1.put("writeConcern.w", "majority");
	    writeOverrides1.put("uri", "mongodb+srv://main_user:pare1212@freetier-fhbg9.mongodb.net/test");
	    WriteConfig writeConfig1 = WriteConfig.create(writeOverrides1);
	    MongoSpark.save(districts, writeConfig1);
		
		/**
         * Client collection
         */
		JavaRDD<String> clientLines = sc.textFile("gs://paresh_bucket/relationalData/fin_client.tsv");
		JavaRDD<String> dispLines = sc.textFile("gs://paresh_bucket/relationalData/fin_disp.tsv");
		JavaRDD<String> cardLines = sc.textFile("gs://paresh_bucket/relationalData/fin_card.tsv");
		
		JavaPairRDD<Integer, Client> clients = clientLines.mapToPair(x -> getClients(x));
		JavaPairRDD<Integer, Disp> disps = dispLines.mapToPair(x -> getDisps(x));
		JavaPairRDD<Integer, Card> cards = cardLines.mapToPair(x -> getCards(x));
		
		JavaPairRDD<Integer, Client> clientDisps = clients.leftOuterJoin(disps).
				mapToPair(x -> getClientDisps(x));
		
		JavaPairRDD<Integer, Client> clientCards = 
				       clientDisps.leftOuterJoin(cards).mapToPair(x -> getClientCards(x));
		
		
		
        /**
         * Account collection
         */
		JavaRDD<String> accLines = sc.textFile("gs://paresh_bucket/relationalData/fin_account.tsv");
		JavaRDD<String> loanLines = sc.textFile("gs://paresh_bucket/relationalData/fin_loan.tsv");
		JavaRDD<String> orderLines = sc.textFile("gs://paresh_bucket/relationalData//fin_order.tsv");
		JavaRDD<String> transLines = sc.textFile("gs://paresh_bucket/relationalData//fin_trans.tsv");

        JavaPairRDD<Integer, Account> accounts = accLines.mapToPair(x -> getAccount(x));
        JavaPairRDD<Integer, Iterable<Loan>> loans = loanLines.mapToPair(x -> getLoan(x)).groupByKey();
        JavaPairRDD<Integer, Iterable<Order>> orders = orderLines.mapToPair(x -> getOrder(x)).groupByKey();
        JavaPairRDD<Integer, Iterable<Transaction>> transactions = transLines.mapToPair(x -> getTrans(x)).groupByKey();
        
        JavaPairRDD<Integer, Tuple2<Account, Optional<Iterable<Loan>>>> accountLoans = accounts.leftOuterJoin(loans);
        JavaPairRDD<Integer, Account> loansToAccountaccount = accountLoans.mapToPair(x -> loansToAccount(x));        
        
        JavaPairRDD<Integer, Tuple2<Account, Optional<Iterable<Order>>>> accountOrders = loansToAccountaccount.leftOuterJoin(orders);
        JavaPairRDD<Integer, Account> ordersToAccount = accountOrders.mapToPair(x -> ordersToAccount(x));
        
        JavaPairRDD<Integer, Tuple2<Account, Optional<Iterable<Transaction>>>> accountTrans = ordersToAccount.leftOuterJoin(transactions);
        JavaPairRDD<Integer, Account> transToAccount = accountTrans.mapToPair(x -> transToAccount(x));
        //JavaRDD<Document> accountDocs = transToAccount.map(x -> getDocumentVal(x));

        
        /**
         * Join ClientAndAccount
         */
        JavaRDD<Document> finalClient = clientCards.leftOuterJoin(transToAccount)
        		.map(x -> getFinalClient(x));
        
        
        
        Map<String, String> writeOverrides = new HashMap<String, String>();
		writeOverrides.put("database", "test");
	    writeOverrides.put("collection", "clients");
	    writeOverrides.put("writeConcern.w", "majority");
	    writeOverrides.put("uri", "mongodb+srv://main_user:pare1212@freetier-fhbg9.mongodb.net/test");
	    WriteConfig writeConfig = WriteConfig.create(writeOverrides);
	    MongoSpark.save(finalClient, writeConfig);
	}
	
	private static Document getFinalClient(Tuple2<Integer, Tuple2<Client, Optional<Account>>> input) {
		Client client = input._2._1;
		if (input._2._2.isPresent()) {
			client.account = input._2._2.get();
		}
		Gson gson = new Gson();
		Document doc = Document.parse(gson.toJson(client));
	    return doc;
	}
	
	private static Tuple2<Integer, Client> getClientCards(Tuple2<Integer, Tuple2<Client, Optional<Card>>> input) {
		Client client = input._2._1();
		if (input._2._2.isPresent()) {
			client.card = input._2._2.get();
			
		}
		return new Tuple2<Integer, Client>(client.disp.account_id, client);
		
	}
	
	private static Tuple2<Integer, Client> getClientDisps(Tuple2<Integer, Tuple2<Client, Optional<Disp>>> input) {
		Client client = input._2._1();
		int dispId = 0;
		if (input._2._2.isPresent()) {
			client.disp = input._2._2.get();
			dispId = client.disp.disp_id;
		}
		return new Tuple2<Integer, Client>(dispId, client);
		
	}
	
	private static Tuple2<Integer, Client> getClients(String input) {
		String[] vals = input.split("\t");
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date date;
		Client client = null;
		Integer clientId = null; 
		
		try {
			   date = format.parse(vals[1]);
			   clientId = Integer.parseInt(vals[0]);
			   client = new Client(clientId, Integer.parseInt(vals[3]), 
					vals[2], date);
			} catch (Exception e) {
				return new Tuple2<Integer, Client>(0, new Client());
			}
	    return new Tuple2<Integer, Client>(clientId, client);
    }
	
    private static Tuple2<Integer, Disp> getDisps(String input) {
       	String[] vals = input.split("\t");

		Disp disp = null;
		Integer clientId = null; 
		
		try {
			   clientId = Integer.parseInt(vals[1]);
			   disp = new Disp(Integer.parseInt(vals[0]), clientId, 
					   Integer.parseInt(vals[2]), vals[3]);
			} catch (Exception e) {
				return new Tuple2<Integer, Disp>(0, new Disp());
			}
	    return new Tuple2<Integer, Disp>(clientId, disp);
    }
    
    private static Tuple2<Integer, Card> getCards(String input) {
    	    String[] vals = input.split("\t");
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date date;
		Card card = null;
		Integer dispId = null; 
		
		try {
			   date = format.parse(vals[3]);
			   dispId = Integer.parseInt(vals[1]);
			   card = new Card(Integer.parseInt(vals[0]), dispId, 
					vals[2], date);
			} catch (Exception e) {
				return new Tuple2<Integer, Card>(0, new Card());
			}
	    return new Tuple2<Integer, Card>(dispId, card);
    }
	
	
	
    private static Document getDocumentVal(Tuple2<Integer, Account> input) {
      	Gson gson = new Gson();
		Document doc = Document.parse(gson.toJson(input._2));
	    return doc;
    }
    
	private static Tuple2<Integer, Account> loansToAccount(Tuple2<Integer, Tuple2<Account, Optional<Iterable<Loan>>>> input) {
		Account acc = input._2._1;
		if (input._2._2.isPresent()) {
			List<Loan> loans = Lists.newArrayList(input._2._2.get());
			acc.loan = loans;
		}
		return new Tuple2<Integer,Account>(input._1, acc);	
	}
	
	private static Tuple2<Integer, Account> ordersToAccount(Tuple2<Integer, Tuple2<Account, Optional<Iterable<Order>>>> input) {
		Account acc = input._2._1;
		if (input._2._2.isPresent()) {
			List<Order> orders = Lists.newArrayList(input._2._2.get());
			acc.order = orders;
		}
		return new Tuple2<Integer,Account>(input._1, acc);	
	}
	
	private static Tuple2<Integer, Account> transToAccount(Tuple2<Integer, Tuple2<Account, Optional<Iterable<Transaction>>>> input) {
		Account acc = input._2._1;
		if (input._2._2.isPresent()) {
			List<Transaction> trans = Lists.newArrayList(input._2._2.get());
			acc.transaction = trans;
		}
		return new Tuple2<Integer,Account>(input._1, acc);	
	}
	
	private static Document getDistrict(String input) {
		String[] vals = input.split("\t");
		/*StringReader reader = new StringReader(input);
		CsvMapper m = new CsvMapper();
		CsvSchema schema = m.schemaFor(District.class).withoutHeader().withLineSeparator("\n").withColumnSeparator('\t');
		try {
		    MappingIterator<District> r = m.reader(District.class).with(schema).readValues(reader);
		    while (r.hasNext()) {
		        System.out.println(r.nextValue());
		    }
		    return r.nextValue();

		} catch (JsonProcessingException e) {
		    e.printStackTrace();
		} catch (IOException e) {
		    e.printStackTrace();
		}*/ 

		District d = null;
		try {
	    d = new District(Integer.parseInt(vals[0]), vals[1], vals[2], Long.parseLong(vals[3]), 
	    		Integer.parseInt(vals[4]), Integer.parseInt(vals[5]),
	    		Integer.parseInt(vals[6]),Integer.parseInt(vals[7]),Integer.parseInt(vals[8]),
	    		Double.parseDouble(vals[9]),Double.parseDouble(vals[10]),Double.parseDouble(vals[11]),
	    		Double.parseDouble(vals[12]),Integer.parseInt(vals[13]),Integer.parseInt(vals[14]),
	    		Integer.parseInt(vals[15]));
		} catch (NumberFormatException e) {
			return new Document();
		}
		
		Gson gson = new Gson();
		Document doc = Document.parse(gson.toJson(d));
	    return doc;
	    
		
	}
	
	private static Tuple2<Integer,Account> getAccount(String input) {
		String[] vals = input.split("\t");
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date date;
		Account acc = null;
		Integer accId = null; 
		
		try {
			   date = format.parse(vals[2]);
			   accId = Integer.parseInt(vals[0]);
			   acc = new Account(accId, Integer.parseInt(vals[1]), 
					date, vals[3]);
			} catch (Exception e) {
				return new Tuple2<Integer, Account>(0, new Account());
			}
	    return new Tuple2<Integer, Account>(accId, acc);
	}
	
	private static Tuple2<Integer, Loan> getLoan(String input) {
		String[] vals = input.split("\t");
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date date;
		Loan loan = null;
		Integer accId = null;
		try {
		   date = format.parse(vals[2]);
		   accId = Integer.parseInt(vals[1]);
		   loan = new Loan(Integer.parseInt(vals[0]), accId, 
				date, Double.parseDouble(vals[3]), Integer.parseInt(vals[4]), 
				Double.parseDouble(vals[5]), vals[6]);
		} catch (Exception e) {
			return new Tuple2<Integer, Loan>(0, new Loan());
		}
		return new Tuple2<Integer, Loan>(accId, loan);
	}
	
	private static Tuple2<Integer, Order> getOrder(String input) {
		String[] vals = input.split("\t");
		Order o;
		Integer accId;
		try {
			accId = Integer.parseInt(vals[1]);
			o = new Order(Integer.parseInt(vals[0]), accId, 
					vals[2], Integer.parseInt(vals[3]), Double.parseDouble(vals[4]), 
					vals[5]);
		} catch (NumberFormatException e) {
			return new Tuple2<Integer, Order>(0, new Order());
		}
		return new Tuple2<Integer, Order>(accId, o);
	}
	
	private static Tuple2<Integer,Transaction> getTrans(String input) {
		String[] vals = input.split("\t");
		Transaction t;
		Integer accId;
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Date date;
		try {
			accId = Integer.parseInt(vals[1]);
			date = format.parse(vals[2]);
			t = new Transaction(Integer.parseInt(vals[0]), accId, date, Double.parseDouble(vals[3]),
					Double.parseDouble(vals[4]), vals[5], vals[6], vals[7], vals[8], 
					Integer.parseInt(vals[9]));
			
			
		} catch (Exception e) {
			return new Tuple2<Integer, Transaction>(0, new Transaction());
		}
		
		return new Tuple2<Integer, Transaction>(accId, t);
	}
	
}

