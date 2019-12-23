package dataproc.demo.pojos;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Account implements Serializable{
	Integer account_id;
	Integer district_id;
	Date create_Date;
	String frequency;
	
	public List<Loan> loan;
	public List<Order> order;
	public List<Transaction> transaction;
	public Account(Integer account_id, Integer district_id, Date create_Date, String frequency, List<Loan> loan,
			List<Order> order, List<Transaction> transaction) {
		super();
		this.account_id = account_id;
		this.district_id = district_id;
		this.create_Date = create_Date;
		this.frequency = frequency;
		this.loan = loan;
		this.order = order;
		this.transaction = transaction;
	}
	
	public Account(Integer account_id, Integer district_id, Date create_Date, String frequency) {
		super();
		this.account_id = account_id;
		this.district_id = district_id;
		this.create_Date = create_Date;
		this.frequency = frequency;
		this.loan = new ArrayList<>();
		this.order = new ArrayList<>();
		this.transaction = new ArrayList<>();
	}
	
	public Account() {
		
	}

}
