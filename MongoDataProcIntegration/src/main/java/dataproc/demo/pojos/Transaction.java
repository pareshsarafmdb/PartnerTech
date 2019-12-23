package dataproc.demo.pojos;
import java.io.Serializable;
import java.util.Date;

public class Transaction implements Serializable{
	Integer transaction_id;
	Integer account_id;
	Date trans_date;
	Double amount;
	Double balance;
	String trans_type;
	String operation;
	String category;
	String other_bank_id;
	Integer other_account_id;
	public Transaction(Integer transaction_id, Integer account_id, Date trans_date, Double amount, Double balance,
			String trans_type, String operation, String category, String other_bank_id, Integer other_account_id) {
		super();
		this.transaction_id = transaction_id;
		this.account_id = account_id;
		this.trans_date = trans_date;
		this.amount = amount;
		this.balance = balance;
		this.trans_type = trans_type;
		this.operation = operation;
		this.category = category;
		this.other_bank_id = other_bank_id;
		this.other_account_id = other_account_id;
	}
	
	public Transaction() {
		
	}
	
	

}
