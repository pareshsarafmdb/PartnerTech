package dataproc.demo.pojos;
import java.io.Serializable;
import java.util.Date;

public class Loan implements Serializable{
	Integer loan_id;
	Integer account_id;
	Date granted_date;
	Double amount;
	Integer duration;
	Double payments;
	String status;
	public Loan(Integer loan_id, Integer account_id, Date granted_date, Double amount, Integer duration,
			Double payments, String status) {
		super();
		this.loan_id = loan_id;
		this.account_id = account_id;
		this.granted_date = granted_date;
		this.amount = amount;
		this.duration = duration;
		this.payments = payments;
		this.status = status;
	}
	
	public Loan() {
		
	}
	
	@Override
    public String toString() { 
        return String.format(loan_id.toString() + "    " + account_id.toString()); 
    } 
	
	
	

}
