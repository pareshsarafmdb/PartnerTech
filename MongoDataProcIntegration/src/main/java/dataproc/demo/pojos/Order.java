package dataproc.demo.pojos;
import java.io.Serializable;

public class Order implements Serializable{
	Integer order_id;
	Integer account_id;
	String bank_to;
	Integer account_to;
	Double amount;
	String category;
	public Order(Integer order_id, Integer account_id, String bank_to, Integer account_to, Double amount,
			String category) {
		super();
		this.order_id = order_id;
		this.account_id = account_id;
		this.bank_to = bank_to;
		this.account_to = account_to;
		this.amount = amount;
		this.category = category;
	}
	
	public Order() {
		
	}

}
