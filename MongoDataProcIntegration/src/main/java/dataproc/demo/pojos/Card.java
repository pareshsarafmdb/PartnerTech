package dataproc.demo.pojos;
import java.io.Serializable;
import java.util.Date;

public class Card implements Serializable{
	Integer card_id;
	Integer disp_id;
	String card_type;
	Date issued_date;
	
	public Card(Integer card_id, Integer disp_id, String card_type, Date issued_date) {
		super();
		this.card_id = card_id;
		this.disp_id = disp_id;
		this.card_type = card_type;
		this.issued_date = issued_date;
	}

	public Card() {
		
	}
}
