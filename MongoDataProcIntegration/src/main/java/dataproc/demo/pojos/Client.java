package dataproc.demo.pojos;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Client implements Serializable{
	
	Integer client_id;
	Integer district_id;
	String gender;
	Date birth_date;
	public Account account;
	
	public Disp disp;
	public Card card;
	public Client(Integer client_id, Integer district_id, String gender, Date birth_date, Disp disp, Card card) {
		super();
		this.client_id = client_id;
		this.district_id = district_id;
		this.gender = gender;
		this.birth_date = birth_date;
		this.disp = disp;
		this.card = card;
	}
	public Client(Integer client_id, Integer district_id, String gender, Date birth_date) {
		super();
		this.client_id = client_id;
		this.district_id = district_id;
		this.gender = gender;
		this.birth_date = birth_date;
		this.disp = new Disp();
		this.card = new Card();
	}
	
	public Client() {
		
	}
	
	
	

}
