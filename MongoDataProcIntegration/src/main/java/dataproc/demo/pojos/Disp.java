package dataproc.demo.pojos;
import java.io.Serializable;

public class Disp implements Serializable{
	public Integer disp_id;
	Integer client_id;
	public Integer account_id;
	String disp_type;
	
	public Disp(Integer disp_id, Integer client_id, Integer account_id, String disp_type) {
		super();
		this.disp_id = disp_id;
		this.client_id = client_id;
		this.account_id = account_id;
		this.disp_type = disp_type;
	}
	
	public Disp() {
		
	}

}
