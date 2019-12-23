package dataproc.demo.pojos;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "district_id", "district_name", "region", "num_inhabitants", "num_muncipalities_gt499", 
	"num_muncipalities_500to1999", "num_muncipalities_2000to9999", "num_muncipalities_gt10000", 
	"num_cities", "ratio_urban", "average_salary", "unemployment_rate95", "unemployment_rate96",
	"num_entrep_per1000", "num_crimes95", "num_crimes96"})
public class District {
	
	Integer district_id;
	String district_name;
	String region;
	Long num_inhabitants;
	Integer num_muncipalities_gt499;
	Integer num_muncipalities_500to1999;
	Integer num_muncipalities_2000to9999;
	Integer num_muncipalities_gt10000;
	Integer num_cities;
	Double ratio_urban;
	Double average_salary;
	Double unemployment_rate95;
	Double unemployment_rate96;
	int num_entrep_per1000;
	Integer num_crimes95;
	Integer num_crimes96;
	
	public District() {
		
	}
	public District(Integer district_id, String district_name, String region, Long num_inhabitants,
			Integer num_muncipalities_gt499, Integer num_muncipalities_500to1999, Integer num_muncipalities_2000to9999,
			Integer num_muncipalities_gt10000, Integer num_cities, Double ratio_urban, Double average_salary,
			Double unemployment_rate95, Double unemployment_rate96, int num_entrep_per1000, Integer num_crimes95,
			Integer num_crimes96) {
		super();
		this.district_id = district_id;
		this.district_name = district_name;
		this.region = region;
		this.num_inhabitants = num_inhabitants;
		this.num_muncipalities_gt499 = num_muncipalities_gt499;
		this.num_muncipalities_500to1999 = num_muncipalities_500to1999;
		this.num_muncipalities_2000to9999 = num_muncipalities_2000to9999;
		this.num_muncipalities_gt10000 = num_muncipalities_gt10000;
		this.num_cities = num_cities;
		this.ratio_urban = ratio_urban;
		this.average_salary = average_salary;
		this.unemployment_rate95 = unemployment_rate95;
		this.unemployment_rate96 = unemployment_rate96;
		this.num_entrep_per1000 = num_entrep_per1000;
		this.num_crimes95 = num_crimes95;
		this.num_crimes96 = num_crimes96;
	}
	public Integer getDistrict_id() {
		return district_id;
	}
	public void setDistrict_id(Integer district_id) {
		this.district_id = district_id;
	}
	public String getDistrict_name() {
		return district_name;
	}
	public void setDistrict_name(String district_name) {
		this.district_name = district_name;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public Long getNum_inhabitants() {
		return num_inhabitants;
	}
	public void setNum_inhabitants(Long num_inhabitants) {
		this.num_inhabitants = num_inhabitants;
	}
	public Integer getNum_muncipalities_gt499() {
		return num_muncipalities_gt499;
	}
	public void setNum_muncipalities_gt499(Integer num_muncipalities_gt499) {
		this.num_muncipalities_gt499 = num_muncipalities_gt499;
	}
	public Integer getNum_muncipalities_500to1999() {
		return num_muncipalities_500to1999;
	}
	public void setNum_muncipalities_500to1999(Integer num_muncipalities_500to1999) {
		this.num_muncipalities_500to1999 = num_muncipalities_500to1999;
	}
	public Integer getNum_muncipalities_2000to9999() {
		return num_muncipalities_2000to9999;
	}
	public void setNum_muncipalities_2000to9999(Integer num_muncipalities_2000to9999) {
		this.num_muncipalities_2000to9999 = num_muncipalities_2000to9999;
	}
	public Integer getNum_muncipalities_gt10000() {
		return num_muncipalities_gt10000;
	}
	public void setNum_muncipalities_gt10000(Integer num_muncipalities_gt10000) {
		this.num_muncipalities_gt10000 = num_muncipalities_gt10000;
	}
	public Integer getNum_cities() {
		return num_cities;
	}
	public void setNum_cities(Integer num_cities) {
		this.num_cities = num_cities;
	}
	public Double getRatio_urban() {
		return ratio_urban;
	}
	public void setRatio_urban(Double ratio_urban) {
		this.ratio_urban = ratio_urban;
	}
	public Double getAverage_salary() {
		return average_salary;
	}
	public void setAverage_salary(Double average_salary) {
		this.average_salary = average_salary;
	}
	public Double getUnemployment_rate95() {
		return unemployment_rate95;
	}
	public void setUnemployment_rate95(Double unemployment_rate95) {
		this.unemployment_rate95 = unemployment_rate95;
	}
	public Double getUnemployment_rate96() {
		return unemployment_rate96;
	}
	public void setUnemployment_rate96(Double unemployment_rate96) {
		this.unemployment_rate96 = unemployment_rate96;
	}
	public int getNum_entrep_per1000() {
		return num_entrep_per1000;
	}
	public void setNum_entrep_per1000(int num_entrep_per1000) {
		this.num_entrep_per1000 = num_entrep_per1000;
	}
	public Integer getNum_crimes95() {
		return num_crimes95;
	}
	public void setNum_crimes95(Integer num_crimes95) {
		this.num_crimes95 = num_crimes95;
	}
	public Integer getNum_crimes96() {
		return num_crimes96;
	}
	public void setNum_crimes96(Integer num_crimes96) {
		this.num_crimes96 = num_crimes96;
	}
	
	

}
