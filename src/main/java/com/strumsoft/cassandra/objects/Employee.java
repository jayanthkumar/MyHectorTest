package com.strumsoft.cassandra.objects;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.persistence.Entity;
import javax.persistence.Table;

import me.prettyprint.hom.annotations.Column;
import me.prettyprint.hom.annotations.Id;

import com.google.common.base.Objects;
import com.strumsoft.cassandra.converters.MapConverter;

@Entity
@Table(name="employee")
public class Employee {
	@Id
	private Long empId;
	@Column(name="name")
	private String name;
	@Column(name="gender")
	private String gender;
	@Column(name="mailId")
	private String mailId;
	@Column(name="dob")
	private Date dob;
	@Column(name="salary")
	private Long salary;
	@me.prettyprint.hom.annotations.Column(name="projectGroup", converter=com.strumsoft.cassandra.converters.CollectionConverter.class)
	private List<String> projectGroup;
	@me.prettyprint.hom.annotations.Column(name="projectInfo", converter=MapConverter.class)
	private Map<String,String> projectInfo;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Long getEmpId() {
		return empId;
	}
	public void setEmpId(Long empId) {
		this.empId = empId;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getMailId() {
		return mailId;
	}
	public void setMailId(String mailId) {
		this.mailId = mailId;
	}
	public Date getDob() {
		return dob;
	}
	public void setDob(Date dob) {
		this.dob = dob;
	}
	public Long getSalary() {
		return salary;
	}
	public void setSalary(Long salary) {
		this.salary = salary;
	}
	public List<String> getProjectGroup() {
		return projectGroup;
	}
	public void setProjectGroup(List<String> projectGroup) {
		this.projectGroup = projectGroup;
	}
	public Map<String, String> getProjectInfo() {
		return projectInfo;
	}
	public void setProjectInfo(Map<String, String> projectInfo) {
		this.projectInfo = projectInfo;
	}
	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("empId", getEmpId())
				.add("Name", getName()).add("Gender", getGender())
				.add("MailId", getMailId()).add("Dob", getDob())
				.add("Salary", getSalary())
				.add("Projectgroup", getProjectGroup())
				.add("ProjectInfo", getProjectInfo()).toString();
	}
	@Override
	public boolean equals(Object obj) {
		if(null == obj || obj.getClass() != this.getClass() ){
			return false;
		}
		if(this == obj) {
			return true;
		}
		Employee emp = (Employee)obj;
		return Objects.equal(this.getEmpId(), emp.getEmpId());
	}
}
