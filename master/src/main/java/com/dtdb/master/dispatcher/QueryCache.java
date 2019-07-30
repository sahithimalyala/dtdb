package com.dtdb.master.dispatcher;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "query_cache")
@NoArgsConstructor
public class QueryCache implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	@Getter
	@Setter
	@Id
	@Column(name = "id", unique = true, nullable = false)
	@GeneratedValue(strategy = GenerationType.AUTO, generator = "query_cache_id_seq")
	@SequenceGenerator(name = "query_cache_id_seq", sequenceName = "query_cache_id_seq")
	private Integer id;
	

	@Getter
	@Setter
	@Column(name = "query", nullable = false, unique = true)
	private String actualQuery;
	
	@Getter
	@Setter
	@Column(name = "result_data", nullable = false)
	private String result;
	
}
