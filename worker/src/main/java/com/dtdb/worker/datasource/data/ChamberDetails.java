package com.dtdb.worker.datasource.data;

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

/**
 * @author Surya Pramodh
 * Entity Class to represent chamber details table.
 *
 */
@Entity
@Table(name = "chamber_details")
@NoArgsConstructor
public class ChamberDetails implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	@Getter
	@Setter
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO, generator = "chamber_details_id_seq")
	@SequenceGenerator(name = "chamber_details_id_seq", sequenceName = "chamber_details_id_seq")
	@Column(name = "id", unique = true, nullable = false)
	private Integer id;

	@Getter
	@Setter
	@Column(name = "chamber_name", nullable = false)
	private String name;

	@Getter
	@Setter
	@Column(name = "ip_address", nullable = false)
	private String ipAddress;

	@Getter
	@Setter
	@Column(name = "port", nullable = false)
	private Integer port;

	@Getter
	@Setter
	@Column(name = "user_name", nullable = false)
	private String user;

	@Getter
	@Setter
	@Column(name = "password", nullable = false)
	private String password;
	
	@Getter
	@Setter
	@Column(name = "database", nullable = false)
	private String database;
}
