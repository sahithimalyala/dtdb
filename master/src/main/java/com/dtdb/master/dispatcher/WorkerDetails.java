package com.dtdb.master.dispatcher;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
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
@Table(name = "worker_details")
@NoArgsConstructor
public class WorkerDetails implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	@Getter
	@Setter
	@Id
	@GeneratedValue
	@Column(name = "id", unique = true, nullable = false)
	private Integer id;

	@Getter
	@Setter
	@Column(name = "worker_name", nullable = false)
	private String name;

	@Getter
	@Setter
	@Column(name = "ip_address", nullable = false)
	private String ipAddress;

	@Getter
	@Setter
	@Column(name = "port", nullable = false)
	private Integer port;

}
