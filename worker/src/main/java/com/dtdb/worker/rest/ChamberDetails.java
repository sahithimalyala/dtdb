package com.dtdb.worker.rest;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ChamberDetails {
	private String name;
	private String ipAddress;
	private Integer port;
	private String user;
	private String password;
	private String database;
}
