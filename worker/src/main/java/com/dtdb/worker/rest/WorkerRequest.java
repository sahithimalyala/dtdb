package com.dtdb.worker.rest;

import lombok.Data;

@Data
public class WorkerRequest {
	private String distributedQuery;
	private String consolidationQuery;
}
