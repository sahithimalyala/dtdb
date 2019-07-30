package com.dtdb.worker.rest;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class RefreshDetails {
	public List<ChamberDetails> chambers = new ArrayList<ChamberDetails>();
}
