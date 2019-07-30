package com.dtdb.worker.datasource;

/**
 * @author Surya Pramodh
 * This is a distributed database source. <br>
 * It creates data sources for all the chambers defined in chamber configuration table.
 */
public interface IVitreosDataSource {
	
	public void initialize();
	public String executeQuery(String sql);
	public void closeAll();
	public void reIniitialize();
}
