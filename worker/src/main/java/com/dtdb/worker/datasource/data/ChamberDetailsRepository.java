package com.dtdb.worker.datasource.data;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Component;

/**
 * @author Surya Pramodh
 * Repository class to perform CRUD operations on {@link ChamberDetails} entity
 */
@Component
public interface ChamberDetailsRepository extends
		PagingAndSortingRepository<ChamberDetails, Integer> {	
}
