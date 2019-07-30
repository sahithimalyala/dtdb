package com.dtdb.master.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.dtdb.master.dispatcher.PlannerCache;
import com.dtdb.master.dispatcher.PlannerCacheRepository;
import com.dtdb.master.util.ColumnInterpreter;
import com.dtdb.master.util.MultiSUMSeperator;

@Component
public class DuplicateParser implements IParser {

	private @Autowired PlannerCacheRepository cacheRepository;
	private @Autowired JdbcTemplate jdbcTemplate;

	private String distributedQuery = "";
	private String consolidationQuery = "";
	private String finalQuery = "";

	private @Autowired IQueryGenerator generator;
	private Map<String, String> aliasMap = new HashMap<String, String>();

	private void planQuery(String sqlText) {
		String query = sqlText.toUpperCase().replace(";", "");
		/*
		 * String groupBy = ""; if(query.lastIndexOf("GROUP BY")>-1){ groupBy =
		 * query.substring(query.lastIndexOf("GROUP BY"), query.length()); }
		 */
		Boolean _isShardKeyLevelQuery = false;
		if(query.lastIndexOf("GROUP BY")>0){
			_isShardKeyLevelQuery = isShardKeyLevelQuery(query.substring(query.lastIndexOf("GROUP BY"), query.length()),query);
		}
		String selectQuery = getSelectPart(query);
		if (_isShardKeyLevelQuery) {
			String selects = jdbcTemplate.query(query, new ColumnInterpreter());
			query = "SELECT " + selects + " FROM (" + query + ") temp";
			sqlText = query;
		}

		String orderBy = "";
		if (sqlText.indexOf("ORDER BY") > 0) {
			orderBy = sqlText.substring(sqlText.indexOf("ORDER BY"), sqlText.length());
		}
		selectQuery = getSelectPart(query);
		if (orderBy.length() > 0) {
			parseOrderBy(orderBy, selectQuery);
		}
		Integer selectStartIndex = sqlText.indexOf(selectQuery);
		Integer selectEndIndex = selectStartIndex + selectQuery.length();

		finalQuery = selectQuery;
		if (cacheRepository.findByActualQuery(query) != null) {
			PlannerCache cache = cacheRepository.findByActualQuery(query);
			distributedQuery = cache.getDistributedQuery();
			consolidationQuery = cache.getConsolidationQuery();
			finalQuery = cache.getFinalQuery();
			return;
		}
		Set<String> hashSet = new HashSet<String>();

		if (selectQuery == null) {
			return;
		}

		String sParts[] = selectQuery.split(",");
		List<String> oldParts = Arrays.asList(sParts);
		List<String> selectParts = new ArrayList<String>();

		for (String part : oldParts) {
			selectParts.add(part);
		}

		for (String part : selectParts) {
			String newPart = "";
			if (part.split("/").length > 1
					&& (part.split("SUM").length > 1 || part.split("AVG").length > 1 || part.split("MAX").length > 1)) {
				for (String item : MultiSUMSeperator.items(part, "SUM")) {
					newPart = removeAlias(item);
					hashSet.add(newPart.trim());
				}
				for (String item : MultiSUMSeperator.items(part, "AVG")) {
					newPart = removeAlias(item);
					hashSet.add(newPart.trim());
				}
				for (String item : MultiSUMSeperator.items(part, "MAX")) {
					newPart = removeAlias(item);
					hashSet.add(newPart.trim());
				}
			} else if (part.split("\\*").length > 1 && part.split("SUM").length > 1) {
				for (String item : MultiSUMSeperator.items(part, "SUM")) {
					newPart = removeAlias(item);
					hashSet.add(newPart.trim());
				}
			} else {
				newPart = removeAlias(part);
				hashSet.add(newPart.trim());
			}
		}

		Map<String, String> originalKeyMap = new HashMap<String, String>();
		Map<String, String> keyRMap = new HashMap<String, String>();
		Map<String, String> keys = new HashMap<String, String>();
		Integer i = 0;
		// System.out.println(finalQuery);

		List<String> data = new ArrayList<String>();
		data.addAll(hashSet);
		Collections.sort(data, Collections.reverseOrder());

		for (String obj : data) {
			obj = obj.trim();
			String key = "key" + i++;
			// System.out.println("Object - "+obj+" "+"key -- "+key);
			if (obj.contains("AVG")) {
				String sum = obj.replace("AVG", "SUM");
				String count = obj.replace("AVG", "COUNT");
				String countKey = "count" + key;
				originalKeyMap.put(obj, key);
				finalQuery = finalQuery.replace(obj, "SUM(" + key + ")/SUM(" + countKey + ")");
				keyRMap.put(obj.replace("AVG", "SUM"), key);
				keyRMap.put(obj.replace("AVG", "COUNT"), countKey);
				keys.put(key, sum.replace(obj.substring(obj.lastIndexOf("(") + 1, obj.indexOf(")")), key));
				keys.put(countKey, count.replace(obj.substring(obj.lastIndexOf("(") + 1, obj.indexOf(")")), countKey)
						.replace("COUNT", "SUM"));
			} else {
				originalKeyMap.put(obj, key);
				keyRMap.put(obj, key);
				String objString = obj;
				if (obj.indexOf("(") > 0 && !obj.contains("CONCAT")) {
					objString = obj.substring(obj.lastIndexOf("(") + 1, obj.indexOf(")"));
				}
				keys.put(key, obj.replace(objString, key));
				if (!obj.trim().equalsIgnoreCase("0")) {
					finalQuery = finalQuery.replace(obj, obj.replace(objString, key));
				}

			}
		}
		Iterator<Entry<String, String>> keysIterator = keyRMap.entrySet().iterator();
		String keyRMapString = "";
		while(keysIterator.hasNext()){
			Entry<String, String> entry = keysIterator.next();
			keyRMapString = keyRMapString+" "+entry.getKey()+" as "+entry.getValue()+",";
		}
		keyRMapString = keyRMapString.trim().substring(0, keyRMapString.length()-2);
		distributedQuery = sqlText.substring(0, selectStartIndex)
				+ keyRMapString
				+ sqlText.substring(selectEndIndex);
		
		if(distributedQuery.contains("~* '^")){
			distributedQuery = distributedQuery.replace("FRM", "FROM").replace(orderBy, "");
		}else{
			distributedQuery = distributedQuery.replace("^", ",").replace("FRM", "FROM").replace(orderBy, "");
		}
		
		if (distributedQuery.endsWith(","))
			distributedQuery = distributedQuery.substring(0, distributedQuery.length() - 1);

		// System.out.println(distributedQuery);

		this.consolidationQuery = generator.generateQuery(distributedQuery, keys);
		if (consolidationQuery.endsWith(","))
			consolidationQuery = consolidationQuery.substring(0, consolidationQuery.length() - 1);
		if (finalQuery.endsWith(","))
			finalQuery = finalQuery.substring(0, finalQuery.length() - 1);
		
		if(finalQuery.contains("~* '^")){
			finalQuery = finalQuery.replace("FRM", "FROM").replace("COUNT", "SUM");
		}else{
			finalQuery = finalQuery.replace("^", ",").replace("FRM", "FROM").replace("COUNT", "SUM");
		}
		
		/*
		 * if(groupBy.contains("PATIENT_ID")){ finalQuery =
		 * finalQuery.replace("SUM", ""); }
		 */
		if (_isShardKeyLevelQuery) {
			consolidationQuery = consolidationQuery.substring(0, consolidationQuery.indexOf("GROUP BY"));
		}

		System.out.println(aliasMap);
		System.out.println(keyRMap);
		Iterator<Entry<String, String>> aliasEntries = aliasMap.entrySet().iterator();
		while (aliasEntries.hasNext()) {
			Entry<String, String> aliasEntry = aliasEntries.next();
			String alias = aliasEntry.getKey();
			String actualElement = aliasEntry.getValue();

			String keyString = keyRMap.get(actualElement);
			if (keyString == null) {
				keyString = keyRMap.get(alias);
			}

			if (keyString == null)
				continue;
			orderBy = orderBy.replace(actualElement, keyString);
			orderBy = orderBy.replace(alias, keyString);
			finalQuery = finalQuery.replace(actualElement.replace("COUNT", "SUM"), keyString);
			finalQuery= finalQuery.replace(alias.replace("COUNT", "SUM"), keyString);
		}
		System.out.println(orderBy);


		consolidationQuery = consolidationQuery + " " + orderBy;
		PlannerCache cache = new PlannerCache();
		cache.setActualQuery(query);
		if(consolidationQuery.contains("~* '^")){
			cache.setConsolidationQuery(consolidationQuery.replace("FRM", "FROM"));
		}else{
			cache.setConsolidationQuery(consolidationQuery.replace("^", ",").replace("FRM", "FROM"));
		}
		if(distributedQuery.contains("~* '^")){
			cache.setDistributedQuery(distributedQuery.replace("FRM", "FROM").replace(orderBy, ""));	
		}else{
			cache.setDistributedQuery(distributedQuery.replace("^", ",").replace("FRM", "FROM").replace(orderBy, ""));	
		}
		//cache.setConsolidationQuery(consolidationQuery.replace("^", ",").replace("FRM", "FROM"));
		//cache.setDistributedQuery(distributedQuery.replace("^", ",").replace("FRM", "FROM").replace(orderBy, ""));
		cache.setFinalQuery(finalQuery);
		cacheRepository.save(cache);

		// parserCache.put(selectQuery, plan);

	}

	private String getSelectPart(String sql) {
		String selectQuery = "";
		Pattern pattern = Pattern.compile("SELECT (.*?) FROM");
		Matcher matcher = pattern.matcher(sql);
		while (matcher.find()) {
			selectQuery = matcher.group(1);
			break;
		}
		return selectQuery;
	}

	private List<String> splitMathematical(String part) {
		List<String> parts = new ArrayList<String>();
		part = clearElements(part);
		if (!part.contains("/")) {
			parts.add(removeAlias(part));
		} else {
			String[] elements = part.split("/");
			for (String element : elements) {
				String ele = clearElements(element);
				try {
					Integer i = Integer.parseInt(ele);
					if (i > 0)
						continue;
				} catch (Exception ex) {
					parts.addAll(splitMathematical(ele));
				}

			}
		}
		if (!part.contains("\\*")) {
			parts.add(removeAlias(part));
		} else {
			String[] elements = part.split("*");
			for (String element : elements) {
				String ele = clearElements(element);
				try {
					Integer i = Integer.parseInt(ele);
					if (i > 0)
						continue;
				} catch (Exception ex) {
					parts.addAll(splitMathematical(ele));
				}

			}
		}

		return parts;
	}

	private void parseOrderBy(String orderBy, String selectPart) {
		String elements[] = selectPart.split(",");
		for (String element : elements) {
			element = element.trim();
			if (element.contains("AS")) {
				String parts[] = element.split("AS");
				aliasMap.put(parts[1].trim(), parts[0].trim());
			} else {
				String parts[] = element.split(" ");
				if (parts.length == 2) {
					aliasMap.put(parts[1].trim(), parts[0].trim());
				} else {
					aliasMap.put(element, element);
				}
			}
		}
		System.out.println(aliasMap);
	}

	private String removeAlias(String element) {
		element = element.trim();
		String alias = "";
		Integer indexOf = element.lastIndexOf(" ");
		if (indexOf > -1) {
			element = element.substring(0, indexOf);
		}

		if (element.trim().endsWith("AS")) {
			element = element.substring(0, element.trim().lastIndexOf("AS"));
		}

		return element;
	}

	private String clearElements(String part) {
		if (part.startsWith(")"))
			part = part.substring(1, part.length());
		if (part.startsWith("(") && !part.endsWith(")"))
			part = part.substring(1, part.length());
		if (!part.startsWith("(") && part.endsWith(")") && (part.indexOf("(") > 0 && part.indexOf("(") > part.length()))
			part = part.substring(0, part.length() - 1);
		if (part.endsWith(")") && part.indexOf("(") < 0)
			part = part.substring(0, part.length() - 1);
		if (part.endsWith("))"))
			part = part.substring(0, part.length() - 1);
		if (part.startsWith("(") && part.endsWith("))"))
			part = clearElements(part);
		return part;
	}

	@Override
	public Map<QueryType, String> parseQuery(String query) {
		planQuery(query);
		Map<QueryType, String> plan = new HashMap<QueryType, String>();
		if(distributedQuery.contains("~* '^")){
			plan.put(QueryType.DISTRIBUTED_QUERY, distributedQuery.replace("FRM", "FROM"));
		}else{
			plan.put(QueryType.DISTRIBUTED_QUERY, distributedQuery.replace("^", ",").replace("FRM", "FROM"));
		}
		//plan.put(QueryType.DISTRIBUTED_QUERY, distributedQuery.replace("^", ",").replace("FRM", "FROM"));
		plan.put(QueryType.CONSOLIDATION_QUERY, consolidationQuery.replace("^", ",").replace("FRM", "FROM"));
		plan.put(QueryType.FINAL_QUERY, finalQuery.replace("^", ",").replace("FRM", "FROM"));
		return plan;
	}

	public static void main(String[] args) {
		System.out.println(new DuplicateParser().parseQuery(
				"SELECT B.XMINVAL XMINVAL, B.XMAXVAL XMAXVAL, B.YMINVAL YMINVAL, B.YMAXVAL YMAXVAL, COUNT(PATIENT_ID) ,AVG(TOTAL_PMPM_COST) TOTAL_PMPM_COST,SUM(TOTAL_COST) TOTAL_COST,AVG(HCCRISK) HCCRISK FROM ( SELECT PATIENT_ID, (SUM(TOTAL_PMPM_COST)/33)/COUNT(DISTINCT TEMP.PATIENT_ID) AS TOTAL_PMPM_COST ,SUM(TOTAL_COST) TOTAL_COST ,AVG(HCCRISK) AS HCCRISK  FROM (  SELECT PS.PATIENT_ID,(COALESCE(SUM(AMB_CLAIM_NETPAY), 0) + COALESCE(SUM(ACUTE_CLAIM_NETPAY), 0)) TOTAL_PMPM_COST ,(COALESCE(SUM(AMB_CLAIM_NETPAY), 0) + COALESCE(SUM(ACUTE_CLAIM_NETPAY), 0)) TOTAL_COST    ,AVG(CASE ONEYEAR_IND WHEN 0 THEN NULL WHEN 1 THEN HCCRISK END) AS HCCRISK  FROM  PATIENT_SUMMARY_FACT PS   GROUP BY PS.PATIENT_ID,PS.ENCOUNTER_ID   )TEMP GROUP BY PATIENT_ID ) A RIGHT OUTER JOIN  (SELECT  X.MINVAL XMINVAL,X.MAXVAL XMAXVAL,  Y.MINVAL YMINVAL,Y.MAXVAL YMAXVAL  FROM  (SELECT 0 AS MINVAL, 500 AS MAXVAL UNION SELECT 500 AS MINVAL, 10671 AS MAXVAL) X ,  (SELECT 0 AS MINVAL, 1 AS MAXVAL UNION SELECT 1 AS MINVAL, 7 AS MAXVAL) Y  ) B ON (COALESCE(A.TOTAL_PMPM_COST,0)) >= B.XMINVAL AND (COALESCE(A.TOTAL_PMPM_COST,0)) < B.XMAXVAL AND (COALESCE(A.HCCRISK,0)) >= B.YMINVAL AND (COALESCE(A.HCCRISK,0)) < B.YMAXVAL GROUP BY B.XMAXVAL,B.YMAXVAL,B.XMINVAL,B.YMINVAL ORDER BY YMINVAL,YMAXVAL,XMINVAL,XMAXVAL"));

	}

	private Boolean isShardKeyLevelQuery(String groupBy,String query) {
		Integer openCounts = StringUtils.countMatches(groupBy,"(");
		Integer closeCounts = StringUtils.countMatches(groupBy, ")");
		/*if (openCounts == 0 && closeCounts == 0)
			return groupBy.contains("PATIENT_ID");*/
		
		if(query.contains("'SUMMARY'::")){	
			return false;
		}

		return groupBy.contains("PATIENT_ID") && openCounts > closeCounts;
	}

}
