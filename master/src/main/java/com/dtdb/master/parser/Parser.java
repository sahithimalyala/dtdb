package com.dtdb.master.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.dtdb.master.util.MultiSUMSeperator;

//@Component
public class Parser implements IParser {

	private String distributedQuery = "";
	private String consolidationQuery = "";
	private String finalQuery = "";
	
	private Map<String, Map<QueryType, String>> parserCache = new HashMap<String, Map<QueryType,String>>();

	private @Autowired IQueryGenerator generator;

	private void planQuery(String sqlText) {
		String query = sqlText.toUpperCase();
		String selectQuery = getSelectPart(query);
		if(parserCache.get(selectQuery)!=null){
			Map<QueryType,String> parsedData = parserCache.get(selectQuery);
			distributedQuery = parsedData.get(QueryType.DISTRIBUTED_QUERY);
			consolidationQuery = parsedData.get(QueryType.CONSOLIDATION_QUERY);
			finalQuery = parsedData.get(QueryType.FINAL_QUERY);
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

			if (part.split("/").length > 1 && part.split("SUM").length > 1) {
				for (String item : MultiSUMSeperator.items(part, "SUM")) {
					newPart = removeAlias(item);
					hashSet.add(newPart);
				}
			} else if (part.split("\\*").length > 1
					&& part.split("SUM").length > 1) {
				for (String item : MultiSUMSeperator.items(part, "SUM")) {
					newPart = removeAlias(item);
					hashSet.add(newPart);
				}
			} 
			 else {
				newPart = removeAlias(part);
				hashSet.add(newPart);
			}

		}

		String reWrittenPart = "";
		String postQuery = selectQuery;
		int i = 0;
		String consolidationQuery = "";
		Map<String, String> keys = new HashMap<String, String>();
		for (String str : hashSet) {
			String key = "key" + i++;
			;
			String replacementString = key;
			if (str.contains("SUM") || str.contains("AVG")
					|| str.contains("COUNT")) {
				replacementString = "SUM(" + replacementString + ")";
				/*
				 * consolidationQuery = consolidationQuery+replacementString;
				 * consolidationQuery = consolidationQuery+" AS "+key+", ";
				 */keys.put(key, replacementString);
				if (str.contains("AVG")) {
					replacementString = replacementString + "/SUM(count"+key+")";
				}
			} else {
				// consolidationQuery = key+/*" AS "+key+*/", ";
				keys.put(key, key);
			}
			postQuery = StringUtils.replace(postQuery, str.trim(),
					replacementString);
			reWrittenPart = reWrittenPart + ", " + str.trim() + " AS " + key;

		}

		consolidationQuery = consolidationQuery + " SUM(ckey) as ckey";
		keys.put("ckey", "ckey");
		if (reWrittenPart.endsWith(",")) {
			reWrittenPart = reWrittenPart.substring(0,
					reWrittenPart.length() - 1);
		}
		reWrittenPart = reWrittenPart + ", COUNT(1) AS ckey";

		if (reWrittenPart.startsWith(",")) {
			reWrittenPart = reWrittenPart.substring(1, reWrittenPart.length());
		}

		String reWrittenQuery = query.replace(selectQuery, reWrittenPart);
		distributedQuery = reWrittenQuery;
		finalQuery = postQuery;
		
		System.out.println(reWrittenQuery);
		System.out.println();
		System.out.println(distributedQuery);
		
		this.consolidationQuery = generator.generateQuery(distributedQuery,
				keys);
		
/*		Map<QueryType, String> plan = new HashMap<QueryType, String>();
		plan.put(QueryType.DISTRIBUTED_QUERY, distributedQuery);
		plan.put(QueryType.CONSOLIDATION_QUERY, consolidationQuery);
		plan.put(QueryType.FINAL_QUERY, finalQuery);
		parserCache.put(selectQuery, plan);*/
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

	private String removeAlias(String element) {
		element = element.trim();
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
		if (!part.startsWith("(") && part.endsWith(")")
				&& (part.indexOf("(") > 0 && part.indexOf("(") > part.length()))
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
		plan.put(QueryType.DISTRIBUTED_QUERY, distributedQuery);
		plan.put(QueryType.CONSOLIDATION_QUERY, consolidationQuery);
		plan.put(QueryType.FINAL_QUERY, finalQuery);
		return plan;
	}

	public static void mainMethod(String[] args) {
		System.out
				.println(new Parser()
						.parseQuery("SELECT CD.CHRONIC_ID,CD.CHRONIC_DESC CHRONIC_DESC,SUM(ENCOUNTERCOUNT) AS ENCOUNTERCOUNT ,COUNT(DISTINCT TEMP2.PATIENT_ID) AS PATIENTCOUNT ,AVG(HCCRISK) AS HCCRISK ,AVG(HCCCHRONIC) AS HCCCHRONIC ,AVG(NON_CLINICAL_RISK) AS NON_CLINICAL_RISK,AVG(ACCESS2CARE_RISK) AS ACCESS2CARE_RISK ,AVG(SOCIO_ECONOMIC_RISK) AS SOCIO_ECONOMIC_RISK ,(SUM(TOTAL_PMPM_COST)/33)/COUNT(DISTINCT TEMP2.PATIENT_ID) AS TOTAL_PMPM_COST ,SUM(TOTAL_COST) AS TOTAL_COST ,SUM(TOTAL_AMB_CLAIM_BILLED_AMT) AS TOTAL_AMB_CLAIM_BILLED_AMT ,CASE WHEN ( SUM(HOSP_ADMISSIONS_RATE) = 0 OR SUM(HOSP_ADMISSIONS_RATE) IS NULL ) THEN 0 WHEN SUM(HOSP_ADMISSIONS_RATE)>0 THEN (SUM(HOSP_ADMISSIONS_RATE) /COUNT(DISTINCT TEMP2.PATIENT_ID))*1000 END AS HOSP_ADMISSIONS_RATE,CASE WHEN ( SUM(HOSP_ADMISSIONS_RATE) = 0 OR SUM(HOSP_ADMISSIONS_RATE) IS NULL ) THEN 0 WHEN SUM(HOSP_ADMISSIONS_RATE)>0 THEN  (SUM(ER_ADMISSIONS_RATE) /SUM(HOSP_ADMISSIONS_RATE))*100 END AS ER_ADMISSIONS_RATE,CASE WHEN ( SUM(ER_VISITS_RATE) = 0 OR SUM(ER_VISITS_RATE) IS NULL ) THEN 0 WHEN SUM(ER_VISITS_RATE)>0 THEN (SUM(ER_VISITS_RATE) /COUNT(DISTINCT TEMP2.PATIENT_ID))*1000  END AS ER_VISITS_RATE,CASE WHEN ( SUM(HOSP_ADMISSIONS_RATE) = 0 OR SUM(HOSP_ADMISSIONS_RATE) IS NULL ) THEN 0 WHEN SUM(HOSP_ADMISSIONS_RATE)>0 THEN (SUM(RE_ADMISSIONS_RATE) /SUM(HOSP_ADMISSIONS_RATE))*100 END AS RE_ADMISSIONS_RATE,SUM(POST_ADMISSIONS_PCP_VISITS) AS POST_ADMISSIONS_PCP_VISITS ,SUM(POST_ADMISSIONS_SPECIALIST_VISITS) AS POST_ADMISSIONS_SPECIALIST_VISITS ,CASE WHEN ( SUM(HOSP_ADMISSIONS_RATE) = 0 OR SUM(HOSP_ADMISSIONS_RATE) IS NULL ) THEN 0 WHEN SUM(HOSP_ADMISSIONS_RATE)>0 THEN SUM(LOS_RATE)/SUM(HOSP_ADMISSIONS_RATE) END AS LOS_RATE FROM (  SELECT  PATIENT_ID PATIENT_ID,CHRONIC_ID, CHRONIC_INDICATOR ,SUM(ENCOUNTERCOUNT) AS ENCOUNTERCOUNT ,0 AS PATIENTCOUNT ,AVG(HCCRISK) AS HCCRISK ,AVG(HCCCHRONIC) AS HCCCHRONIC ,AVG(NON_CLINICAL_RISK) AS NON_CLINICAL_RISK ,AVG(ACC	ESS2CARE_RISK) AS ACCESS2CARE_RISK ,AVG(SOCIO_ECONOMIC_RISK) AS SOCIO_ECONOMIC_RISK ,SUM(TOTAL_PMPM_COST) AS TOTAL_PMPM_COST ,SUM(TOTAL_COST) AS TOTAL_COST ,SUM(TOTAL_AMB_CLAIM_BILLED_AMT) AS TOTAL_AMB_CLAIM_BILLED_AMT  ,COUNT(DISTINCT HOSP_ADMISSIONS_RATE) AS HOSP_ADMISSIONS_RATE ,SUM(ER_ADMISSIONS_RATE) AS ER_ADMISSIONS_RATE ,SUM(ER_VISITS_RATE) AS ER_VISITS_RATE ,SUM(RE_ADMISSIONS_RATE) AS RE_ADMISSIONS_RATE ,SUM(POST_ADMISSIONS_PCP_VISITS) AS POST_ADMISSIONS_PCP_VISITS ,SUM(POST_ADMISSIONS_SPECIALIST_VISITS) AS POST_ADMISSIONS_SPECIALIST_VISITS ,SUM(LOS_RATE) AS LOS_RATE  FROM (  SELECT PS.PATIENT_ID PATIENT_ID,CAST(NULL AS TEXT) CHRONIC_INDICATOR  ,CE.CHRONIC_ID AS CHRONIC_ID,COUNT(DISTINCT PS.ENCOUNTER_ID) AS ENCOUNTERCOUNT  ,0 AS PATIENTCOUNT ,AVG(CASE ONEYEAR_IND WHEN 0 THEN NULL WHEN 1 THEN HCCRISK END) AS HCCRISK ,AVG(CASE ONEYEAR_IND WHEN 0 THEN NULL WHEN 1 THEN HCCCHRONIC END) AS HCCCHRONIC ,AVG(NON_CLINICAL_RISK) AS NON_CLINICAL_RISK  ,AVG(ACCESS2CARE_RISK) AS ACCESS2CARE_RISK ,AVG(SOCIO_ECONOMIC_RISK) AS SOCIO_ECONOMIC_RISK ,(COALESCE(SUM(AMB_CLAIM_NETPAY), 0) + COALESCE(SUM(ACUTE_CLAIM_NETPAY), 0)) AS TOTAL_PMPM_COST  ,(COALESCE(SUM(AMB_CLAIM_NETPAY), 0) + COALESCE(SUM(ACUTE_CLAIM_NETPAY), 0)) AS TOTAL_COST  ,COALESCE(SUM(AMB_CLAIM_BILLED_AMT), 0) AS TOTAL_AMB_CLAIM_BILLED_AMT,MAX(HOSP_ADMISSIONS) AS HOSP_ADMISSIONS_RATE ,MAX(ER_ADMISSIONS) AS ER_ADMISSIONS_RATE ,MAX(ER_VISITS) AS ER_VISITS_RATE ,MAX(RE_ADMISSIONS) AS RE_ADMISSIONS_RATE ,MAX(POST_ADMISSIONS_PCP_VISITS) AS POST_ADMISSIONS_PCP_VISITS ,MAX(POST_ADMISSIONS_SPECIALIST_VISITS) AS POST_ADMISSIONS_SPECIALIST_VISITS ,MAX(LOS) AS LOS_RATE  FROM PATIENT_SUMMARY_FACT  PS  INNER JOIN CHRONIC_BY_ENCOUNTER CE ON PS.ENCOUNTER_ID=CE.ENCOUNTER_ID and ps.patient_id in (1,2,3,4,5,6,7,8,8,9,10) GROUP BY CE.CHRONIC_ID,PS.PATIENT_ID,PS.ENCOUNTER_ID, PS.PATIENT_ID,CE.CHRONIC_ID ) TEMP  GROUP BY PATIENT_ID,CHRONIC_ID,CHRONIC_INDICATOR ) TEMP2 INNER JOIN CHRONIC_DIM CD ON CD.CHRONIC_ID= TEMP2.CHRONIC_ID  GROUP BY CD.CHRONIC_ID,CD.CHRONIC_DESC"));

	}

}
