package org.acme.kafka.streams.aggregator.streams;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import org.junit.jupiter.api.Test;

import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.SearchEntity;

public class JsonTest {

	@Test
	public void jsonTest()
	{
	       SearchEntity searchBE = new SearchEntity("SBE_DEF", "DEF check")
	                .addSort("PRI_NAME", "Created", SearchEntity.Sort.ASC)
	                .addFilter("PRI_CODE", SearchEntity.StringFilter.LIKE, "DEF_%")
	                .addColumn("PRI_CODE", "Name");

	        searchBE.setRealm("genny");
	        searchBE.setPageStart(0);
	        searchBE.setPageSize(1000);
	        
	//		BaseEntity searchBE = new BaseEntity("SBE_DEF","Search DEFs");
		
	        Jsonb jsonb = JsonbBuilder.create();
	        
	        String json = jsonb.toJson(searchBE);
	        
	        System.out.println(json);
	}
}
