package com.alibaba.middleware.race;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystem.TypeException;

import com.alibaba.middleware.race.engine.Row;

/**
 * 
 * @author root
 *
 */
public class ResultImp implements Result{
	
    @Override
	public String toString() {
		return "ResultImp [orderId=" + orderId + ", row=" + row + "]";
	}

	private long orderId;
    private Row row;

    private ResultImp(long orderId, Row row) {
        this.orderId = orderId;
        this.row = row;
      }

    public static ResultImp createResultRow(Row orderData, Row buyerData,
            Row goodData, Collection<String> queryingKeys) {
          if (orderData == null) {
            throw new RuntimeException("Bad data!");
          }
          Row row = new Row();
          long orderid;
          try {
            orderid = orderData.get("orderid").valueAsLong();
          } catch (TypeException e) {
            throw new RuntimeException("Bad data!");
          }

          for (KV kv : orderData.getValues()) {
            if (queryingKeys == null || queryingKeys.contains(kv.key())) {
              row.put(kv.key(), kv.valueAsString());
            }
          }
          
          if(buyerData != null){
        	  for (KV kv : buyerData.getValues()) {
        		  if (queryingKeys == null || queryingKeys.contains(kv.key())) {
        			  row.put(kv.key(), kv.valueAsString());
        		  }
        	  }
          }
          if(goodData != null){
        	  for (KV kv : goodData.getValues()) {
        		  if (queryingKeys == null || queryingKeys.contains(kv.key())) {
        			  row.put(kv.key(), kv.valueAsString());
        		  }
        	  }
          }
          return new ResultImp(orderid, row);
        }
    
    public static ResultImp createResultRow(Row orderData, Row buyerData,
            Row goodData) {
          if (orderData == null) {
            throw new RuntimeException("Bad data!");
          }
          Row resultRow = new Row();
          long orderid;
          try {
            orderid = orderData.get("orderid").valueAsLong();
          } catch (TypeException e) {
            throw new RuntimeException("Bad data!");
          }

          resultRow.putAll(orderData.getAll());
          
          if(buyerData != null)
        	  resultRow.putAll(buyerData.getAll());
          
          if(goodData != null)
        	  resultRow.putAll(goodData.getAll());
          
          return new ResultImp(orderid, resultRow);
        }
    
    
    public static ResultImp createNoRow(Long orderid){
    	return new ResultImp(orderid, new Row());
    }

	@Override
	public KeyValue get(String key) {
		return this.row.get(key);
	}

	@Override
	public KeyValue[] getAll() {
		return this.row.getValues().toArray(new KeyValue[0]);
	}

	@Override
	public long orderId() {
		return this.orderId;
	}

}
