package com.alibaba.middleware.race.engine;

/**
 * 
 * @author root
 *
 */
public class QueryMultiRowWhereTask extends QueryMultiRowTask {

		private WhereStrategy where;
		
		public QueryMultiRowWhereTask(Index index, String tablePath, Object key,WhereStrategy where) {
			super(index, tablePath, key);
			this.where = where;
		}
		
		protected boolean hook(Row row){
			if(where.inRange(row))
				return true;
			else
				return false;
		}
	}

