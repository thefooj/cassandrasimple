package com.tioscapital.cassandrasimple;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Hashtable;
import java.util.Date;
import com.tioscapital.cassandrasimple.CassandraColumn;


public class CassandraReader {





  List<CassandraColumn> columns;
  Hashtable<String, Integer> colNameToIndexLookup;


  public CassandraReader(Session cassSess, String cqlQuery, int estimatedRows, boolean debug) {
    ResultSet rs =  cassSess.execute(cqlQuery);


    if (debug) { System.out.println("DONE with query... now getting column mappings... "); }
    this.initializeColumnMappings(rs, estimatedRows, debug);
    if (debug) { System.out.println("Column Mapping: " + colNameToIndexLookup.toString()); }

    if (debug) {
      for (int i = 0; i < columns.size(); i++) {
        ((CassandraColumn)(columns.get(i))).printDebug();
      }
    }

    if (debug) { System.out.println("START fetching data in Java"); }
    this.loadData(rs);
    if (debug) { System.out.println("DONE fetching data in Java"); }
  }

  public int getNumColumns() {
    return(columns.size());
  }

  /* i is the 0-based index */
  public CassandraColumn getColumn(int i) {
    return((CassandraColumn)(columns.get(i)));
  }

  private void loadData(ResultSet rs) {
    int i;
    int rowNum = 0;
    while (!rs.isExhausted()) {
      rowNum++;
      Row row = rs.one();
      for (i = 0; i < columns.size(); i++) {
        ((CassandraColumn)(columns.get(i))).addFromRow(row);
      }
    }
  }

  private void initializeColumnMappings(ResultSet rs, int estimatedRows, boolean debug) {
    ColumnDefinitions cdefs = rs.getColumnDefinitions();
    int numCols = cdefs.size();
    String cName;

    this.columns = new ArrayList<CassandraColumn>(numCols);
    this.colNameToIndexLookup = new Hashtable<String, Integer>();

    for (int i=0; i < numCols; i++) {
      cName = cdefs.getName(i);
      if (debug) { System.out.println("Got col " + cName); }

      // String colName, String colCassTypeStr, int cassColIdx, int estimatedSize
      CassandraColumn col = new CassandraColumn(cName, cdefs.getType(i).asFunctionParameterString(), i, estimatedRows, debug);
      if (debug) { System.out.println("Done with col setup: " + cName); }
      this.columns.add(col);
      this.colNameToIndexLookup.put(cName, i);
    }
  }


}