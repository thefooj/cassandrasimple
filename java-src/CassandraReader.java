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


public class CassandraReader {




  public abstract class CassColumn {
    String colName;
    int colIdx;

    public CassColumn(String colName, int colIdx, int estimatedSize) {
      this.colName = colName;
      this.colIdx = colIdx;
    }

    public abstract void addFromRow(Row row);

    public int getColIdx() { return this.colIdx; }

    public String getColName() { return this.colName; }

    public void printDebug() {
      System.out.println(this.getClass().getSimpleName() + " for " + this.colName + " at idx " + this.colIdx);
    }

    public Boolean[] getBoolData() { return(null);  }
    public Float[] getFloatData() { return(null);  }
    public Integer[] getIntData() { return(null);  }
    public Double[] getDoubleData() { return(null);  }
    public Long[] getLongData() { return(null);  }
    public String[] getStringData() { return(null); }

    public abstract String getDataMethodName();
    public abstract String getDataJniType();
    public abstract String getDataRConverter();


  }

  public class CassStringColumn extends CassColumn {

    List<String> data;

    public CassStringColumn(String colName, int colIdx, int estimatedSize) {
      super(colName, colIdx, estimatedSize);
      data = new ArrayList<String>(estimatedSize);
    }

    public void addFromRow(Row row) {
      if (row.isNull(this.colIdx)) {
        data.add(null);
      } else {
        data.add(row.getString(this.colIdx));
      }
    }

    public String getDataMethodName() { return("getStringData"); }
    public String getDataJniType()    { return("[Ljava/lang/String;"); }
    public String getDataRConverter() { return(null); }
    public String[] getStringData() {
      String[] retArr = data.toArray(new String[data.size()]);
      return(retArr);
    }
  }

  public class CassDateColumn extends CassColumn {

    List<String> data;

    public CassDateColumn(String colName, int colIdx, int estimatedSize) {
      super(colName, colIdx, estimatedSize);
      data = new ArrayList<String>(estimatedSize);
    }

    public void addFromRow(Row row) {
      if (row.isNull(this.colIdx)) {
        data.add(null);
      } else {
        data.add(row.getDate(this.colIdx).toString());  // yyyy-mm-dd
      }
    }

    public String getDataMethodName() { return("getStringData"); }
    public String getDataJniType()    { return("[Ljava/lang/String;"); }
    public String getDataRConverter() { return("as_date_from_cql_date"); }
    public String[] getStringData() {
      String[] retArr = data.toArray(new String[data.size()]);
      return(retArr);
    }

  }

  public class CassIntColumn extends CassColumn {
    List<Integer> data;

    public CassIntColumn(String colName, int colIdx, int estimatedSize) {
      super(colName, colIdx, estimatedSize);
      data = new ArrayList<Integer>(estimatedSize);
    }

    public void addFromRow(Row row) {
      if (row.isNull(this.colIdx)) {
        data.add(null);
      } else {
        data.add(new Integer(row.getInt(this.colIdx)));
      }
    }

    public String getDataMethodName() { return("getIntData"); }
    public String getDataJniType()    { return("[Ljava/lang/Integer;"); }
    public String getDataRConverter() { return("as.integer"); }
    public Integer[] getIntData() {
      Integer[] retArr = data.toArray(new Integer[data.size()]);
      return(retArr);
    }
  }

  public class CassTinyintColumn extends CassColumn {
    List<Byte> data;

    public CassTinyintColumn(String colName, int colIdx, int estimatedSize) {
      super(colName, colIdx, estimatedSize);
      data = new ArrayList<Byte>(estimatedSize);
    }

    public void addFromRow(Row row) {
      if (row.isNull(this.colIdx)) {
        data.add(null);
      } else {
        data.add(new Byte(row.getByte(this.colIdx)));
      }
    }

    public String getDataMethodName() { return("getByteData"); }
    public String getDataJniType()    { return("[Ljava/lang/Byte;"); }
    public String getDataRConverter() { return("as.integer"); }
    public Byte[] getByteData() {
      Byte[] retArr = data.toArray(new Byte[data.size()]);
      return(retArr);
    }

  }

  public class CassLongColumn extends CassColumn {
    List<Long> data;

    public CassLongColumn(String colName, int colIdx, int estimatedSize) {
      super(colName, colIdx, estimatedSize);
      data = new ArrayList<Long>(estimatedSize);
    }

    public void addFromRow(Row row) {
      if (row.isNull(this.colIdx)) {
        data.add(null);
      } else {
        data.add(new Long(row.getLong(this.colIdx)));
      }
    }

    public String getDataMethodName() { return("getLongData"); }
    public String getDataJniType()    { return("[Ljava/lang/Long;"); }
    public String getDataRConverter() { return("as.integer"); }
    public Long[] getLongData() {
      Long[] retArr = data.toArray(new Long[data.size()]);
      return(retArr);
    }

  }

  public class CassTimeColumn extends CassLongColumn {
    public CassTimeColumn(String colName, int colIdx, int estimatedSize) {
      super(colName, colIdx, estimatedSize);
    }

    public void addFromRow(Row row) {
      if (row.isNull(this.colIdx)) {
        data.add(null);
      } else {
        data.add(new Long(row.getTimestamp(this.colIdx).getTime() / 1000));  // seconds since epoch 1970-01-01 UTC
      }
    }

    public String getDataMethodName() { return("getLongData"); }
    public String getDataJniType()    { return("[Ljava/lang/Long;"); }
    public String getDataRConverter() { return("as_posixct_from_1970_epoch_seconds"); }
    public Long[] getLongData() {
      Long[] retArr = data.toArray(new Long[data.size()]);
      return(retArr);
    }
  }

  public class CassFloatColumn extends CassColumn {
    List<Float> data;

    public CassFloatColumn(String colName, int colIdx, int estimatedSize) {
      super(colName, colIdx, estimatedSize);
      data = new ArrayList<Float>(estimatedSize);
    }

    public void addFromRow(Row row) {
      if (row.isNull(this.colIdx)) {
        data.add(null);
      } else {
        data.add(new Float(row.getFloat(this.colIdx)));
      }
    }

    public String getDataMethodName() { return("getFloatData"); }
    public String getDataJniType()    { return("[Ljava/lang/Float;"); }
    public String getDataRConverter() { return("as.numeric"); }
    public Float[] getFloatData() {
      Float[] retArr = data.toArray(new Float[data.size()]);
      return(retArr);
    }
  }

  public class CassDoubleColumn extends CassColumn {
    List<Double> data;

    public CassDoubleColumn(String colName, int colIdx, int estimatedSize) {
      super(colName, colIdx, estimatedSize);
      data = new ArrayList<Double>(estimatedSize);
    }


    public void addFromRow(Row row) {
      if (row.isNull(this.colIdx)) {
        data.add(null);
      } else {
        data.add(new Double(row.getDouble(this.colIdx)));
      }
    }

    public String getDataMethodName() { return("getDoubleData"); }
    public String getDataJniType()    { return("[Ljava/lang/Double;"); }
    public String getDataRConverter() { return("as.numeric"); }
    public Double[] getDoubleData() {
      Double[] retArr = data.toArray(new Double[data.size()]);
      return(retArr);
    }
  }

  public class CassBooleanColumn extends CassColumn {
    List<Boolean> data;

    public CassBooleanColumn(String colName, int colIdx, int estimatedSize) {
      super(colName, colIdx, estimatedSize);
      data = new ArrayList<Boolean>(estimatedSize);
    }


    public void addFromRow(Row row) {
      if (row.isNull(this.colIdx)) {
        data.add(null);
      } else {
        data.add(new Boolean(row.getBool(this.colIdx)));
      }
    }

    public String getDataMethodName() { return("getBoolData"); }
    public String getDataJniType()    { return("[Ljava/lang/Boolean;"); }
    public String getDataRConverter() { return("as.numeric"); }
    public Boolean[] getBoolData() {
      Boolean[] retArr = data.toArray(new Boolean[data.size()]);
      return(retArr);
    }
  }

  List<CassColumn> columns;
  Hashtable<String, Integer> colNameToIndexLookup;


  public CassandraReader(Session cassSess, String cqlQuery, int estimatedRows) {
    System.out.println("Inside CassandraReader constructor!");

    System.out.println("START query....");
    ResultSet rs =  cassSess.execute(cqlQuery);

    System.out.println("DONE with query... now getting column mappings... ");
    this.initializeColumnMappings(rs, estimatedRows);
    System.out.println("Column Mapping: " + colNameToIndexLookup.toString());

    for (int i = 0; i < columns.size(); i++) {
      ((CassColumn)(columns.get(i))).printDebug();
    }

    System.out.println("START fetching data in Java");
    this.loadData(rs);
    System.out.println("DONE fetching data in Java");
  }

  public int getNumColumns() {
    return(columns.size());
  }

  /* i is the 0-based index */
  public CassColumn getColumn(int i) {
    return((CassColumn)(columns.get(i)));
  }

  private void loadData(ResultSet rs) {
    int i;
    while (!rs.isExhausted()) {
      Row row = rs.one();
      for (i = 0; i < columns.size(); i++) {
        ((CassColumn)(columns.get(i))).addFromRow(row);
      }
    }
  }

  public static Float[] floatObjVals() {
    Float[] f = new Float[4];
    f[0] = new Float(3.44);
    f[1] = new Float(141.20);
    f[2] = null;
    f[3] = new Float(-3.14);
    return(f);
  }

  public static float[] floatPrimVals() {
    float[] f = { 3.44, 141.20, 0, -3.14 };
    return(f);
  }

  public static String[] stringObjVals() {
    String[] f = new String[4];
    f[0] = "hello";
    f[1] = "goodbye";
    f[2] = null;
    f[3] = "another";
    return(f);
  }

  public static Long[] longObjVals() {
    Long[] f = new Long[4];
    f[0] = new Long(-3013);
    f[1] = new Long(492930);
    f[2] = null;
    f[3] = new Long(3393993);
    return(f);
  }


  public static long[] longPrimVals() {
    long[] f = { -3013, 492930, 0, 3393993 };
    return(f);
  }


  private void initializeColumnMappings(ResultSet rs, int estimatedRows) {
    ColumnDefinitions cdefs = rs.getColumnDefinitions();
    int numCols = cdefs.size();
    String cName;
    DataType cType;

    this.columns = new ArrayList<CassColumn>(numCols);
    this.colNameToIndexLookup = new Hashtable<String, Integer>();

    for (int i=1; i < numCols; i++) {
      cName = cdefs.getName(i);
      cType = cdefs.getType(i);

      CassColumn col;
      if (cType == DataType.smallint() || cType == DataType.cint()) {
        col = new CassIntColumn(cName, i, estimatedRows);
      } else if (cType == DataType.tinyint()) {
        col = new CassTinyintColumn(cName, i, estimatedRows);
      } else if (cType == DataType.date()) {
        col = new CassDateColumn(cName, i, estimatedRows);
      } else if (cType == DataType.text() || cType == DataType.varchar()) {
        col = new CassStringColumn(cName, i, estimatedRows);
      } else if (cType == DataType.timestamp()) {
        col = new CassTimeColumn(cName, i, estimatedRows);
      } else if (cType == DataType.cfloat()) {
        col = new CassFloatColumn(cName, i, estimatedRows);
      } else if (cType == DataType.cdouble()) {
        col = new CassDoubleColumn(cName, i, estimatedRows);
      } else if (cType == DataType.cboolean()) {
        col = new CassBooleanColumn(cName, i, estimatedRows);
      } else {
        System.err.println("ERROR:::: Unknown type: " + cName + " type " + cType + " ... " + cdefs.getType(i).asFunctionParameterString());
        // TODO: raise exception
        col = null;
      }

      if (col != null) {
        this.columns.add(col);
        this.colNameToIndexLookup.put(cName, i-1);
      }
    }
  }

/*
  public static void getColumns(cassSession, String cqlQuery) {
    ResultSet rs = cassSession.execute(cqlQuery);
    int ctr = 0;
    while (!rs.isExhausted()) {
      Row row = rs.one();
    }
  }
  */
}