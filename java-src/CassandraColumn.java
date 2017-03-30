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


public class CassandraColumn {

  public enum CassType {
    CASS_FLOAT,
    CASS_DOUBLE,
    CASS_STRING,
    CASS_INT,
    CASS_TINYINT,
    CASS_LONG,
    CASS_DATE,
    CASS_TIMESTAMP
  }

  String colName;
  String colCassTypeStr;
  CassType colCassType;
  int cassColIdx;
  String rConversionName;
  String getMethodName;
  String getMethodJniType;

  String[] stringData;
  double[] doubleData;
  float[] floatData;
  int[] intData;
  long[] longData;
  //boolean[] booleanData;

  int dataCount;


  int[] nullIndexes;
  int nullIndexesCount;

  public CassandraColumn(String colName, String colCassTypeStr, int cassColIdx, int estimatedSize, boolean debug) {
    this.colName = colName;
    this.cassColIdx = cassColIdx;

    this.colCassTypeStr = new String(colCassTypeStr);

    if (this.colCassTypeStr.equals("float")) {
      this.colCassType = CassType.CASS_FLOAT;
      this.floatData = new float[estimatedSize];
      this.getMethodName    = "getFloats";
      this.getMethodJniType = "[F";
      this.rConversionName  = "as.numeric";

    } else if (this.colCassTypeStr.equals("double")) {
      this.colCassType = CassType.CASS_DOUBLE;
      this.doubleData = new double[estimatedSize];
      this.getMethodName    = "getDoubles";
      this.getMethodJniType = "[D";
      this.rConversionName  = "as.numeric";

    } else if (this.colCassTypeStr.equals("text") || this.colCassTypeStr.equals("varchar")) {
      this.colCassType = CassType.CASS_STRING;
      this.stringData = new String[estimatedSize];
      this.getMethodName    = "getStrings";
      this.getMethodJniType = "[S";
      this.rConversionName  = "as.character";

    } else if (this.colCassTypeStr.equals("int")) {
      this.colCassType = CassType.CASS_INT;
      this.intData = new int[estimatedSize];
      this.getMethodName    = "getInts";
      this.getMethodJniType = "[I";
      this.rConversionName  = "as.integer";

    } else if (this.colCassTypeStr.equals("tinyint")) {
      this.colCassType = CassType.CASS_TINYINT;
      this.intData = new int[estimatedSize];
      this.getMethodName    = "getInts";
      this.getMethodJniType = "[I";
      this.rConversionName  = "as.integer";

    } else if (this.colCassTypeStr.equals("long")) {
      this.colCassType = CassType.CASS_LONG;
      this.longData = new long[estimatedSize];
      this.getMethodName    = "getLongs";
      this.getMethodJniType = "[J";
      this.rConversionName  = "as.integer";

    } else if (this.colCassTypeStr.equals("date")) {
      this.colCassType = CassType.CASS_DATE;
      this.intData = new int[estimatedSize]; // days since epoch
      this.getMethodName    = "getInts";
      this.getMethodJniType = "[I";
      this.rConversionName  = "as_date_from_1970_epoch_days";

    } else if (this.colCassTypeStr.equals("timestamp")) {
      this.colCassType = CassType.CASS_TIMESTAMP;
      this.longData = new long[estimatedSize];  // will be long
      this.getMethodName    = "getLongs";
      this.getMethodJniType = "[J";
      this.rConversionName  = "as_posixct_from_1970_epoch_seconds";

    } else {
      System.err.println("ERROR:::: Unknown type: " + this.colName + " type " + this.colCassTypeStr);
      // TODO: raise exception
    }

    this.nullIndexes = new int[estimatedSize];
    this.nullIndexesCount = 0;
    this.dataCount = 0;
  }

  public String getColName() { return(this.colName); }
  public String getGetMethodName() { return(this.getMethodName); }
  public String getGetMethodJniType() { return(this.getMethodJniType); }
  public String getRConversionName() { return(this.rConversionName); }

  public void printDebug() {
    System.out.println("Column: " + this.colName + " idx: " + this.cassColIdx + " type: " + this.colCassTypeStr);
  }

  public void addFromRow(Row row) {

    this.dataCount++;

    switch(this.colCassType) {
      case CASS_FLOAT :
        expandFloatDataIfNeeded(this.dataCount);
        this.floatData[this.dataCount-1] = row.getFloat(this.cassColIdx);
        break;
      case CASS_DOUBLE :
        expandDoubleDataIfNeeded(this.dataCount);
        this.doubleData[this.dataCount-1] = row.getDouble(this.cassColIdx);
        break;
      case CASS_STRING :
        expandStringDataIfNeeded(this.dataCount);
        this.stringData[this.dataCount-1] = row.getString(this.cassColIdx);
        break;
      case CASS_INT :
        expandIntDataIfNeeded(this.dataCount);
        this.intData[this.dataCount-1] = row.getInt(this.cassColIdx);
        break;
      case CASS_TINYINT :
        expandIntDataIfNeeded(this.dataCount);
        this.intData[this.dataCount-1] = (int)(row.getByte(this.cassColIdx));
        break;
      case CASS_LONG :
        expandLongDataIfNeeded(this.dataCount);
        this.longData[this.dataCount-1] = row.getLong(this.cassColIdx);
        break;
      case CASS_TIMESTAMP :
        expandLongDataIfNeeded(this.dataCount);
        // handle null here to prevent null pointer exceptions in the math.  proper NAs will be handled by R
        if (row.isNull(this.cassColIdx)) {
          this.longData[this.dataCount-1] = 0;
        } else {
          this.longData[this.dataCount-1] = row.getTimestamp(this.cassColIdx).getTime() / 1000;  // seconds since 1970-01-01 epoch
        }
        break;
      case CASS_DATE :
        expandIntDataIfNeeded(this.dataCount);
        // handle null here to prevent null pointer exceptions in the math.  proper NAs will be handled by R
        if (row.isNull(this.cassColIdx)) {
          this.intData[this.dataCount-1] = 0;
        } else {
          this.intData[this.dataCount-1] = row.getDate(this.cassColIdx).getDaysSinceEpoch();  // days since 1970-01-01 epoch
        }
        break;
    }

    // register NULL cases
    if (row.isNull(this.cassColIdx)) {
      this.nullIndexesCount++;
      expandNullIndexDataIfNeeded(this.nullIndexesCount);
      this.nullIndexes[this.nullIndexesCount-1] = dataCount-1;
    }
  }

  public int[] getNullIndexes()  {
    if (this.nullIndexesCount == 0) { return(new int[0]); }

    int[] retArr = new int[this.nullIndexesCount];
    System.arraycopy(this.nullIndexes, 0, retArr, 0, this.nullIndexesCount);
    return(retArr);
  }

  public double[] getDoubles() {
    if (this.dataCount == 0) { return(new double[0]); }

    double[] retArr = new double[this.dataCount];
    System.arraycopy(this.doubleData, 0, retArr, 0, this.dataCount);
    return(retArr);
  }

  public float[] getFloats() {
    if (this.dataCount == 0) { return(new float[0]); }

    float[] retArr = new float[this.dataCount];
    System.arraycopy(this.floatData, 0, retArr, 0, this.dataCount);
    return(retArr);
  }

  public int[] getInts() {
    if (this.dataCount == 0) { return(new int[0]); }

    int[] retArr = new int[this.dataCount];
    System.arraycopy(this.intData, 0, retArr, 0, this.dataCount);
    return(retArr);
  }

  public long[] getLongs() {
    if (this.dataCount == 0) { return(new long[0]); }

    long[] retArr = new long[this.dataCount];
    System.arraycopy(this.longData, 0, retArr, 0, this.dataCount);
    return(retArr);
  }

  public String[] getStrings() {
    if (this.dataCount == 0) { return(new String[0]); }

    String[] retArr = new String[this.dataCount];
    System.arraycopy(this.stringData, 0, retArr, 0, this.dataCount);
    return(retArr);
  }

  private int expansionNewSize(int newDataCount) {
    if (newDataCount < 1000) {
      return(newDataCount + 1000);
    } else {
      return(newDataCount * 2);
    }
  }


  private void expandNullIndexDataIfNeeded(int newNullCount) {
    if (this.nullIndexes.length <= newNullCount) {
      int[] newArr = new int[this.expansionNewSize(newNullCount)];
      System.arraycopy(this.nullIndexes, 0, newArr, 0, newNullCount-1);
      this.nullIndexes = null; // hint GC?
      this.nullIndexes = newArr;
    }
  }

  private void expandFloatDataIfNeeded(int newDataCount) {
    if (this.floatData.length <= newDataCount) {
      float[] newArr = new float[this.expansionNewSize(newDataCount)];
      System.arraycopy(this.floatData, 0, newArr, 0, newDataCount-1);
      this.floatData = null; // hint GC?
      this.floatData = newArr;
    }
  }

  private void expandDoubleDataIfNeeded(int newDataCount) {
    if (this.doubleData.length <= newDataCount) {
      double[] newArr = new double[this.expansionNewSize(newDataCount)];
      System.arraycopy(this.doubleData, 0, newArr, 0, newDataCount-1);
      this.doubleData = null; // hint GC?
      this.doubleData = newArr;
    }
  }

  private void expandStringDataIfNeeded(int newDataCount) {
    if (this.stringData.length <= newDataCount) {
      String[] newArr = new String[this.expansionNewSize(newDataCount)];
      System.arraycopy(this.stringData, 0, newArr, 0, newDataCount-1);
      this.stringData = null; // hint GC?
      this.stringData = newArr;
    }
  }

  private void expandIntDataIfNeeded(int newDataCount) {
    if (this.intData.length <= newDataCount) {
      int[] newArr = new int[this.expansionNewSize(newDataCount)];
      System.arraycopy(this.intData, 0, newArr, 0, newDataCount-1);
      this.intData = null; // hint GC?
      this.intData = newArr;
    }
  }

  private void expandLongDataIfNeeded(int newDataCount) {
    if (this.longData.length <= newDataCount) {
      long[] newArr = new long[this.expansionNewSize(newDataCount)];
      System.arraycopy(this.longData, 0, newArr, 0, newDataCount-1);
      this.longData = null; // hint GC?
      this.longData = newArr;
    }
  }
}
