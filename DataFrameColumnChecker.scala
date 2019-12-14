package com.sample

import org.apache.spark.sql.DataFrame

case class MissingDataFrameColumnException(msg : String) extends Exception(msg);

case class DataFrameColumnChecker(df:DataFrame,requiredFields:Seq[String]) {
  val missingColumns = requiredFields.diff(df.columns.toSeq)
  def missingColumnMessage():String={
    val missingColNames = missingColumns.mkString(", ")
    val allColumns = df.columns.toSeq.mkString(", ")
    s"${missingColNames} not included in the list of columns ${allColumns}"
  }
  def validateColumns:Boolean={
    if(missingColumns.nonEmpty) false
      //throw new MissingDataFrameColumnException(missingColumnMessage())
    else true
  }
}

