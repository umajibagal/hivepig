package com.bagal.hive_pig_project;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveJdbcClient {
	  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	   public static void main(String[] args) throws SQLException, InterruptedException {
	      try {
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      e.printStackTrace();
	      System.exit(1);
	    }
	    //replace "hive" here with the name of the user the queries should run as
	    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/bagal", "cloudera", "cloudera");
	    Statement stmt = con.createStatement();
	    String tableName = "student";
	   // System.out.println("\nResult of drop table : "+stmt.execute("drop table if exists " + tableName));
	   // System.out.println("\nResut of create table"+stmt.execute("create table " + tableName + " (roll int, name string,dept string)"+" row format delimited"
	  //  		+ " fields terminated by ',' stored as textfile"));
	    // show tables
	    String sql = "show tables " ;//+ tableName + "'";
	    System.out.println("Running: " + sql);
	    ResultSet res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getString(1));
	    }
	    System.out.println("\n************************************\n");   
	    // describe table
	    sql = "describe " + tableName;
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getString(1) + "\t" + res.getString(2)+"\t"+res.getString(3));
	    }
	 
    
	   /* String filepath = "hdfs://quickstart.cloudera:8020/user/cloudera/data/student.txt";
	    sql = "load data inpath '" + filepath + "' into table " + tableName;
	    System.out.println("Running: " + sql);
	    stmt.execute(sql);*/
	 
	    // select * query
	    sql = "select * from " + tableName;
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getInt(1)+ "\t" + res.getString(2)+"\t"+res.getString(3));
	    }
	 
	   /* // regular hive query
	    sql = "select count(1) from " + tableName;
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	   // while (res.next()) {
	    res.next();
	      System.out.println("Total count : "+res.getString(1));
	    //}*/
	    System.out.println("End of programmmm");
	    res.close();
	    stmt.close();
	    con.close();
	  }
	}