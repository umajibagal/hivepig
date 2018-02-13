package com.bagal.hive_pig_project;

import java.io.IOException;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class PigClient {

	public static void main(String[] args) throws ExecException {
		Properties p = new Properties();
		p.setProperty("fs.default.name", "hdfs://localhost:8020/");
		p.setProperty("mapred.job.tracker", "localhost:8021");
		PigServer pigServer = new PigServer(ExecType.LOCAL, p);
		String filepath = "hdfs://quickstart.cloudera:8020/user/cloudera/data/student.txt";
		try {
			runMyQuery(pigServer, filepath);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void runMyQuery(PigServer pigServer, String inputfile)
			throws IOException {
		pigServer.setJobName("Embedded PIG Statements in JAVA");
		pigServer.registerQuery("student_data = LOAD '" + inputfile
				+ "' Using PigStorage(',') AS (roll: int, name: chararray, dept:chararray);");
		pigServer.registerQuery("no_dept = GROUP student_data BY dept;");
	    pigServer.registerQuery("number_of_student = FOREACH no_dept GENERATE group, COUNT(student_data);");
		pigServer.store("number_of_student","hdfs://quickstart.cloudera:8020/user/cloudera/data/output1");
	}
}

