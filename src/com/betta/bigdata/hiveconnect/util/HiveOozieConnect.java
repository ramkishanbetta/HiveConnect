package com.betta.bigdata.hiveconnect.util;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

public class HiveOozieConnect {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {

			// TODO
			e.printStackTrace();
		}

		try {

			try {

				Configuration conf = new Configuration();
				conf.set("hadoop.security.authentication", "Kerberos");
//				UserGroupInformation.setConfiguration(conf);
//				UserGroupInformation.loginUserFromKeytab("LDAPAC@USER.AD.COM", "LDAPAC.keytab");
				System.setProperty("java.security.auth.login.config", "gss-jaas.conf");
				System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
				System.setProperty("java.security.krb5.conf", "krb5.conf");
				System.setProperty("user.name", "LDAPAC");
				conf.set("hive.server2.authentication.kerberos.principal",
						"hive/gl0002.USER.AD.COM@USER.AD.COM");
				conf.set("hive.metastore.kerberos.principal",
						"hive/gl0002.USER.AD.COM@USER.AD.COM");
				conf.set("hcat.metastore.uri", "thrift://gl0002.USER.AD.COM:9083");
				conf.set("hcat.metastore.principal", "hive/gl0002.USER.AD.COM@USER.AD.COM");
				conf.set("hive.server2.authentication.kerberos.keytab", "LDAPAC.keytab");
				conf.set("hive.server.user.principal", "LDAPAC@USER.AD.COM");
				SecurityUtil.login(conf, "hive.server2.authentication.kerberos.keytab", "hive.server.user.principal");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Connection con = DriverManager.getConnection(
					"jdbc:hive2://gl0002:10000/myDB;principal=hive/gl0002.USER.AD.COM@USER.AD.COM",
					"", "");
			Statement statement = con.createStatement();
			ResultSet rs = statement.executeQuery(
					"select activity_a_ext.rec_chng_type from activity_a_ext where period_date='2015-12-08' limit 1");
			System.out.println("**********Query");
			if (rs != null) {

				System.out.println("********Res");
				while (rs.next()) {

					System.out.println( 	);
					File file = new File(System.getProperty("oozie.action.output.properties"));
					Properties props = new Properties();
					props.setProperty("Value", rs.getString(1));
				}

			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
