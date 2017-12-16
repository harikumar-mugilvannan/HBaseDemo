package com.hbase;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.BasicConfigurator;

import com.google.protobuf.ServiceException;

public class HBaseFirstDemo {

	public static void main(String[] args) throws IOException, ServiceException
	{

		BasicConfigurator.configure();

		//HBase Config Creation
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("/usr/iop/current/hbase-client/conf/hbase-site.xml"));
	
		//Initialize Connect
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//Get Tables
		Table tbl = conn.getTable(TableName.valueOf("emp"));
		
		//Scan a Table
		Scan scn = new Scan();
		ResultScanner resScan = tbl.getScanner(scn);
		System.out.println("*** Column Families, Column Specifications, Column Value *** : ");	
		for(Result res : resScan)
		{
			// Print RowKey
			System.out.println(":"+new String(res.getRow()));
			
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> row = res.getMap();
			for(byte[] colFam : row.keySet())
			{
				// Print Column Family
				System.out.println("\t:"+new String(colFam));

				NavigableMap<byte[],NavigableMap<Long,byte[]>> colSpecLst = row.get(colFam);
				for(byte[] colSpec : colSpecLst.keySet())
				{
					NavigableMap<Long,byte[]> val = colSpecLst.get(colSpec);
					System.out.println("\t\t:"+new String(colSpec)+"\t:"+new String(val.get(val.firstKey())));
				}
			
			}
		}
		System.out.println("**************************************************");
	}
}
