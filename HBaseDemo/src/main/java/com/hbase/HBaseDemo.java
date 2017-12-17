package com.hbase;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo {

	public Configuration installConfig()
	{
		// Create HBase Configuration Object 
		Configuration conf = HBaseConfiguration.create();
		//Add the remote HBase Site XML file which has all HBase properties
		conf.addResource(new Path("/usr/iop/current/hbase-client/conf/hbase-site.xml"));
		return conf;
	}
	
	public Connection installConnection() throws IOException
	{
		// Create HBase Configuration Object 
		Configuration conf = installConfig();
		//Create Connection and get Admin hold off
		Connection conn = ConnectionFactory.createConnection(conf);
		return conn;
	}
	
	public Admin connectHBase() throws IOException
	{
		// Create HBase Configuration Object 
		Configuration conf = installConfig();
		//Create Connection and get Admin hold off
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		return admin;
	}
	
	public void createNameSpace(Admin admin, String nameSpace) throws IOException
	{
		System.out.println("Creating Namespace : "+ nameSpace);
		NamespaceDescriptor nsd = NamespaceDescriptor.create(nameSpace).build();
		admin.createNamespace(nsd);
		System.out.println("Namespace Created : "+ nameSpace);
	}
	
	public void listAllNameSpace(Admin admin) throws IOException
	{
		NamespaceDescriptor[] nameSpaceList = admin.listNamespaceDescriptors();
		System.out.println("List of Namespace available in the HBase");
		System.out.println("___________________________________________");
		for(NamespaceDescriptor nameSpace : nameSpaceList)
		{
			System.out.println(nameSpace);
		}
	}
	
	
	public void createNewTable(Admin admin, String tableName, String[] colFamilies) throws IOException
	{
		System.out.println("Creating Table : "+ tableName);
		
		TableName tblNm = TableName.valueOf(tableName);
		HTableDescriptor hTblDesc = new HTableDescriptor(tblNm);
		for(String colFmly : colFamilies)
		hTblDesc.addFamily(new HColumnDescriptor(colFmly));
		admin.createTable(hTblDesc);
		System.out.println("Table Created : "+ tableName);
	}
	
	public void listAllTables(Admin admin) throws IOException
	{
		TableName[] tableList = admin.listTableNames();
		System.out.println("List of Tables available in the HBase");
		System.out.println("___________________________________________");
		for(TableName table : tableList)
		{
			System.out.println(table);
		}
	}
	
	public void addUpdateProduct(Hashtable product) throws IOException
	{
		Connection conn = installConnection();
		Table table = conn.getTable(TableName.valueOf("product"));
		Put p = new Put(Bytes.toBytes(Integer.parseInt(product.get("prdc_base_dtl:upc").toString())));
		Set<String> allKeys = product.keySet();
		for (String productKey : allKeys)
		{
			String[] columns = productKey.split(":");
			p.addColumn(Bytes.toBytes(columns[0]), Bytes.toBytes(columns[1]), Bytes.toBytes(product.get(productKey).toString()));
		}
		table.put(p);
	}
	
	
	public void getProduct(String upcCode) throws IOException
	{
		Connection conn = installConnection();
		Table table = conn.getTable(TableName.valueOf("product"));
		Get g = new Get(Bytes.toBytes(upcCode));
		g.addColumn(Bytes.toBytes("prdc_base_dtl"), Bytes.toBytes("upc"));
		g.addColumn(Bytes.toBytes("prdc_base_dtl"), Bytes.toBytes("description"));
		g.addColumn(Bytes.toBytes("prdc_base_dtl"), Bytes.toBytes("manufacturer"));
		g.addColumn(Bytes.toBytes("prdc_price"), Bytes.toBytes("actual_price"));
		g.addColumn(Bytes.toBytes("prdc_feedback"), Bytes.toBytes("rating"));
		Result res = table.get(g);
		System.out.println("UPC\t:"+ new String(res.getValue(Bytes.toBytes("prdc_base_dtl"), Bytes.toBytes("upc"))));
		System.out.println("Description\t:"+ new String(res.getValue(Bytes.toBytes("prdc_base_dtl"), Bytes.toBytes("description"))));
		System.out.println("Manufacturer\t:"+ new String(res.getValue(Bytes.toBytes("prdc_base_dtl"), Bytes.toBytes("manufacturer"))));
		System.out.println("Price\t:"+ new String(res.getValue(Bytes.toBytes("prdc_price"), Bytes.toBytes("actual_price"))));
		System.out.println("Rating\t:"+ new String(res.getValue(Bytes.toBytes("prdc_feedback"), Bytes.toBytes("rating"))));		
	}
	
	public void scanProduct() throws IOException
	{
		Connection conn = installConnection();
		Table table = conn.getTable(TableName.valueOf("product"));
		Scan scan = new Scan();
		ResultScanner resScan = table.getScanner(scan);
		for(Result res : resScan)
		{
				System.out.println("UPC\t:"+ new String(res.getValue(Bytes.toBytes("prdc_base_dtl"), Bytes.toBytes("upc"))));
				System.out.println("Description\t:"+ new String(res.getValue(Bytes.toBytes("prdc_base_dtl"), Bytes.toBytes("description"))));
				System.out.println("Manufacturer\t:"+ new String(res.getValue(Bytes.toBytes("prdc_base_dtl"), Bytes.toBytes("manufacturer"))));
				System.out.println("Price\t:"+ new String(res.getValue(Bytes.toBytes("prdc_price"), Bytes.toBytes("actual_price"))));
				System.out.println("Rating\t:"+ new String(res.getValue(Bytes.toBytes("prdc_feedback"), Bytes.toBytes("rating"))));		
		}
	}
	
}
