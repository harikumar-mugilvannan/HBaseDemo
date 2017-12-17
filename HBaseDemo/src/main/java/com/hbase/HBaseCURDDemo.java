package com.hbase;

import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.log4j.BasicConfigurator;

import com.google.protobuf.ServiceException;

public class HBaseCURDDemo {

	public static void main(String[] args) throws IOException, ServiceException
	{
		
		BasicConfigurator.configure();

		HBaseDemo demo = new HBaseDemo();
		//Establish HBase Admin Connection
		Admin admin = demo.connectHBase();
		
		System.out.println("HBase Operations through Java API");
		System.out.println("___________________________________");
		System.out.println("1. Create New Namespace");
		System.out.println("2. List All Namespace in HBase");
		System.out.println("3. Create New Table");
		System.out.println("4. List All Tables in HBase");
		System.out.println("5. Insert/Update Row in PRODUCT table");
		System.out.println("6. Get a Row in PRODUCT table");
		System.out.println("7. Scan All Rows in PRODUCT table");
		System.out.println("___________________________________");
		
		System.out.println("Your Option ????");
		Scanner scn = new Scanner(System.in);
		int option = scn.nextInt();
		
		switch(option)
		{
		case 1:			
				//Create Namespace
				System.out.println("What is your Namespace name?");
				demo.createNameSpace(admin, scn.next());
				break;
		case 2:
				//List All Namespace from HBase
				demo.listAllNameSpace(admin);
				break;
		case 3:
				//Create a brand new Table in HBase
				System.out.println("What is your Table Name?");
				String tblName = scn.next();
				System.out.println("List Column families in comma separated");
				String[] colFamilies = scn.next().split(",");			
				demo.createNewTable(admin,tblName,colFamilies);
				break;
		case 4:
				//List All Tables from HBase
				demo.listAllTables(admin);
				break;
		case 5:
				//Insert/Update Rows
				Hashtable<String, String> inpRow = new Hashtable<String, String>();
				System.out.println("What is your Product UPC Code?");
				inpRow.put("prdc_base_dtl:upc", scn.next());
				System.out.println("What is your Product Description?");
				inpRow.put("prdc_base_dtl:description", scn.next());
				System.out.println("Who Manufactured the Product?");
				inpRow.put("prdc_base_dtl:manufacturer", scn.next());
				System.out.println("What is Size of the Product?");
				inpRow.put("prdc_attrb:size", scn.next());
				System.out.println("What is your Product Color?");
				inpRow.put("prdc_attrb:color", scn.next());
				System.out.println("What made your products?");
				inpRow.put("prdc_attrb:ingredients", scn.next());
				System.out.println("What is your Product List Price?");
				inpRow.put("prdc_price:list_price", scn.next());
				System.out.println("What is your Product Actual Price?");
				inpRow.put("prdc_price:actual_price", scn.next());
				System.out.println("What is your rating to the Poduct?");
				inpRow.put("prdc_feedback:rating", scn.next());
				System.out.println("Comment your Product");
				inpRow.put("prdc_feedback:comments", scn.next());
				demo.addUpdateProduct(inpRow);
				break;
				
		case 6:
				//Get a particular Product
				System.out.println("What is your Product code?");
				demo.getProduct(scn.next());
				break;
		
		case 7:
				//Scan Product
				demo.scanProduct();
				break;
		default:
				System.out.println("Choose only the options that are available");
		}
		scn.close();

	}
}
