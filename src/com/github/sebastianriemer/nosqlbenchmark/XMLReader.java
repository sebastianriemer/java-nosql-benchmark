package com.github.sebastianriemer.nosqlbenchmark;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import java.io.File;
import java.io.IOException;
import java.net.URL;

/*
 * Currently not in use, since class ConvertXMLToJSON already reads the XML and needs an inputstream instead
 */
public final class XMLReader {
	 
		public static Document getXMLFromFile(String fileName) {
	 
			try {
				try {				  		    	  
		    	  URL finalFile = ClassLoader.class.getResource("/resources/"+ fileName);
		    	  System.out.println("Final filepath : " + finalFile.toString());
		    	  File file = new File(finalFile.toURI());
		 
		    	  DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		    	  DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		    	  Document doc = dBuilder.parse(file);
		    	  doc.getDocumentElement().normalize(); // might not be needed or either unwanted
		    	  
		    	  return doc;		    	  
		    	  			  
		    	} catch (IOException e) {
			      e.printStackTrace();
			      return null;
			}
			  					
			
//			System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
//			NodeList nList = doc.getElementsByTagName("staff");
//			System.out.println("-----------------------");
//	 
//			for (int temp = 0; temp < nList.getLength(); temp++) {
//	 
//			   Node nNode = nList.item(temp);
//			   if (nNode.getNodeType() == Node.ELEMENT_NODE) {
//	 
//			      Element eElement = (Element) nNode;
//	 
//			      System.out.println("First Name : " + getTagValue("firstname", eElement));
//			      System.out.println("Last Name : " + getTagValue("lastname", eElement));
//		              System.out.println("Nick Name : " + getTagValue("nickname", eElement));
//			      System.out.println("Salary : " + getTagValue("salary", eElement));
//	 
//			   }
//			}
			
		  } catch (Exception e) {
			e.printStackTrace();
			return null;
		  }
	  }
	 
//	  private static String getTagValue(String sTag, Element eElement) {
//		NodeList nlList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();
//	 
//	        Node nValue = (Node) nlList.item(0);
//	 
//		return nValue.getNodeValue();
//	  }
	 
	
}
