package com.github.sebastianriemer.nosqlbenchmark;

import java.io.IOException;
import java.io.InputStream;

import net.sf.json.JSON;
import net.sf.json.JSONObject;
import net.sf.json.xml.XMLSerializer;

import org.apache.commons.io.IOUtils;

public final class ConvertXMLToJSON {

	public static JSON convertXmlToJson(String fileName) {

		InputStream is = ConvertXMLToJSON.class
				.getResourceAsStream("/resources/" + fileName);
		try {
			System.out.println("Converting from XML to JSON ...");
			String xml = IOUtils.toString(is);
			XMLSerializer xmlSerializer = new XMLSerializer();

			JSON json = xmlSerializer.read(xml);
//			System.out.println("json.toString()=" + json.toString());
			JSONObject jsonObj = (JSONObject) json;
			
			System.out.println("jsonObj.toString()=" + jsonObj.toString());
//			Iterator<?> keys = jsonObj.keys();

//			while (keys.hasNext()) {
//				String key = (String) keys.next();
//				System.out.println("Found a key of type ... " +jsonObj.get(key).getClass().toString());				
//				System.out.println("[" + key + "]=" + jsonObj.get(key).toString());				
//			}

			return json;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
		return null;

	}

}
