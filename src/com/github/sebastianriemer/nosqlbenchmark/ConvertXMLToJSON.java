package com.github.sebastianriemer.nosqlbenchmark;

import java.io.IOException;
import java.io.InputStream;

import net.sf.json.JSON;
import net.sf.json.xml.XMLSerializer;

import org.apache.commons.io.IOUtils;

public final class ConvertXMLToJSON {

	public static JSON convertXmlToJson(String fileName) {

		InputStream is = ConvertXMLToJSON.class
				.getResourceAsStream("resources/xmlfiles/" + fileName);
		try {
			String xml = IOUtils.toString(is);
			XMLSerializer xmlSerializer = new XMLSerializer();

			JSON json = xmlSerializer.read(xml);

			return json;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
		return null;

	}

}
