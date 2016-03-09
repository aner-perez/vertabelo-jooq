package com.vertabelo.jooq;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.jooq.util.xml.XMLDatabase;

/**
 * The Vertabelo XML Database
 *
 * @author Michał Kołodziejski
 */
public class VertabeloXMLDatabase extends BaseVertabeloDatabase {


	protected void readXML() {

		String xml = "";
		String url = properties.getProperty(XMLDatabase.P_XML_FILE);
		try {
			try (BufferedReader fr = new BufferedReader(new FileReader(url))) {
				String line;
				while ((line = fr.readLine()) != null) {
					xml += line;
				}
				
			}
			this.setVertabeloXML(xml);
		} catch (IOException e) {
			throw new RuntimeException("Error while reading file: " + url, e);
		}
	}
}