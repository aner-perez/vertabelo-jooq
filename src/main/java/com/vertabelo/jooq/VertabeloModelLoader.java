package com.vertabelo.jooq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.jooq.tools.JooqLogger;
import org.jooq.tools.StringUtils;
import org.jooq.meta.xml.XMLDatabase;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Base class for classes called by end user.
 * 
 * 
 * @author Rafał Strzaliński
 * @author Michał Kołodziejski
 *
 */
public class VertabeloModelLoader {

	private static final JooqLogger log = JooqLogger.getLogger(VertabeloModelLoader.class);

	protected static final String API_TOKEN_PARAM = "api-token";
	protected static final String MODEL_ID_PARAM = "model-id";
	protected static final String TAG_NAME_PARAM = "tag-name";

	protected Properties properties;
	
	private String vertabeloXML;
	private String vertabeloXMLVersion;

	public VertabeloModelLoader(Properties properties) {
		this.properties = properties;
	}

	/**
	 * @return the vertabeloXML
	 */
	public String getVertabeloXML() {
		return vertabeloXML;
	}

	/**
	 * @return the vertabeloXMLVersion
	 */
	public String getVertabeloXMLVersion() {
		return vertabeloXMLVersion;
	}

	public void readXML() {
		String xml;
		String xmlFileName = properties.getProperty(XMLDatabase.P_XML_FILE);
		String apiToken = properties.getProperty(API_TOKEN_PARAM);
		String modelId = properties.getProperty(MODEL_ID_PARAM);
		String tagName = properties.getProperty(TAG_NAME_PARAM);
		if(xmlFileName != null) {
			xml = readFileXML(xmlFileName);
		} else if(apiToken != null && modelId != null) {
			xml = readAPIXML(apiToken, modelId, tagName);
		} else {
			throw new IllegalStateException("Either ['"+XMLDatabase.P_XML_FILE+ "'] or ['" + API_TOKEN_PARAM + "' and '"+ MODEL_ID_PARAM +"'] parameters must be specified.");			
		}
		setVertabeloXML(xml);
	}

	protected String readFileXML(String fileName) {
		try {
			File file = new File(fileName);
			StringBuilder xml = new StringBuilder((int)file.length());
			try (BufferedReader fr = new BufferedReader(new FileReader(file))) {
				String line;
				while ((line = fr.readLine()) != null) {
					xml.append(line);
				}
				
			}
			return xml.toString();
		} catch (IOException e) {
			throw new RuntimeException("Error while reading file: " + fileName, e);
		}
	}

	protected String readAPIXML(String apiToken, String modelId, String tagName) {
		if (StringUtils.isEmpty(apiToken)) {
			throw new IllegalStateException("Missing \"" + API_TOKEN_PARAM + "\" parameter.");
		}

		if (StringUtils.isEmpty(modelId)) {
			throw new IllegalStateException("Missing \"" + MODEL_ID_PARAM + "\" parameter.");
		}

		VertabeloAPIClient client = new VertabeloAPIClient(apiToken);
		
		return client.getXML(modelId, tagName);
	}

	protected void setVertabeloXML(String xml) {
		vertabeloXML = xml;

		vertabeloXMLVersion = this.parseVersion(xml);
	
		log.info("Detected Vertabelo XML version: ", vertabeloXMLVersion);
	}

	private String parseVersion(String xml) {

		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dbBuilder = dbFactory.newDocumentBuilder();
			InputSource is = new InputSource();
			is.setCharacterStream(new StringReader(xml));
			
			Document doc = dbBuilder.parse(is);
				        Node root = doc.getElementsByTagName("DatabaseModel").item(0);

	        Node attr = root.getAttributes().getNamedItem("VersionId");
	        return attr.getNodeValue();
	        
		} catch (ParserConfigurationException | SAXException | IOException e) {
			throw new RuntimeException("Error while parsing Vertabelo XML file.",e);
		}

	}
}
