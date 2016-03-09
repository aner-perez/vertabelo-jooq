package com.vertabelo.jooq;

import org.jooq.tools.JooqLogger;
import org.jooq.tools.StringUtils;

/**
 * The Vertabelo API Database
 *
 * @author Michał Kołodziejski
 */
public class VertabeloAPIDatabase extends BaseVertabeloDatabase {

	private static final JooqLogger log = JooqLogger.getLogger(VertabeloAPIDatabase.class);

	protected static final String API_TOKEN_PARAM = "api-token";
	protected static final String MODEL_ID_PARAM = "model-id";
	protected static final String TAG_NAME_PARAM = "tag-name";

	protected String apiToken;
	protected String modelId;
	protected String tagName;

	public VertabeloAPIDatabase() {
		
	}
	
	@Override
	protected void readXML() {
		readSettings();
		
		VertabeloAPIClient client = new VertabeloAPIClient(apiToken);
		
		String xml = client.getXML(modelId, tagName);
		setVertabeloXML(xml);

	}


	private void readSettings() {

		apiToken = properties.getProperty(API_TOKEN_PARAM);
		if (StringUtils.isEmpty(apiToken)) {
			throw new IllegalStateException("Lack of \"" + API_TOKEN_PARAM + "\" parameter.");
		}

		modelId = properties.getProperty(MODEL_ID_PARAM);
		if (StringUtils.isEmpty(modelId)) {
			throw new IllegalStateException("Lack of \"" + MODEL_ID_PARAM + "\" parameter.");
		}

		tagName = properties.getProperty(TAG_NAME_PARAM);
	}

}
