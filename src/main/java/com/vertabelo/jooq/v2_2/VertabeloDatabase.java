package com.vertabelo.jooq.v2_2;

import com.vertabelo.jooq.VertabeloModelLoader;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXB;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.JooqLogger;
import org.jooq.tools.StringUtils;
import org.jooq.meta.AbstractDatabase;
import org.jooq.meta.ArrayDefinition;
import org.jooq.meta.CatalogDefinition;
import org.jooq.meta.CheckConstraintDefinition;
import org.jooq.meta.ColumnDefinition;
import org.jooq.meta.DataTypeDefinition;
import org.jooq.meta.DefaultCheckConstraintDefinition;
import org.jooq.meta.DefaultDataTypeDefinition;
import org.jooq.meta.DefaultRelations;
import org.jooq.meta.DefaultSequenceDefinition;
import org.jooq.meta.DomainDefinition;
import org.jooq.meta.EnumDefinition;
import org.jooq.meta.PackageDefinition;
import org.jooq.meta.RoutineDefinition;
import org.jooq.meta.SchemaDefinition;
import org.jooq.meta.SequenceDefinition;
import org.jooq.meta.TableDefinition;
import org.jooq.meta.UDTDefinition;
import com.vertabelo.jooq.jaxb.v2_2.AlternateKey;
import com.vertabelo.jooq.jaxb.v2_2.AlternateKeyColumn;
import com.vertabelo.jooq.jaxb.v2_2.Column;
import com.vertabelo.jooq.jaxb.v2_2.DatabaseModel;
import com.vertabelo.jooq.jaxb.v2_2.Property;
import com.vertabelo.jooq.jaxb.v2_2.Reference;
import com.vertabelo.jooq.jaxb.v2_2.ReferenceColumn;
import com.vertabelo.jooq.jaxb.v2_2.Sequence;
import com.vertabelo.jooq.jaxb.v2_2.Table;
import com.vertabelo.jooq.jaxb.v2_2.TableCheck;
import com.vertabelo.jooq.jaxb.v2_2.View;
import org.jooq.meta.xml.XMLDatabase;

/**
 * The Vertabelo XML Database (XML version v2.2)
 *
 * @author Michał Kołodziejski
 */
public class VertabeloDatabase extends AbstractDatabase  {

    interface TableOperation {
        void invoke(Table table, String schemaName);
    }

    interface ViewOperation {
        void invoke(View view, String schemaName);
    }

	private static final JooqLogger log = JooqLogger.getLogger(VertabeloDatabase.class);

    // XML additional properties
    private static final String SCHEMA_ADDITIONAL_PROPERTY_NAME = "Schema";
    private static final String PK_ADDITIONAL_PROPERTY_NAME = "Primary key name";

    protected DatabaseModel databaseModel;
    
    protected DatabaseModel databaseModel() {
		if(databaseModel == null) {
			VertabeloModelLoader loader = new VertabeloModelLoader(getProperties());
			loader.readXML();
			String version = loader.getVertabeloXMLVersion();
			if(!"2.1".equals(version) && !"2.2".equals(version)) {
				throw new IllegalStateException("This class cannot parse data model version "+version);
			}
			try {
				databaseModel = JAXB.unmarshal(new ByteArrayInputStream(loader.getVertabeloXML().getBytes("UTF-8")), DatabaseModel.class);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Impossible has happen.", e);
			}
		}
        return databaseModel;
    }

    @Override
    protected DSLContext create0() {
		SQLDialect dialect = SQLDialect.DEFAULT;

		try {
			dialect = SQLDialect.valueOf(getProperties().getProperty("dialect"));
		} catch (Exception ignore) {
		}

		return DSL.using(dialect.family());
    }

    @Override
    protected void loadPrimaryKeys(final DefaultRelations relations) throws SQLException {

		filterTablesBySchema(databaseModel().getTables(), new TableOperation() {
            @Override
            public void invoke(Table table, String schemaName) {

                SchemaDefinition schema = getSchema(schemaName);
                TableDefinition tableDefinition = getTable(schema, table.getName());

                if (tableDefinition != null) {
                    String pkName = getTablePkName(table);

                    // iterate through all columns and find PK columns
					for (Column column : table.getColumns()) {
                        if (column.isPK()) {
                            relations.addPrimaryKey(pkName, tableDefinition, tableDefinition.getColumn(column.getName()));
                        }
                    }
                }
            }
        });
    }


    private String getTablePkName(Table table) {
        Property pkAdditionalProperty = VertabeloDatabase.findAdditionalProperty(PK_ADDITIONAL_PROPERTY_NAME,
            table.getProperties());
        String pkName = VertabeloDatabase.getAdditionalPropertyValueOrEmpty(pkAdditionalProperty);
        if (StringUtils.isEmpty(pkName)) {
            pkName = table.getName().toUpperCase() + "_PK";
        }
        return pkName;
    }

    @Override
    protected void loadUniqueKeys(final DefaultRelations relations) throws SQLException {

		filterTablesBySchema(databaseModel().getTables(), new TableOperation() {
            @Override
            public void invoke(Table table, String schemaName) {
                SchemaDefinition schema = getSchema(schemaName);
                TableDefinition tableDefinition = getTable(schema, table.getName());

                if (tableDefinition != null) {
                    // iterate through all UNIQUE keys for this table
					for (AlternateKey alternateKey : table.getAlternateKeys()) {

                        // iterate through all columns of this key
						for (AlternateKeyColumn alternateKeyColumn : alternateKey.getColumns()) {
							Column column = (Column) alternateKeyColumn.getColumn();
                            relations.addUniqueKey(alternateKey.getName(), tableDefinition, tableDefinition.getColumn(column.getName()));
                        }

                    }
                }
            }
        });

    }

    @Override
    protected void loadForeignKeys(final DefaultRelations relations) throws SQLException {

		for (final Reference reference : databaseModel().getReferences()) {
            final Table pkTable = (Table) reference.getPKTable();
            final Table fkTable = (Table) reference.getFKTable();

            filterTablesBySchema(Arrays.asList(pkTable), new TableOperation() {
                @Override
                public void invoke(Table table, String schemaName) {
                    SchemaDefinition schema = getSchema(schemaName);
                    TableDefinition pkTableDefinition = getTable(schema, pkTable.getName());
                    TableDefinition fkTableDefinition = getTable(schema, fkTable.getName());

					// we need to find unique key among PK and all alternate
					// keys...
                    String uniqueKeyName = findUniqueConstraintNameForReference(reference);
					if (uniqueKeyName == null) {
                        // no matching key - ignore this foreign key
                        return;
                    }

					for (ReferenceColumn referenceColumn : reference.getReferenceColumns()) {
                        Column fkColumn = (Column) referenceColumn.getFKColumn();
                        ColumnDefinition fkColumnDefinition = fkTableDefinition.getColumn(fkColumn.getName());

                        relations.addForeignKey(reference.getName(), fkTableDefinition, fkColumnDefinition,
                                uniqueKeyName, pkTableDefinition);
                    }
                }
            });

        }
    }

    private String findUniqueConstraintNameForReference(Reference reference) {
        // list of referenced columns
        List<Column> uniqueKeyColumns = new ArrayList<Column>();
		for (ReferenceColumn referenceColumn : reference.getReferenceColumns()) {
            uniqueKeyColumns.add((Column) referenceColumn.getPKColumn());
        }

        // list of PK columns
        Table pkTable = (Table) reference.getPKTable();
        List<Column> pkColumns = new ArrayList<Column>();
        for (Column column : pkTable.getColumns()) {
            if (column.isPK()) {
                pkColumns.add(column);
            }
        }

        if (uniqueKeyColumns.equals(pkColumns)) {
            // PK matches FK
            log.info("Primary key constraint matches foreign key: " + reference.getName());
            return getTablePkName((Table) reference.getPKTable());
        }

        // need to check alternate keys
		for (AlternateKey alternateKey : pkTable.getAlternateKeys()) {
            List<Column> akColumns = new ArrayList<Column>();
            for (AlternateKeyColumn column : alternateKey.getColumns()) {
                akColumns.add((Column) column.getColumn());
            }

            if (uniqueKeyColumns.equals(akColumns)) {
                // AK matches FK
                log.info("Alternate key constraint matches foreign key: " + reference.getName());
                return alternateKey.getName();
            }
        }

        // no match
        log.info("No matching unique constraint for foreign key: " + reference.getName());
        return null;
    }

    @Override
    protected void loadCheckConstraints(final DefaultRelations relations) throws SQLException {

		filterTablesBySchema(databaseModel().getTables(), new TableOperation() {
            @Override
            public void invoke(Table table, String schemaName) {
                SchemaDefinition schema = getSchema(schemaName);
                TableDefinition tableDefinition = getTable(schema, table.getName());

                if (tableDefinition != null) {

                    // iterate through all table checks
					for (TableCheck tableCheck : table.getTableChecks()) {
                        CheckConstraintDefinition checkConstraintDefinition = new DefaultCheckConstraintDefinition(
                            schema,
                            tableDefinition,
                            tableCheck.getName(),
                            tableCheck.getCheckExpression());

                        relations.addCheckConstraint(tableDefinition, checkConstraintDefinition);
                    }

                    // iterate through all columns and find columns with checks
					for (Column column : table.getColumns()) {
						if (!StringUtils.isBlank(column.getCheckExpression())) {
                            CheckConstraintDefinition checkConstraintDefinition = new DefaultCheckConstraintDefinition(
                                schema,
                                tableDefinition,
                                table.getName() + "_" + column.getName() + "_check",
                                column.getCheckExpression());

                            relations.addCheckConstraint(tableDefinition, checkConstraintDefinition);
                        }
                    }
                }
            }
        });
    }

    @Override
    protected List<CatalogDefinition> getCatalogs0() throws SQLException {
        List<CatalogDefinition> result = new ArrayList<CatalogDefinition>();
        result.add(new CatalogDefinition(this, "", ""));
        return result;
    }

    @Override
    protected List<SchemaDefinition> getSchemata0() throws SQLException {
        List<SchemaDefinition> result = new ArrayList<SchemaDefinition>();
        List<String> schemaNames = new ArrayList<String>();

        // search in tables
        for (Table table : databaseModel().getTables()) {
            Property additionalProperty = findAdditionalProperty(SCHEMA_ADDITIONAL_PROPERTY_NAME, table.getProperties());
            addUniqueSchemaName(additionalProperty, schemaNames);
        }

        // search in views
		for (View view : databaseModel().getViews()) {
			Property additionalProperty = findAdditionalProperty(SCHEMA_ADDITIONAL_PROPERTY_NAME, view.getProperties());
            addUniqueSchemaName(additionalProperty, schemaNames);
        }

        // transform
        for (String schemaName : schemaNames) {
            result.add(new SchemaDefinition(this, schemaName, null));
        }

        return result;
    }

    private void addUniqueSchemaName(Property additionalProperty, List<String> schemaNames) {
        String schemaName = ""; // default to empty string
        if (additionalProperty != null) {
            // additional property is set
            schemaName = additionalProperty.getValue();
        }

        if (!schemaNames.contains(schemaName)) {
            schemaNames.add(schemaName);
        }
    }

    @Override
    protected List<SequenceDefinition> getSequences0() throws SQLException {
        List<SequenceDefinition> result = new ArrayList<SequenceDefinition>();

		for (Sequence sequence : databaseModel().getSequences()) {
            Property additionalProperty = findAdditionalProperty(SCHEMA_ADDITIONAL_PROPERTY_NAME,
					sequence.getProperties());
			String schemaName = getAdditionalPropertyValueOrEmpty(additionalProperty);

            if (getInputSchemata().contains(schemaName)) {
                SchemaDefinition schema = getSchema(schemaName);

                DataTypeDefinition type = new DefaultDataTypeDefinition(
                    this,
                    schema,
                    "BIGINT"
                );

                result.add(new DefaultSequenceDefinition(schema, sequence.getName(), type));
            }
        }

        return result;
    }

    @Override
    protected List<TableDefinition> getTables0() throws SQLException {
        final List<TableDefinition> result = new ArrayList<TableDefinition>();

        // tables
		filterTablesBySchema(databaseModel().getTables(), new TableOperation() {
            @Override
            public void invoke(Table table, String schemaName) {
                SchemaDefinition schema = getSchema(schemaName);
                result.add(new VertabeloXMLTableDefinition(schema, table));
            }
        });

        // views
		filterViewsBySchema(databaseModel().getViews(), new ViewOperation() {
            @Override
            public void invoke(View view, String schemaName) {
                SchemaDefinition schema = getSchema(schemaName);
                result.add(new VertabeloXMLTableDefinition(schema, view));
            }
        });

        return result;
    }

    @Override
    protected List<EnumDefinition> getEnums0() {
        List<EnumDefinition> result = new ArrayList<EnumDefinition>();
        return result;
    }

    @Override
    protected List<DomainDefinition> getDomains0() throws SQLException {
        List<DomainDefinition> result = new ArrayList<DomainDefinition>();
        return result;
    }

    @Override
    protected List<UDTDefinition> getUDTs0() {
        List<UDTDefinition> result = new ArrayList<UDTDefinition>();
        return result;
    }

    @Override
    protected List<ArrayDefinition> getArrays0() {
        List<ArrayDefinition> result = new ArrayList<ArrayDefinition>();
        return result;
    }

    @Override
    protected List<RoutineDefinition> getRoutines0() {
        List<RoutineDefinition> result = new ArrayList<RoutineDefinition>();
        return result;
    }

    @Override
    protected List<PackageDefinition> getPackages0() {
        List<PackageDefinition> result = new ArrayList<PackageDefinition>();
        return result;
    }

    protected void filterTablesBySchema(List<Table> tables, TableOperation operation) {
        for (Table table : tables) {
            Property schemaAdditionalProperty = findAdditionalProperty(SCHEMA_ADDITIONAL_PROPERTY_NAME,
					table.getProperties());
            String schemaName = getAdditionalPropertyValueOrEmpty(schemaAdditionalProperty);

            if (getInputSchemata().contains(schemaName)) {

                operation.invoke(table, schemaName);

            }
        }
    }

    protected void filterViewsBySchema(List<View> views, ViewOperation operation) {
        for (View view : views) {
            Property schemaAdditionalProperty = findAdditionalProperty(SCHEMA_ADDITIONAL_PROPERTY_NAME,
					view.getProperties());
            String schemaName = getAdditionalPropertyValueOrEmpty(schemaAdditionalProperty);

            if (getInputSchemata().contains(schemaName)) {

                operation.invoke(view, schemaName);

            }
        }
    }

    public static Property findAdditionalProperty(String name, List<Property> properties) {
        for (Property property : properties) {
            if (property.getName().equalsIgnoreCase(name)) {
                return property;
            }
        }
        return null;
    }

    public static String getAdditionalPropertyValueOrEmpty(Property additionalProperty) {
        if (additionalProperty != null) {
            return additionalProperty.getValue();
        }
        return "";
    }
}
