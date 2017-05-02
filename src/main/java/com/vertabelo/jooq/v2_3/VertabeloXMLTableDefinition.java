package com.vertabelo.jooq.v2_3;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jooq.DataType;
import org.jooq.impl.DefaultDataType;
import org.jooq.tools.StringUtils;
import org.jooq.util.AbstractTableDefinition;
import org.jooq.util.ColumnDefinition;
import org.jooq.util.DataTypeDefinition;
import org.jooq.util.DefaultColumnDefinition;
import org.jooq.util.DefaultDataTypeDefinition;
import org.jooq.util.SchemaDefinition;
import com.vertabelo.jooq.jaxb.v2_3.Column;
import com.vertabelo.jooq.jaxb.v2_3.Property;
import com.vertabelo.jooq.jaxb.v2_3.Table;
import com.vertabelo.jooq.jaxb.v2_3.View;
import com.vertabelo.jooq.jaxb.v2_3.ViewColumn;

/**
 * Definition of the Vertabelo XML Table
 *
 * @author Michał Kołodziejski
 */
public class VertabeloXMLTableDefinition extends AbstractTableDefinition {

    protected Table table;
    protected View view;

    public VertabeloXMLTableDefinition(SchemaDefinition schema, Table table) {
        super(schema, table.getName(), "");

        this.table = table;
    }

    public VertabeloXMLTableDefinition(SchemaDefinition schema, View view) {
        super(schema, view.getName(), "");

        this.view = view;
    }


    @Override
    protected List<ColumnDefinition> getElements0() throws SQLException {
        if(table != null) {
            // table
            return getTableElements();

        } else {
            // view
            return getViewElements();
        }
    }


    protected List<ColumnDefinition> getTableElements() {
        List<ColumnDefinition> result = new ArrayList<ColumnDefinition>();

        String schemaName = getSchemaName();
        SchemaDefinition schema = getDatabase().getSchema(schemaName);

        int position = 0;
        for(Column column : table.getColumns().getColumn()) {
            ++position;

            // convert data type
            DataType<?> dataType = DefaultDataType.getDataType(getDialect(), column.getType());

            DataTypeDefinition type = new DefaultDataTypeDefinition(
                getDatabase(),
                schema,
                dataType.getTypeName(),
                dataType.hasLength() ? dataType.length() : null,
                dataType.hasPrecision() ? dataType.precision() : null,
                dataType.hasScale() ? dataType.scale() : null,
                column.isNullable(),
                !StringUtils.isEmpty(column.getDefaultValue()));

            ColumnDefinition columnDefinition = new DefaultColumnDefinition(
                this,
                column.getName(),
                position,
                type,
                false,
                column.getDescription());

            result.add(columnDefinition);
        }

        return result;
    }


    protected List<ColumnDefinition> getViewElements() {
        List<ColumnDefinition> result = new ArrayList<ColumnDefinition>();

        String schemaName = getSchemaName();
        SchemaDefinition schema = getDatabase().getSchema(schemaName);

        int position = 0;
        for(ViewColumn column : view.getViewColumns().getViewColumn()) {
            ++position;

            // convert data type
            DataType<?> dataType = DefaultDataType.getDataType(getDialect(), column.getType());

            DataTypeDefinition type = new DefaultDataTypeDefinition(
                getDatabase(),
                schema,
                dataType.getTypeName(),
                dataType.hasLength() ? dataType.length() : null,
                dataType.hasPrecision() ? dataType.precision() : null,
                dataType.hasScale() ? dataType.scale() : null,
                true,
                false);

            ColumnDefinition columnDefinition = new DefaultColumnDefinition(
                this,
                column.getName(),
                position,
                type,
                false,
                column.getDescription());

            result.add(columnDefinition);
        }

        return result;
    }

    protected String getSchemaName() {
        Property additionalProperty;

        if(table != null) {
            additionalProperty = VertabeloDatabase.findAdditionalProperty(VertabeloDatabase.SCHEMA_ADDITIONAL_PROPERTY_NAME, table.getProperties().getProperty());
        } else {
            additionalProperty = VertabeloDatabase.findAdditionalProperty(VertabeloDatabase.SCHEMA_ADDITIONAL_PROPERTY_NAME, view.getProperties().getProperty());
        }

        return VertabeloDatabase.getAdditionalPropertyValueOrEmpty(additionalProperty);
    }
}
