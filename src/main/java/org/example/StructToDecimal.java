package org.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

public class StructToDecimal<R extends ConnectRecord<R>> implements Transformation<R> {

    private Set<String> fieldNames;

    @Override
    public void configure(Map<String, ?> configs) {
        String fieldNamesStr = (String) configs.get("field.names");
        if (fieldNamesStr == null || fieldNamesStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing 'field.names' config.");
        }
        this.fieldNames = new HashSet<>();
        for (String name : fieldNamesStr.split(",")) {
            fieldNames.add(name.trim());
        }
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Struct)) {
            return record;
        }

        Struct valueStruct = (Struct) record.value();
        Schema schema = record.valueSchema();

        // Yeni schema
        SchemaBuilder newSchemaBuilder = SchemaBuilder.struct().name(schema.name());
        for (Field f : schema.fields()) {
            if (fieldNames.contains(f.name())) {
                newSchemaBuilder.field(f.name(), Schema.FLOAT64_SCHEMA);
            } else {
                newSchemaBuilder.field(f.name(), f.schema());
            }
        }
        Schema newSchema = newSchemaBuilder.build();

        // Yeni struct
        Struct newStruct = new Struct(newSchema);
        for (Field f : schema.fields()) {
            if (fieldNames.contains(f.name())) {
                Struct decimalStruct = valueStruct.getStruct(f.name());
                if (decimalStruct != null) {
                    Integer scale = decimalStruct.getInt32("scale");
                    ByteBuffer bytes = ByteBuffer.wrap(decimalStruct.getBytes("value"));
                    BigInteger unscaled = new BigInteger(bytes.array());
                    BigDecimal decimalValue = new BigDecimal(unscaled, scale);
                    newStruct.put(f.name(), decimalValue.doubleValue());
                } else {
                    newStruct.put(f.name(), null);
                }
            } else {
                newStruct.put(f.name(), valueStruct.get(f));
            }
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema,
                newStruct,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef().define(
                "field.names",
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Comma-separated list of field names to convert from STRUCT to double"
        );
    }

    @Override
    public void close() {}

    public String version() {
        return "1.0";
    }
}
