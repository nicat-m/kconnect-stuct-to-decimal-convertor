package org.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;

public class StructToDecimal<R extends ConnectRecord<R>> implements Transformation<R> {

    private String fieldName;

    @Override
    public void configure(Map<String, ?> configs) {
        this.fieldName = (String) configs.get("field.name");
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Struct)) {
            return record;
        }

        Struct valueStruct = (Struct) record.value();
        Schema schema = record.valueSchema();

        Field field = schema.field(fieldName);
        if (field == null) {
            return record;
        }

        Struct decimalStruct = valueStruct.getStruct(fieldName);
        if (decimalStruct == null) {
            return record;
        }

        Integer scale = decimalStruct.getInt32("scale");
        ByteBuffer bytes = ByteBuffer.wrap(decimalStruct.getBytes("value"));
        BigInteger unscaled = new BigInteger(bytes.array());
        BigDecimal decimalValue = new BigDecimal(unscaled, scale);

        // New schema: same as original but replace the field type
        SchemaBuilder newSchemaBuilder = SchemaBuilder.struct().name(schema.name());
        for (Field f : schema.fields()) {
            if (f.name().equals(fieldName)) {
                newSchemaBuilder.field(f.name(), Schema.FLOAT64_SCHEMA); // use float64 (double)
            } else {
                newSchemaBuilder.field(f.name(), f.schema());
            }
        }
        Schema newSchema = newSchemaBuilder.build();

        Struct newStruct = new Struct(newSchema);
        for (Field f : schema.fields()) {
            if (f.name().equals(fieldName)) {
                newStruct.put(f.name(), decimalValue.doubleValue());  // converted field
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
        return new ConfigDef().define("field.name", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Field name to convert");
    }

    @Override
    public void close() {}

    public String version() {
        return "1.0";
    }
}
