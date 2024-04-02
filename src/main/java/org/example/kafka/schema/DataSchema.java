package org.example.kafka.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.fasterxml.jackson.databind.type.LogicalType.Map;

public class DataSchema {

    public boolean createShemaFile() throws IOException {
        Path path = Paths.get("src")
                .resolve("main")
                .resolve("resources")
                .resolve("data.avsc");
        boolean newFile = path.toFile().createNewFile();
        try (BufferedWriter fileWriter = Files.newBufferedWriter(path))
        {
            fileWriter.write(createDataSchema().toString());
        }
        return newFile;
    }

    private Schema createDataSchema() {
        List<Schema> basicTypes = new ArrayList<>();
        basicTypes.add(Schema.create(Schema.Type.STRING));
        basicTypes.add(Schema.create(Schema.Type.INT));
        basicTypes.add(Schema.create(Schema.Type.LONG));
        basicTypes.add(Schema.create(Schema.Type.FLOAT));
        basicTypes.add(Schema.create(Schema.Type.DOUBLE));
        basicTypes.add(Schema.create(Schema.Type.BOOLEAN));
        basicTypes.add(Schema.create(Schema.Type.NULL));

        Schema arraySchema = Schema.createArray(Schema.createUnion(basicTypes));
        Schema mapSchema = Schema.createMap(Schema.createUnion(basicTypes));

        List<Schema> allTypes = new ArrayList<>(basicTypes);
        allTypes.add(arraySchema);
        allTypes.add(mapSchema);

        return SchemaBuilder.record("DataAvro")
                .namespace("org.example.avro.model")
                .fields()
                .name("data")
                .type(Schema.createMap(Schema.createUnion(allTypes)))
                .noDefault()
                .endRecord();
    }
}
