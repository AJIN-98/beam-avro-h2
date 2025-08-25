package com.example.beam;

import com.example.beam.model.PersonEnc;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Objects;

@Slf4j
@Service
public class BeamService {

    private final String jdbcUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private final String keyB64 = System.getenv().getOrDefault("CRYPTO_KEY_B64",
            "5p0W2z2nT4oQz7mU2G8w9W8Tg9bq1pK8bq1m4l0mS8E=");

    public void runPipeline() throws Exception {
        // Create H2 table
        DbInit.createTable(jdbcUrl);

        // Generate temporary encrypted Avro file
        Path tmpDir = Files.createTempDirectory("beam-avro");
        Path inputAvro = tmpDir.resolve("people.enc.avro");

        Schema schema = loadSchema("/person.avsc");
        generateSampleEncryptedAvro(schema, inputAvro.toFile(), keyB64);

        // Create Beam pipeline
        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        p.apply("ReadAvro", AvroIO.readGenericRecords(schema)
                        .from(inputAvro.toAbsolutePath().toString()))
                .apply("Transform", ParDo.of(new PersonTransformFn(keyB64)))
                .setCoder(SerializableCoder.of(PersonEnc.class))
                .apply("WriteH2", JdbcIO.<PersonEnc>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "org.h2.Driver", jdbcUrl)
                                .withUsername("sa").withPassword(""))
                        .withStatement("MERGE INTO people_enc (id, name_enc, email_enc) VALUES (?, ?, ?)")
                        .withPreparedStatementSetter((pRec, ps) -> {
                            ps.setLong(1, pRec.getId());
                            ps.setString(2, pRec.getNameEnc());
                            ps.setString(3, pRec.getEmailEnc());
                        }));

        p.run().waitUntilFinish();

        // Verify H2 contents
        try (Connection c = DriverManager.getConnection(jdbcUrl, "sa", "");
             Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT id, name_enc, email_enc FROM people_enc ORDER BY id");
            log.info("=== H2 contents (decrypted) ===");
            while (rs.next()) {
                long id = rs.getLong(1);
                String n = rs.getString(2);
                String e = rs.getString(3);
                String name = CryptoUtil.decryptFromB64(n, keyB64);
                String email = CryptoUtil.decryptFromB64(e, keyB64);
                log.info("id={}, name={}, email={}", id, name, email);
            }
        }
    }

    private Schema loadSchema(String resource) throws Exception {
        try (InputStream in = getClass().getResourceAsStream(resource)) {
            if (in == null) throw new IllegalStateException("Schema not found: " + resource);
            return new Schema.Parser().parse(new String(in.readAllBytes()));
        }
    }

    private void generateSampleEncryptedAvro(Schema schema, File outFile, String keyB64) throws Exception {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dfw = new DataFileWriter<>(writer)) {
            dfw.create(schema, outFile);
            write(dfw, schema, 1L, "Alice", "Alice@example.com", keyB64);
            write(dfw, schema, 2L, "Bob",   "Bob@Example.com",   keyB64);
            write(dfw, schema, 3L, "Carol", "Carol@Example.com", keyB64);
        }
        log.info("Generated encrypted Avro at: {}", outFile.getAbsolutePath());
    }

    private void write(DataFileWriter<GenericRecord> dfw, Schema schema,
                       long id, String name, String email, String keyB64) throws Exception {
        GenericRecord rec = new GenericData.Record(schema);
        rec.put("id", id);
        rec.put("name_enc", CryptoUtil.encryptToB64(name, keyB64));
        rec.put("email_enc", CryptoUtil.encryptToB64(email, keyB64));
        dfw.append(rec);
    }

    // Beam DoFn to transform GenericRecord -> PersonEnc
    public static class PersonTransformFn extends DoFn<GenericRecord, PersonEnc> {
        private final String keyB64;

        public PersonTransformFn(String keyB64) {
            this.keyB64 = keyB64;
        }

        @ProcessElement
        public void processElement(@Element GenericRecord r, OutputReceiver<PersonEnc> out) {
            long id = (Long) r.get("id");
            String nameEnc = Objects.toString(r.get("name_enc"), "");
            String emailEnc = Objects.toString(r.get("email_enc"), "");

            String name = CryptoUtil.decryptFromB64(nameEnc, keyB64);
            String email = CryptoUtil.decryptFromB64(emailEnc, keyB64);

            String nameX = name.trim().toUpperCase();
            String emailX = email.toLowerCase();

            String nameRe = CryptoUtil.encryptToB64(nameX, keyB64);
            String emailRe = CryptoUtil.encryptToB64(emailX, keyB64);

            out.output(new PersonEnc(id, nameRe, emailRe));
        }
    }
}
