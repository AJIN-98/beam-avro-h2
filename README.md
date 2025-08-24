# Apache Beam Pipeline: Encrypted Avro → H2 Database

## Overview

This project demonstrates how to use **Apache Beam** to process **encrypted Avro files** and write them to an **H2 in-memory database**.

Key features:

* Read encrypted Avro files with `AvroIO`.
* Decrypt and transform data.
* Re-encrypt and store in H2 database using `JdbcIO`.
* Fully in-memory for easy testing.
* REST endpoint to trigger the pipeline via **Spring Boot**.

---

## Project Structure

```
beam-avro-h2/
├── src/
│   ├── main/
│   │   ├── java/com/example/beam/
│   │   │   ├── BeamService.java
│   │   │   ├── CryptoUtil.java
│   │   │   ├── DbInit.java
│   │   │   ├── PersonEnc.java
│   │   │   ├── PersonTransform.java
│   │   │   └── controller/PipelineController.java
│   │   └── resources/
│   │       └── person.avsc
├── pom.xml
└── README.md
```

---

## Setup & Prerequisites

* Java 17+
* Maven 3.8+
* Spring Boot 3.x
* Apache Beam 2.63.0
* H2 Database (in-memory)

**Optional Environment Variable**:

```bash
export CRYPTO_KEY_B64=<base64-encoded-key>
```

---

## Avro Schema (`person.avsc`)

```json
{
  "type": "record",
  "name": "Person",
  "namespace": "com.example.beam.model",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name_enc", "type": "string"},
    {"name": "email_enc", "type": "string"}
  ]
}
```

---

## Pipeline Walkthrough

1. **Generate Encrypted Avro**

    * Creates `people.enc.avro` with encrypted fields.
2. **Read Avro**

    * Uses `AvroIO.readGenericRecords(schema)`.
3. **Transform Records**

    * Decrypt → normalize → re-encrypt using `PersonTransform`.
    * Uses `SerializableCoder` to avoid Beam serialization issues.
4. **Write to H2 Database**

    * `JdbcIO.write()` with upsert (`MERGE INTO`) into `people_enc`.
5. **Query H2 (Optional)**

    * Verify decrypted contents in H2.

---

## Sample REST Endpoint

```bash
POST http://localhost:8080/pipeline/run
```

**Controller:**

```java
@RestController
@RequestMapping("/pipeline")
public class PipelineController {
    private final BeamService beamService;
    public PipelineController(BeamService beamService) { this.beamService = beamService; }

    @PostMapping("/run")
    public String runPipeline() throws Exception {
        beamService.runPipeline();
        return "Pipeline executed successfully!";
    }
}
```

---

## Build & Run

```bash
# Build the project
mvn clean package

# Run Spring Boot app
mvn spring-boot:run

# Trigger pipeline
curl -X POST http://localhost:8080/pipeline/run
```

---

## Notes & Best Practices

* **Serialization**: Ensure `DoFn` does not capture non-serializable fields.
* **H2 Database**: Use `DB_CLOSE_DELAY=-1` to keep in-memory DB alive.
* **Encryption Key**: Use environment variables or secret manager in production.
* **Cleanup**: Delete temporary Avro files after pipeline completion.

---

## Example Output

```
Generated encrypted Avro at: C:\Temp\people.enc.avro

=== H2 contents (decrypted) ===
id=1, name=ALICE, email=alice@example.com
id=2, name=BOB, email=bob@example.com
id=3, name=CAROL, email=carol@example.com
```

---

## Key Learnings

* Reading and writing **Avro files in Beam**.
* Using **JdbcIO** for database writes.
* Handling **encryption/decryption** inside Beam pipelines.
* Managing **serialization issues** with Beam `DoFn`.
* Integrating **Spring Boot** with Apache Beam.
