# RavenDB → Microsoft Fabric Lakehouse ETL (POC) by IntegrIdia
#### Dynamic Schema Evolution, Automated Flattening, Centralized Transformations & Hard Delete Detection

## 📌 Project Overview
This repository contains a full Proof of Concept (POC) validating the integration between RavenDB’s OLAP ETL and Microsoft Fabric Lakehouse.
The objective was to design and validate a fully automated, schema-flexible, scalable ETL pipeline capable of supporting analytical use cases across multiple collections.
This POC addresses four critical requirements:

1. Automatic incremental full load
2. Automatic detection of new fields (schema evolution)
3. Automated flattening for nested JSON structures
4. Centralized transformation logic for simple collections
5. Hard delete detection using RavenDB Subscriptions (External Worker)

## 🧱 Architecture Overview
                      RavenDB (NoSQL)
                           │
                           │  OLAP ETL (Parquet auto-schema)
                           ▼
                 Azure Data Lake Storage (ADLS)
                           │
                           │ Shortcuts / OneLake
                           ▼
                 Microsoft Fabric Lakehouse
                   ├── Bronze (raw parquet)
                   ├── Silver (Delta tables – auto schema)
                   └── Gold (BI-ready)
                           
          ┌───────────────────────────────────────────────────────┐
          │            Hard Delete Worker (Node.js)               │
          │  RavenDB Subscription → Deleted items log  in ADLS    │
          └───────────────────────────────────────────────────────┘

## 🚀 1. Automatic Detection of New Columns (Schema Evolution)
RavenDB’s OLAP ETL was configured to export documents without relying on a predefined schema, enabling:

1. Automatic detection of new fields
2. Automatic creation of new columns in Parquet
3. Seamless ingestion into Fabric Delta tables

##### Fabric Notebook

```python
    for file_path, file_ts in files_to_process:
    print(f"\n→ Processing: {file_path.split('/')[-1]}")

    df_file = (
        spark.read.format("parquet")
        .load(file_path)
        .withColumn("_source_file", F.lit(file_path))
        .withColumn("file_timestamp", F.to_timestamp(F.lit(str(file_ts)), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
    )

    row_count = df_file.count()
    print(f"   Rows in file: {row_count}")

    if DeltaTable.isDeltaTable(spark, silver_table_path):
        delta_table = DeltaTable.forPath(spark, silver_table_path)
        # Build the SET clause dynamically from source columns only.
        # Backtick-escape names with dots/spaces so Delta doesn't interpret them as struct accessors.
        def q(col_name: str) -> str:
            return f"`{col_name}`"

        update_cols = {q(c): f"source.{q(c)}" for c in df_file.columns}
        # Cast key to string on both sides to avoid type mismatch silently skipping updates
        merge_condition = f"CAST(target.{upsert_key} AS STRING) = CAST(source.{upsert_key} AS STRING)"
        (
            delta_table.alias("target")
            .merge(df_file.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_cols)
            .whenNotMatchedInsert(values=update_cols)
            .execute()
        )
    else:
        # First file: create the Delta table
        df_file.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_table_path)

    total_upserted += row_count
    print(f"   ✓ Upserted {row_count} rows")
```
  
This confirmed that Microsoft Fabric automatically recognizes and registers schema changes.

## 🚀 2. Automated Flattening of Nested JSON
✔ Validated
A custom flattenJSON() function was implemented to standardize transformation of nested objects.
Features:

1. Recursive traversal of nested JSON
2. Column naming pattern: level1.level2.level3
3. Special character cleanup (@)
4. Array exclusion (extendable if needed)


##### RavenDB OLAP ETL Transformation
```JavaScript
    function flattenJSON(obj, prefix = "", result = {}) {
        for (const key in obj) {
            const value = obj[key];
            const sanitizedKey = key.replace(/@/g, "");
            const newKey = prefix ? `${prefix}.${sanitizedKey}` : sanitizedKey;
    
            // Recursión para objetos, excepto arrays
            if (value !== null && typeof value === "object" && !Array.isArray(value)) {
                flattenJSON(value, newKey, result);
            } else {
                result[newKey] = value;
            }
        }
        return result;
    }   
   
}
```
The result is a clean, fully column-ready structure ideal for Delta Lake.

## 🚀 3. Centralized Transformations Per Collection
✔ Validated
A routing switch was added to simplify processing of multiple collections:

```JavaScript
   ...

    var meta = getMetadata(this);
    var collection = meta["@collection"];
    
    var doc = Object.assign({}, this);
    doc["metadata"] = meta;
    
    var flattened = flattenJSON(doc);
    
    switch (collection) {
    
        case "Customers":
            loadToCustomers(noPartition(), flattened);
            break;
    
        case "Employees":
            loadToEmployees(noPartition(), flattened);
            break;
    
        case "Invoices":
            loadToInvoices(noPartition(), flattened);
            break;
    
        case "Products":
            loadToProducts(noPartition(), flattened);
            break;
    
        case "Suppliers":
            loadToSuppliers(noPartition(), flattened);
            break;
        default:
            break;
    }

```
##### Benefits:

One single transformation block (flattening + metadata)
No duplicated logic
Easy onboarding of new collections

## 🚀 4. Hard Delete Detection Using a RavenDB Subscription Worker
✔ Validated (External Mechanism)
Since RavenDB OLAP ETL does not handle deletes, I implemented a Node.js worker using RavenDB Subscriptions.
The worker:

1. Listens for real-time Delete events
2. Extracts document ID and collection
3. Writes deletion logs to ADLS
4. Enables Fabric to synchronize deletions in analytical tables

```JavaScript
  const changesClient = store.changes(DATABASE_NAME);

  changesClient.forDocumentsInCollection(COLLECTION)
      .on("data", (change) => {
          if (change.type !== "Delete") return;
  
          const record = {
              timestamp:     new Date().toISOString(),
              id:            change.id,
              collection:    COLLECTION,
              changeVector:  change.changeVector ?? "",
          };
  
          pendingDeletes.push(record);
          console.log("DELETE detectado -> encolado | ID:", change.id);
      })
      .on("error", (err) => {
          console.error("ERROR Changes API:", err.message ?? err);
      });

    ...

    for (const [collection, records] of Object.entries(byCollection)) {
        try {
            const filePath   = getHourlyFilePath(collection);
            const fsClient   = adlsClient.getFileSystemClient(ADLS_FILESYSTEM);
            const fileClient = fsClient.getFileClient(filePath);

            let offset = 0;

            // Si el archivo no existe, lo crea con header CSV
            try {
                const props = await fileClient.getProperties();
                offset = props.contentLength;
            } catch (_) {
                await fileClient.create();
                const headerBuf = Buffer.from(CSV_HEADER);
                await fileClient.append(headerBuf, 0, headerBuf.length);
                await fileClient.flush(headerBuf.length);
                offset = headerBuf.length;
            }

            // Escribe las lineas nuevas
            const lines = records
                .map(r => `"${r.timestamp}","${r.id}","${r.collection}","${r.changeVector || ""}"`)
                .join("\n") + "\n";

            const dataBuf = Buffer.from(lines);
            await fileClient.append(dataBuf, offset, dataBuf.length);
            await fileClient.flush(offset + dataBuf.length);

            console.log(`ADLS: ${records.length} delete(s) -> ${filePath}`);
        } catch (err) {
            console.error(`Error escribiendo en ADLS [${collection}]:`, err.message);
        }
    }
  ```

## 📊 POC Results Summary

| Capability              | Status | Description                               |
|-------------------------|--------|-------------------------------------------|
| Schema Evolution        | ✔      | Fully automatic column detection          |
| JSON Flattening         | ✔      | Recursive, reusable transform             |
| Centralized ETL Logic   | ✔      | One shared logic block                    |
| Hard Delete Detection   | ✔      | Via external subscription worker          |

## Automated tests
I have created a notebook for the automated tests. This notebook is a comprehensive test suite for the silver_{table} Delta table that validates data quality and schema integrity. It performs six categories of assertions: (1) verifies the table exists and contains records, 
(2) inspects the schema to ensure no complex types (StructType, MapType, ArrayType) remain from the source, indicating proper flattening, 
(3) samples all string columns to detect JSON-serialized values that should have been normalized, 
(4) validates the upsert key (CustomerId) has no NULL values or duplicates, and 
(5) compiles all test results into a styled pandas DataFrame with color-coded PASS/FAIL indicators. 

The notebook is designed to run as a quality gate in a data pipeline, hard-failing (via assertion) if any test fails, ensuring downstream consumers receive properly flattened, normalized data without nested structures.
<img width="664" height="550" alt="image" src="https://github.com/user-attachments/assets/18f2fae1-aaa1-4f19-ae20-7f62e22fdb73" />

## 🧰 Technologies Used

##### RavenDB (OLAP ETL, Subscriptions)
##### Azure Data Lake Storage (ADLS)
##### Microsoft Fabric Lakehouse (Delta, PySpark, Notebooks)
##### Node.js
##### Python
##### Parquet & Delta Lake


## 📁 Repository Structure

```
portfolio-raven-fabric-etl-poc/
│
├── notebooks/
│   ├── validation_schema_evolution.ipynb
│   └── flattening_tests.ipynb
│
├── node_worker/
│   └── delete_subscription_worker.js
│
├── etl_scripts/
│   └── flatten_json.js
│
│
└── README.md

```

## 🧠 Key Learnings

How to design schema-agnostic pipelines for NoSQL sources
Best practices for Lakehouse ingestion in Fabric
Working with real-time event-driven architectures
Data modeling considerations for Delta Lake


## 📌 Next Steps

Add array processing strategy in flattening
Add Azure Functions for orchestration
Convert delete worker into a microservice


## ✨ Author
### IntegrIdia
### 📧 idia.herrera@gmail.com
🔗 GitHub: irivero


## Keywords
- RavenDB
- Microsoft Fabric
- Delta Lake
- ETL Automation
- JSON Flattening
- Hard Delete Detection
- Data Engineering
- Event-Driven Architecture
- Node.js Worker
- PySpark
- ADLS
- Lakehouse
