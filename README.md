# RavenDB → Microsoft Fabric Lakehouse ETL (POC)
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
