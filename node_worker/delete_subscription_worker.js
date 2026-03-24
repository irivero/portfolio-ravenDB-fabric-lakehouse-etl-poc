"use strict";

const fs = require("fs");
const { DocumentStore } = require("ravendb");
const { DataLakeServiceClient, StorageSharedKeyCredential } = require("@azure/storage-file-datalake");

// ─── Configuracion RavenDB ───────────────────────────────────────────────────
const RAVENDB_URL       = process.env.RAVENDB_URL       || "https://your_ravendb_url.ravendb.cloud/";
const DATABASE_NAME     = process.env.DATABASE_NAME     || "your_ravendb";
const SUBSCRIPTION_NAME = process.env.SUBSCRIPTION_NAME || "YOUR_SUBSCRIPTION";
const COLLECTION        = process.env.COLLECTION        || "Your_Collection";
const CERT_PATH         = process.env.RAVEN_CERT_PATH   || "./certs/path/cert.pem";

// ─── Configuracion ADLS ──────────────────────────────────────────────────────
const ADLS_ACCOUNT_NAME = process.env.ADLS_ACCOUNT_NAME || "your_adls_storage";
const ADLS_ACCOUNT_KEY  = process.env.ADLS_ACCOUNT_KEY  || "account_key";
const ADLS_FILESYSTEM   = process.env.ADLS_FILESYSTEM   || "adls_container";
const ADLS_BASE_PATH    = process.env.ADLS_BASE_PATH    || "adls_folder";

// ─── ADLS: cliente ───────────────────────────────────────────────────────────
const adlsClient = new DataLakeServiceClient(
    `https://${ADLS_ACCOUNT_NAME}.dfs.core.windows.net`,
    new StorageSharedKeyCredential(ADLS_ACCOUNT_NAME, ADLS_ACCOUNT_KEY)
);

// Genera el path del archivo para la hora actual
// Ejemplo: deletes/Customers/2026-03-23-11.csv
function getHourlyFilePath(collection) {
    const now = new Date();
    const y  = now.getUTCFullYear();
    const mo = String(now.getUTCMonth() + 1).padStart(2, "0");
    const d  = String(now.getUTCDate()).padStart(2, "0");
    const h  = String(now.getUTCHours()).padStart(2, "0");
    return `${ADLS_BASE_PATH}/${collection}/${y}-${mo}-${d}-${h}.csv`;
}

// Buffer de eventos pendientes de escritura
const pendingDeletes = [];
const CSV_HEADER = "timestamp,id,collection,changeVector\n";

// Escribe los deletes acumulados en ADLS (flush cada 15 segundos)
async function flushToAdls() {
    if (pendingDeletes.length === 0) return;

    const items = pendingDeletes.splice(0);
    const byCollection = {};

    for (const item of items) {
        if (!byCollection[item.collection]) byCollection[item.collection] = [];
        byCollection[item.collection].push(item);
    }

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
}

// Flush cada 15 segundos
const flushTimer = setInterval(flushToAdls, 15_000);

// ─── RavenDB: store ──────────────────────────────────────────────────────────
const store = new DocumentStore(RAVENDB_URL, DATABASE_NAME, {
    type: "pem",
    certificate: fs.readFileSync(CERT_PATH),
});
store.initialize();

// ─── [2] CHANGES API: deletes ─────────────────────────────────────────────────
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

// ─── Cierre limpio ────────────────────────────────────────────────────────────
async function shutdown(signal) {
    console.log("Signal " + signal + " recibida. Cerrando...");
    clearInterval(flushTimer);
    await flushToAdls();   // flush final antes de salir
    try {
        changesClient.dispose();
        await worker.dispose();
        store.dispose();
    } catch (err) {
        console.error("Error al cerrar:", err);
    }
    console.log("Cerrado correctamente.");
    process.exit(0);
}

process.on("SIGINT",  () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

console.log("Worker iniciado");
console.log("   RavenDB:       " + RAVENDB_URL + " | DB: " + DATABASE_NAME);
console.log("   Subscription:  " + SUBSCRIPTION_NAME);
console.log("   ADLS:          " + ADLS_ACCOUNT_NAME + ".dfs.core.windows.net / " + ADLS_FILESYSTEM + "/" + ADLS_BASE_PATH);
console.log("   Archivos:      " + ADLS_BASE_PATH + "/{Coleccion}/YYYY-MM-DD-HH.csv");
