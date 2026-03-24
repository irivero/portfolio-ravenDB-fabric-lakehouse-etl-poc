function flattenJSON(obj, prefix = "", result = {}) {
    for (const key in obj) {
        const value = obj[key];
        const sanitizedKey = key.replace(/@/g, "");
        const newKey = prefix ? `${prefix}.${sanitizedKey}` : sanitizedKey;


        if (value !== null && typeof value === "object" && !Array.isArray(value)) {
            flattenJSON(value, newKey, result);
        } else {
            result[newKey] = value;
        }
    }
    return result;
}

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
    case "Salesorders":
        loadToSalesorders(noPartition(), flattened);
        break;

    default:

        break;
}
