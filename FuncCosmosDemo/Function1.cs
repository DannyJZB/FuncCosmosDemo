using Azure.Storage.Blobs.Specialized;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using System.Text.Json;

namespace FuncCosmosDemo;

public class BlobToCosmos
{
    public record Order(
        string id,
        string customerId,
        decimal total,
        string[] items,
        string blobName,
        string container,
        DateTime createdUtc
    );

    [Function("BlobToCosmos")]
    public static async Task Run(
        // Se dispara cuando hay un blob nuevo en "uploads"
        [BlobTrigger("uploads/{name}", Connection = "AzureWebJobsStorage")] Stream blobStream,
        string name,
        FunctionContext ctx)
    {
        var log = ctx.GetLogger("BlobToCosmos");

        // Leer metadatos del blob
        var storageConn = Environment.GetEnvironmentVariable("AzureWebJobsStorage")
            ?? throw new InvalidOperationException("Falta AzureWebJobsStorage");

        // Usamos BlockBlobClient para leer metadata
        var uriBase = new UriBuilder(new Uri($"https://dummy")).Uri; // no se usa, solo evita warnings
        var client = new BlockBlobClient(storageConn, "uploads", name);
        var props = await client.GetPropertiesAsync();
        var meta = props.Value.Metadata;

        // OBLIGATORIO: customerid y total en metadata
        if (!meta.TryGetValue("customerid", out var customerId) || string.IsNullOrWhiteSpace(customerId))
        {
            log.LogWarning("Blob {name} sin metadata 'customerid'. Se omite.", name);
            return;
        }
        if (!meta.TryGetValue("total", out var totalStr) || !decimal.TryParse(totalStr, out var total))
        {
            log.LogWarning("Blob {name} sin metadata 'total' válido. Se omite.", name);
            return;
        }
        if (!meta.TryGetValue("items", out var items) || string.IsNullOrWhiteSpace(items))
        {
            log.LogWarning("Blob {items} sin metadata 'items' válido. Se omite.", items);
            return;
        }

        string[]? itemsAray = JsonSerializer.Deserialize<string[]?>(items);
        
        var order = new Order(
            id: Guid.NewGuid().ToString(),
            customerId: customerId,
            total: total,
            itemsAray ?? [],
            blobName: name,
            container: "uploads",
            createdUtc: DateTime.UtcNow
        );

        // Insertar en Cosmos
        var cosmosCs = Environment.GetEnvironmentVariable("CosmosConnectionString")
            ?? throw new InvalidOperationException("Falta CosmosConnectionString");
        var dbName = Environment.GetEnvironmentVariable("CosmosDatabase") ?? "appdb";
        var contName = Environment.GetEnvironmentVariable("CosmosContainer") ?? "orders";

        using var cosmos = new CosmosClient(cosmosCs);
        var container = cosmos.GetContainer(dbName, contName);
        await container.CreateItemAsync(order, new PartitionKey(order.customerId));

        log.LogInformation("Insertado en Cosmos: {id} desde blob {blob}", order.id, name);
    }
}
