using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Text.Json;

namespace FuncCosmosDemo;

public class CreateOrder
{
    public record Order(
        string id,
        string customerId,
        decimal total,
        string[] items,
        DateTime createdUtc
    );

    [Function("CreateOrder")]
    public static async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "orders")] HttpRequestData req, // Anonymous para probar local
        FunctionContext ctx)
    {
        var log = ctx.GetLogger("CreateOrder");
        var dbName = Environment.GetEnvironmentVariable("CosmosDatabase") ?? "appdb";
        var cont = Environment.GetEnvironmentVariable("CosmosContainer") ?? "orders";

        // --- Body ---
        var body = await new StreamReader(req.Body).ReadToEndAsync();
        if (string.IsNullOrWhiteSpace(body))
            return await Bad(req, "Empty body");

        Order order;
        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;

            var id = root.TryGetProperty("id", out var _id) && _id.ValueKind == JsonValueKind.String
                ? _id.GetString()!
                : Guid.NewGuid().ToString();

            var customerId = root.GetProperty("customerId").GetString()!;
            var total = Convert.ToDecimal(root.GetProperty("total").GetString());

            var items = JsonSerializer.Deserialize<string[]?>(root.GetProperty("items").GetString());

            order = new Order(id, customerId, total, items, DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            return await Bad(req, $"Invalid JSON: {ex.Message}");
        }

        // --- Cosmos client (sin Lazy, con validación y catch general) ---
        var cs = Environment.GetEnvironmentVariable("CosmosConnectionString");
        if (string.IsNullOrWhiteSpace(cs))
            return await Fail(req, "CosmosConnectionString is missing. Define it in local.settings.json (Values) o en App Settings.");

        CosmosClient client;
        try
        {
            client = new CosmosClient(cs);
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Error creating CosmosClient. Check Connection String format (debe empezar con 'AccountEndpoint=' y tener 'AccountKey=').");
            return await Fail(req, $"Cannot create CosmosClient: {ex.Message}");
        }

        try
        {
            var container = client.GetContainer(dbName, cont);
            await container.CreateItemAsync(order, new PartitionKey(order.customerId));
        }
        catch (CosmosException cex)
        {
            log.LogError(cex, "Cosmos error {Status} - {Message}", cex.StatusCode, cex.Message);
            var r = req.CreateResponse(HttpStatusCode.BadRequest);
            await r.WriteStringAsync($"Cosmos error {cex.StatusCode}: {cex.Message}");
            return r;
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Unexpected error writing to Cosmos");
            return await Fail(req, $"Unexpected error: {ex.Message}");
        }

        var ok = req.CreateResponse(HttpStatusCode.Created);
        await ok.WriteAsJsonAsync(order);
        return ok;
    }

    private static async Task<HttpResponseData> Bad(HttpRequestData req, string msg)
    {
        var r = req.CreateResponse(HttpStatusCode.BadRequest);
        await r.WriteStringAsync(msg);
        return r;
    }

    private static async Task<HttpResponseData> Fail(HttpRequestData req, string msg)
    {
        var r = req.CreateResponse(HttpStatusCode.InternalServerError);
        await r.WriteStringAsync(msg);
        return r;
    }
}
