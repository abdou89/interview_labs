// =============================================================================
// Code .NET avec problèmes de performance à identifier par le candidat
// Contexte: API de gestion de commandes pour un système à haute charge
// =============================================================================

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using System.Data.SqlClient;
using Dapper;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

string connectionString = "Server=localhost;Database=Demo;Trusted_Connection=True;";

// -----------------------------------------------------------------------------
// ENDPOINT 1: Récupération des commandes récentes
// -----------------------------------------------------------------------------
app.MapGet("/orders", async () =>
{
    using var conn = new SqlConnection(connectionString);
    var data = await conn.QueryAsync("SELECT TOP 1000 * FROM Orders ORDER BY OrderDate DESC");
    return Results.Ok(data);
});

// -----------------------------------------------------------------------------
// ENDPOINT 2: Traitement par lot
// -----------------------------------------------------------------------------
app.MapPost("/orders/process-batch", async (List<int> orderIds) =>
{
    var results = new List<string>();
    
    foreach (var orderId in orderIds)
    {
        // Anti-pattern: appel synchrone bloquant dans une boucle
        var result = ProcessOrderAsync(orderId).Result;
        results.Add(result);
    }
    
    return Results.Ok(results);
});

async Task<string> ProcessOrderAsync(int orderId)
{
    await Task.Delay(100); // Simule un traitement
    return $"Processed {orderId}";
}

// -----------------------------------------------------------------------------
// ENDPOINT 3: Recherche de clients - 
// -----------------------------------------------------------------------------
app.MapGet("/customers/{customerId}/orders", async (int customerId) =>
{
    using var conn = new SqlConnection(connectionString);
    
    var customer = await conn.QueryFirstOrDefaultAsync(
        "SELECT * FROM Customers WHERE CustomerId = @Id", 
        new { Id = customerId });
    
    if (customer == null) return Results.NotFound();
    
    var orders = await conn.QueryAsync(
        "SELECT * FROM Orders WHERE CustomerId = @Id", 
        new { Id = customerId });
    
     
    var summary = "";
    foreach (var order in orders)
    {
        summary += $"Order {order.OrderId}: {order.Amount}\n";
    }
    
    return Results.Ok(new { Customer = customer, Orders = orders, Summary = summary });
});

// -----------------------------------------------------------------------------
// ENDPOINT 4: Export de données volumineuses
 
// -----------------------------------------------------------------------------
app.MapGet("/orders/export", async () =>
{
    using var conn = new SqlConnection(connectionString);
    
    // Anti-pattern: charge tout en mémoire avant de sérialiser
    var allOrders = await conn.QueryAsync(
        "SELECT * FROM Orders WHERE OrderDate >= DATEADD(year, -1, GETDATE())");
    
     var json = JsonSerializer.Serialize(allOrders.ToList());
    
    return Results.Text(json, "application/json");
});

 

// -----------------------------------------------------------------------------
// ENDPOINT 5: Mise à jour en masse  
// -----------------------------------------------------------------------------
app.MapPost("/orders/update-status", async (List<OrderStatusUpdate> updates) =>
{
    using var conn = new SqlConnection(connectionString);
    await conn.OpenAsync();
    
    using var transaction = conn.BeginTransaction();
    
    try
    {
        // Anti-pattern: une requête par ligne dans une transaction longue
        foreach (var update in updates)
        {
            await conn.ExecuteAsync(
                "UPDATE Orders SET Status = @Status, ModifiedDate = GETDATE() WHERE OrderId = @OrderId",
                new { update.OrderId, update.Status },
                transaction);
        }
        
        transaction.Commit();
        return Results.Ok("Updated");
    }
    catch
    {
        transaction.Rollback();
        throw;
    }
});

app.Run();

// DTOs
public record OrderStatusUpdate(int OrderId, string Status);
