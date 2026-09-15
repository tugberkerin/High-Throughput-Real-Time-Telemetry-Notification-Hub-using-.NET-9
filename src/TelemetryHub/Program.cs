using System.Text.Json;
using System.Text.Json.Serialization;
using TelemetryHub.Endpoints;
using TelemetryHub.Infrastructure.Channels;
using TelemetryHub.Infrastructure.Storage;
using TelemetryHub.Services;

var builder = WebApplication.CreateBuilder(args);

// Configure System.Text.Json high-performance options
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
    options.SerializerOptions.NumberHandling = JsonNumberHandling.AllowReadingFromString;
});

// Configure Telemetry Channel Options & Singleton Registrations
builder.Services.Configure<TelemetryChannelOptions>(
    builder.Configuration.GetSection(TelemetryChannelOptions.SectionName));

builder.Services.AddSingleton<ITelemetryChannel, TelemetryChannel>();
builder.Services.AddSingleton<ITelemetryStorage, TelemetryStorage>();

// Register Background Worker Service Consumer
builder.Services.AddHostedService<TelemetryConsumerService>();

var app = builder.Build();

// Enable static file serving for the interactive Web Dashboard
app.UseDefaultFiles();
app.UseStaticFiles();

// Health check endpoint for container orchestrators & load balancers
app.MapGet("/health", () => Results.Ok(new { status = "Healthy", timestamp = DateTimeOffset.UtcNow }))
   .WithName("HealthCheck")
   .WithTags("Health");

// Map Minimal API telemetry ingestion endpoints
app.MapTelemetryEndpoints();

app.Run();

// Exposed for integration testing (WebApplicationFactory)
public partial class Program { }
