using MQTTnet;
using System.Text.Json;

namespace PW7_MudBlazor.Services;

public sealed class MqttBackgroundService : BackgroundService
{
    private readonly MqttDataStore _store;
    private const string Topic = "beaver-iot/mqtt@default/mqtt-device/testkok001";
    public MqttBackgroundService(MqttDataStore store) => _store = store;

    private static bool ReadBool(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var value)) return false;
        return value.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.Number => value.TryGetDouble(out var n) && n != 0,
            JsonValueKind.String => value.GetString() is "1" or "true" or "TRUE",
            _ => false
        };
    }
    private static double ReadNumber(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var value)) return 0;
        if (value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var number)) return number;
        if (value.ValueKind == JsonValueKind.String &&
            double.TryParse(value.GetString(), System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var parsed)) return parsed;
        return 0;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mqttFactory = new MqttClientFactory();
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var mqttClient = mqttFactory.CreateMqttClient();
                mqttClient.ApplicationMessageReceivedAsync += e =>
                {
                    try
                    {
                        using var doc = JsonDocument.Parse(e.ApplicationMessage.ConvertPayloadToString());
                        var root = doc.RootElement;
                        var id = root.TryGetProperty("device_id", out var idValue)
                            ? idValue.GetString() ?? "--" : "--";
                        var voltage = ReadNumber(root, "voltage");
                        var sensor = ReadBool(root, "sensor_pnp");
                        var button = ReadBool(root, "push_button");
                        var door = root.TryGetProperty("door_open", out _)
                            ? ReadBool(root, "door_open")
                            : ReadBool(root, "spillway_gate"); // backward compatibility
                        var hasManualMode = root.TryGetProperty("manual_mode", out _);
                        var manualMode = ReadBool(root, "manual_mode");
                        _store.UpdateReading(id, voltage, sensor, button, door,
                            e.ApplicationMessage.ConvertPayloadToString(), manualMode, hasManualMode);
                        Console.WriteLine($"WAGO data: sensor={(sensor ? 1 : 0)}, button={(button ? 1 : 0)}, door={(door ? 1 : 0)}, voltage={voltage:0.##}");
                    }
                    catch (Exception ex) { Console.WriteLine($"MQTT payload error: {ex.Message}"); }
                    return Task.CompletedTask;
                };

                var options = new MqttClientOptionsBuilder()
                    .WithTcpServer("192.168.101.59", 1883)
                    .WithClientId("pw7-door-" + Guid.NewGuid().ToString("N"))
                    .WithCredentials("mqtt@default", "12345678")
                    .Build();
                _store.SetMqttStatus("Connecting to 192.168.101.59:1883...");
                await mqttClient.ConnectAsync(options, stoppingToken);
                var subscription = mqttFactory.CreateSubscribeOptionsBuilder()
                    .WithTopicFilter(Topic).Build();
                await mqttClient.SubscribeAsync(subscription, stoppingToken);
                _store.SetMqttStatus("Subscribed to " + Topic);
                Console.WriteLine($"Beaver IoT subscribed: {Topic}");
                while (!stoppingToken.IsCancellationRequested && mqttClient.IsConnected)
                    await Task.Delay(1000, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _store.SetMqttStatus("Disconnected / retrying: " + ex.Message);
                Console.WriteLine($"MQTT unavailable, retrying: {ex.Message}");
            }
            try { await Task.Delay(5000, stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }
}
