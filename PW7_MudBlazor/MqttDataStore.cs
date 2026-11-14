namespace PW7_MudBlazor.Services;

public sealed class MqttDataStore
{
    private readonly object _sync = new();
    private DoorReading _current = new("--", 0, false, false, false, DateTime.MinValue, false);
    private string _rawPayload = "No message received";
    private string _mqttStatus = "Waiting for MQTT broker";
    public event Action? OnChange;
    public DoorReading Current { get { lock (_sync) return _current; } }
    public string RawPayload { get { lock (_sync) return _rawPayload; } }
    public string MqttStatus { get { lock (_sync) return _mqttStatus; } }
    public string DeviceId => Current.DeviceId;
    public double Voltage => Current.Voltage;
    public DateTime LastUpdated => Current.LastUpdated;

    public void SetMqttStatus(string status)
    {
        lock (_sync) _mqttStatus = status;
        OnChange?.Invoke();
    }
    public void UpdateReading(string deviceId, double voltage, bool sensor, bool button,
        bool doorOpen, string rawPayload, bool manualMode = false, bool hasManualMode = false)
    {
        lock (_sync)
        {
            _rawPayload = rawPayload;
            _current = new DoorReading(deviceId, voltage, sensor, button, doorOpen,
                DateTime.Now, true, manualMode, hasManualMode);
        }
        OnChange?.Invoke();
    }
    public void UpdateReading(string deviceId, double voltage)
    {
        var existing = Current;
        UpdateReading(deviceId, voltage, existing.Sensor, existing.Button,
            existing.DoorOpen, RawPayload, existing.ManualMode, existing.HasManualMode);
    }
}

public sealed record DoorReading(string DeviceId, double Voltage, bool Sensor, bool Button,
    bool DoorOpen, DateTime LastUpdated, bool HasData, bool ManualMode = false,
    bool HasManualMode = false);
