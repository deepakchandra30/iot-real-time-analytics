const express = require('express');
const cors = require('cors');
const app = express();
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand, ScanCommand } = require("@aws-sdk/lib-dynamodb");
const csvStringify = require('csv-stringify/sync'); // npm install csv-stringify

const client = DynamoDBDocumentClient.from(new DynamoDBClient({ region: "eu-north-1" }));
const TABLE_NAME = "SensorData";

// Enable CORS (for easy dev/testing/multi-frontend)
app.use(cors());

// Serve static files from the public directory
app.use(express.static(__dirname + "/public"));

// Helper for consistent sensor fields
const SENSOR_FIELDS = ['temperature', 'humidity', 'pressure', 'latitude', 'longitude'];

// Utility: flatten item for UI
function flatten(item) {
    if (!item) return {};
    const flat = { ...item, ...item.payload };
    delete flat.payload;
    return flat;
}

// GET: Unique device IDs
app.get('/api/devices', async (req, res) => {
    try {
        const params = { TableName: TABLE_NAME };
        const data = await client.send(new ScanCommand(params));
        const devices = new Set(data.Items.map(item => item.device_id).filter(Boolean));
        res.json(Array.from(devices));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET: Latest value for device (default: first device found)
app.get('/api/latest', async (req, res) => {
    try {
        let deviceId = req.query.device_id;
        if (!deviceId) {
            // Find any existing device (fallback)
            const tmp = await client.send(new ScanCommand({ TableName: TABLE_NAME, Limit: 1 }));
            deviceId = tmp.Items[0]?.device_id || "";
        }
        if (!deviceId) return res.json({});
        const params = {
            TableName: TABLE_NAME,
            KeyConditionExpression: "device_id = :id",
            ExpressionAttributeValues: { ":id": deviceId },
            ScanIndexForward: false,
            Limit: 1
        };
        const data = await client.send(new QueryCommand(params));
        res.json(flatten(data.Items && data.Items[0]));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET: Aggregate min/max/avg for each sensor, for last hour (or provided range)
app.get('/api/aggregate', async (req, res) => {
    try {
        let { device_id, from, to } = req.query;
        if (!device_id) {
            // fallback: get any device
            const tmp = await client.send(new ScanCommand({ TableName: TABLE_NAME, Limit: 1 }));
            device_id = tmp.Items[0]?.device_id || "";
        }
        const toDate = to ? new Date(to) : new Date();
        const fromDate = from ? new Date(from) : new Date(toDate.getTime() - 60 * 60 * 1000);
        const params = {
            TableName: TABLE_NAME,
            KeyConditionExpression: "device_id = :id AND #ts BETWEEN :from AND :to",
            ExpressionAttributeNames: { "#ts": "timestamp" },
            ExpressionAttributeValues: {
                ":id": device_id,
                ":from": fromDate.toISOString(),
                ":to": toDate.toISOString()
            },
            ScanIndexForward: true
        };
        const data = await client.send(new QueryCommand(params));
        const result = {};
        SENSOR_FIELDS.forEach(f => {
            const values = (data.Items || []).map(item => Number(item.payload?.[f])).filter(v => !isNaN(v));
            if (values.length) {
                result[f] = {
                    min: Math.min(...values),
                    max: Math.max(...values),
                    avg: (values.reduce((a, b) => a + b) / values.length).toFixed(2),
                    count: values.length
                };
            }
        });
        res.json(result);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET: Full chart history data (default last hour, or pass ?from=...&to=...)
app.get('/api/history', async (req, res) => {
    try {
        let { device_id, from, to } = req.query;
        if (!device_id) {
            const tmp = await client.send(new ScanCommand({ TableName: TABLE_NAME, Limit: 1 }));
            device_id = tmp.Items[0]?.device_id || "";
        }
        const toDate = to ? new Date(to) : new Date();
        const fromDate = from ? new Date(from) : new Date(toDate.getTime() - 60 * 60 * 1000);
        const params = {
            TableName: TABLE_NAME,
            KeyConditionExpression: "device_id = :id AND #ts BETWEEN :from AND :to",
            ExpressionAttributeNames: { "#ts": "timestamp" },
            ExpressionAttributeValues: {
                ":id": device_id,
                ":from": fromDate.toISOString(),
                ":to": toDate.toISOString()
            },
            ScanIndexForward: true
        };
        const data = await client.send(new QueryCommand(params));
        // Structure: per-sensor array
        const out = {};
        SENSOR_FIELDS.forEach(sensor => {
            out[sensor] = (data.Items || []).map(item => ({
                timestamp: item.timestamp,
                value: item.payload?.[sensor] && Number(item.payload?.[sensor])
            })).filter(v => !isNaN(v.value));
        });
        res.json(out);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET: Full raw table, or for specific device (caution: can be large!)
app.get('/api/raw', async (req, res) => {
    try {
        let { device_id } = req.query;
        const params = { TableName: TABLE_NAME };
        const data = await client.send(new ScanCommand(params));
        let items = data.Items.map(flatten);
        if (device_id) items = items.filter(item => item.device_id === device_id);
        res.json(items);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET: CSV export for history or raw data
app.get('/api/csv', async (req, res) => {
    try {
        let { device_id, from, to } = req.query;
        if (!device_id) {
            const tmp = await client.send(new ScanCommand({ TableName: TABLE_NAME, Limit: 1 }));
            device_id = tmp.Items[0]?.device_id || "";
        }
        const toDate = to ? new Date(to) : new Date();
        const fromDate = from ? new Date(from) : new Date(toDate.getTime() - 60 * 60 * 1000);
        const params = {
            TableName: TABLE_NAME,
            KeyConditionExpression: "device_id = :id AND #ts BETWEEN :from AND :to",
            ExpressionAttributeNames: { "#ts": "timestamp" },
            ExpressionAttributeValues: {
                ":id": device_id,
                ":from": fromDate.toISOString(),
                ":to": toDate.toISOString()
            },
            ScanIndexForward: true
        };
        const data = await client.send(new QueryCommand(params));
        const rows = (data.Items || []).map(flatten);
        const csv = csvStringify.stringify(rows, { header: true });
        res.header('Content-Type', 'text/csv');
        res.attachment(`sensor_export_${device_id}.csv`);
        res.send(csv);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 404 fallback
app.use((req, res) => res.status(404).json({error: "Not found"}));

app.listen(3000, () => console.log('Dashboard backend running on http://localhost:3000'));