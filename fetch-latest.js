// npm install @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");

const REGION = "eu-north-1"; // update if your table is in a different region

const client = DynamoDBDocumentClient.from(new DynamoDBClient({ region: REGION }));

async function getLatestData() {
    const params = {
        TableName: "SensorData",
        KeyConditionExpression: "device_id = :id",
        ExpressionAttributeValues: { ":id": "raspberrypi-sim-01" },
        ScanIndexForward: false, // latest first
        Limit: 1
    };
    try {
        const data = await client.send(new QueryCommand(params));
        console.log("Latest:", data.Items[0]);
    } catch (err) {
        console.error("Error fetching data:", err);
    }
}

getLatestData();