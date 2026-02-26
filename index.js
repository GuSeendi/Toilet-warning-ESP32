/**
 * ESP32 NOTI — MQTT → FCM Bridge Server
 *
 * Subscribe MQTT topic จาก ESP32 แล้วส่ง push notification
 * ผ่าน Firebase Cloud Messaging (FCM) ไปยังทุกอุปกรณ์ที่ลงทะเบียน
 */

require("dotenv").config();
const mqtt = require("mqtt");
const admin = require("firebase-admin");
const http = require("http");

// ─── Firebase Admin SDK ───────────────────────────────
// รองรับทั้ง env var (Render/Cloud) และไฟล์ (local)
let serviceAccount;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
} else {
  serviceAccount = require(process.env.GOOGLE_APPLICATION_CREDENTIALS || "./service-account.json");
}

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
const messaging = admin.messaging();

// ─── MQTT Config ──────────────────────────────────────
const BROKER = process.env.MQTT_BROKER || "broker.hivemq.com";
const PORT = parseInt(process.env.MQTT_PORT || "1883");
const TOPIC = process.env.MQTT_TOPIC || "esp32/notifications";

console.log("═══════════════════════════════════════════");
console.log("  ESP32 NOTI — MQTT→FCM Bridge Server");
console.log("═══════════════════════════════════════════");
console.log(`  Broker : ${BROKER}:${PORT}`);
console.log(`  Topic  : ${TOPIC}`);
console.log("═══════════════════════════════════════════\n");

// ─── Connect MQTT ─────────────────────────────────────
const client = mqtt.connect(`mqtt://${BROKER}:${PORT}`, {
  clientId: `fcm_bridge_${Date.now()}`,
  clean: true,
});

client.on("connect", () => {
  console.log("✅ MQTT Connected!");
  client.subscribe(TOPIC, (err) => {
    if (err) {
      console.error("❌ Subscribe error:", err);
    } else {
      console.log(`📡 Subscribed to: ${TOPIC}`);
      console.log("⏳ Waiting for messages from ESP32...\n");
    }
  });
});

client.on("error", (err) => {
  console.error("❌ MQTT Error:", err.message);
});

client.on("reconnect", () => {
  console.log("🔄 Reconnecting to MQTT...");
});

// ─── Handle MQTT Messages ─────────────────────────────
client.on("message", async (topic, payload) => {
  const message = payload.toString();
  const timestamp = new Date().toLocaleString("th-TH");

  console.log(`\n📨 [${timestamp}] Message received:`);
  console.log(`   Topic: ${topic}`);
  console.log(`   Data : ${message}`);

  // Parse message (รองรับทั้ง JSON และ plain text)
  let title = "ESP32 Notification";
  let body = message;
  let type = "info";

  try {
    const json = JSON.parse(message);
    title = json.title || title;
    body = json.message || json.body || body;
    type = json.type || type;
  } catch (e) {
    // Plain text message — ใช้ default title
  }

  // ดึง FCM tokens จาก Firestore
  try {
    const tokensSnapshot = await db.collection("fcm_tokens").get();

    if (tokensSnapshot.empty) {
      console.log("⚠️  No registered devices found");
      return;
    }

    const tokens = [];
    tokensSnapshot.forEach((doc) => {
      const data = doc.data();
      if (data.token) {
        tokens.push(data.token);
      }
    });

    if (tokens.length === 0) {
      console.log("⚠️  No valid FCM tokens");
      return;
    }

    console.log(`📱 Sending to ${tokens.length} device(s)...`);

    // ส่ง FCM notification
    const response = await messaging.sendEachForMulticast({
      tokens: tokens,
      notification: {
        title: title,
        body: body,
      },
      data: {
        type: type,
        topic: topic,
        raw: message,
        timestamp: new Date().toISOString(),
      },
      android: {
        priority: "high",
        notification: {
          channelId: "esp32_notifications",
          sound: "default",
          priority: "high",
        },
      },
    });

    console.log(`✅ Sent! Success: ${response.successCount}, Failed: ${response.failureCount}`);

    // ลบ token ที่ใช้ไม่ได้แล้ว
    if (response.failureCount > 0) {
      response.responses.forEach(async (resp, idx) => {
        if (!resp.success) {
          const failedToken = tokens[idx];
          console.log(`   🗑️ Removing invalid token: ${failedToken.substring(0, 20)}...`);

          // ลบจาก Firestore
          const snapshot = await db
            .collection("fcm_tokens")
            .where("token", "==", failedToken)
            .get();
          snapshot.forEach((doc) => doc.ref.delete());
        }
      });
    }
  } catch (err) {
    console.error("❌ FCM send error:", err.message);
  }
});

// ─── HTTP Health Check (Render / UptimeRobot) ────────
const HTTP_PORT = process.env.PORT || 3000;
http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: "running",
    mqtt: client.connected ? "connected" : "disconnected",
    uptime: Math.floor(process.uptime()) + "s",
  }));
}).listen(HTTP_PORT, () => {
  console.log(`🌐 Health check: http://localhost:${HTTP_PORT}`);
});

// ─── Graceful Shutdown ────────────────────────────────
process.on("SIGINT", () => {
  console.log("\n🛑 Shutting down...");
  client.end();
  process.exit(0);
});
