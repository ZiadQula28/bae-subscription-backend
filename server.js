// =============================================================================
//  BAE Payments+ — Subscription Engine
//  Stack: Node.js · Express · Firebase Admin SDK · node-cron
// =============================================================================

import express from "express";
import cors    from "cors";
import cron    from "node-cron";
import admin   from "firebase-admin";

// ─────────────────────────────────────────────────────────────────────────────
//  FIREBASE INITIALISATION
// ─────────────────────────────────────────────────────────────────────────────

const serviceAccount = {
  type: "service_account",
  project_id: "paymentplus",
  private_key_id: "1abe5311dade39397706c3e7a0d2cd445728bedd",
  private_key: "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDO9yViOADD2tHp\nM3M+utY7Y/S6Wv9F+BmCnjLtvY/o3naRcn3WeuJOKh2dx1bo0H1mSAVGXOmAryTs\nwTceXbrQeWTa+im4nhyunng6D3UjswIEZ3EHQpYIgUX9i58b8f6KAcDtqCF1ogmN\nheatySNcFgAaaH9/jsz7moFuBFmGzBcKZOSh/WMCL+DOYA5ycDSmzHF6tPdYOchj\nHktrH3v0Ac8ku7GSCw+P/BeIkkQ5pqgVjwCJ1BKvE2XIZY22A17D5ycZKGBXYNmH\norHktQzaY/8XmxplQqg0o4jfkDcfjeAPik7oRqQfifEo7k7QLZ62hZ98WJ64Scys\n7c0u37InAgMBAAECggEATNfslGwwtIdxesF0n6zlUAXL3slckJEOf7XXOPgyD4dR\nxK0a1eCoV9dBScyykxyeArTo6Hgk5nehIRqYKXkpnLAF51wM05Q5GAiUoRIOLyQO\nsq3phIWPiTbeUsZdXBXcqUb/Z/4N9kwkedxmis3siAtPHH8CH81YhG6Ycc3Bo89G\nJnVeXDzKcw+y9Ifm/F7BVXEBsgjsUcg/+f+SLqIHHqNSkhnjX3Jvbmy+7oev2Bdt\n7AsXn6lpjXR+rHLISDFtLSgumLg/ioOOZYPw80jUVFxtuJxCByN6Xz19p4yfopjh\nV91jsk0gnJPSyMupvp3qM1k8gTgk0RuJ8Se4STGhcQKBgQDqNadGUR9o0VcCoc93\n4DehbAU95+5xTxT4jq4tL1B2Hj8bfkTqS8kUie2/yQ7VhVHZa8slulv+TR8oNc3N\nyOJxjgGeVbssmwJg7bI9+jpyvzJthItTpfDW53oVU15epsrt1DYD1qEVDp9mtuBC\nJHipJgT5wFECGyDztbWTmpqcqQKBgQDiOJj/b8EXPXBYOOLSrvy/PwD9fGxLXWtI\nLxbI9hX6GPDoDKLYOEG4BNK/8+D/4bxXXTH/1dge6k0+hSR844FYSLQvK5KRn/5i\n9aJSPKB1oQT1xes7NQ0kxopj3xLkS27VezwZqsGWPooy8u28Ga7paQjptuOJM1lm\nZgmieKzKTwKBgQCU2gy6EAzaXVHjhluJYKN6oGCke6d7tDsrzeK9LPkCWS7almHO\nVuQxfYfxACPBaL4gZPXzwIF/GhzXPXrTcv0YFpzIuMwBCXN1jxxdb49+Ji4ylK7I\nNwreSre4nge4OGF4zgqWXe56p5R/f3MAFpXpSxSWdrgFkqBaw/3nFbd2IQKBgBU6\nhnECBbaul0pQHEj1DtbLjMY81NTFsXmWoaa+IRXVHVZJSUbNEK26FiTZN/YLMudn\n4XdGk/9isVj6mY/2sX6dY/tBNYHg57hVoKPi+JPRRmvh5M8rjU40K2xv1JAD3CTA\nacVm1r/IoqNhVJngNx/EMYftF7EaVntbe+/5Lw3VAoGBAI+Se7Xt29hjfG97i+3d\nygfrg2rqECMj1gAU536+JVRhmpyZsvukHwTrcqEZTjSxg50rEY9od3TRPsJX+5Oe\naJ7c2ezss0CwpT2B/pQ0+4p61PM03fqwBjKDmqkPqycK6zCQrCSKlINaRXdEctAT\noPqPi9FARkgDcHCjHQKV0PEg\n-----END PRIVATE KEY-----\n",
  client_email: "firebase-adminsdk-fbsvc@paymentplus.iam.gserviceaccount.com",
  client_id: "114694120884075429530",
  auth_uri: "https://accounts.google.com/o/oauth2/auth",
  token_uri: "https://oauth2.googleapis.com/token",
  auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs",
  client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40paymentplus.iam.gserviceaccount.com",
  universe_domain: "googleapis.com",
};

admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

// ─────────────────────────────────────────────────────────────────────────────
//  BAE CREDENTIALS & ENDPOINTS
// ─────────────────────────────────────────────────────────────────────────────

const BAE_CONFIG = {
  dev: {
    captureUrl: "https://merchant-order-token.baelab.net/v1/payments/capture-context",
    processUrl: "https://api.apps-console.bankaletihad.com/BAF3E974-52AA-7598-FF04-56945EF93500/045FCC75-62A0-EE53-FF87-4FD683745500/services/businessMarketplace/pay/hostedCheckout",
    mitUrl:     "https://merchant-order-token.baelab.net/v1/payments/PLACEHOLDER_MIT_ENDPOINT",
    authKey:    "MDAxMTUwOTkyOilFVj02UU1GX2RDVmdUYW4yUEd+NnBYaCNzRUtrbg==",
    companyId:  "A4B4A51F-0E6A-41BE-A8FB-5FCCA54C2F58",
  },
  prod: {
    captureUrl: "https://merchant-order-token.bankaletihad.com/v1/payments/app2/capture-context",
    processUrl: "https://api.apps-console.bankaletihad.com/BAF3E974-52AA-7598-FF04-56945EF93500/045FCC75-62A0-EE53-FF87-4FD683745500/services/businessMarketplace/pay/hostedCheckout",
    mitUrl:     "https://merchant-order-token.bankaletihad.com/v1/payments/PLACEHOLDER_MIT_ENDPOINT",
    authKey:    "MDAxNjA4Njg3Ol8hI19MdjQqUnp1OUw1YzZoOVRFMnllfWNdNEtCMg==",
    companyId:  "6361F8DC-BCAE-4D4A-B903-7B8121A47922",
  },
};

const HEADERS = (authKey) => ({
  "Content-Type":  "application/json",
  "Accept":        "application/json",
  "Authorization": authKey,
  "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
});

// ─────────────────────────────────────────────────────────────────────────────
//  HELPERS
// ─────────────────────────────────────────────────────────────────────────────

const todayString      = ()         => new Date().toISOString().split("T")[0];
const futureDateString = (days = 1) => {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return d.toISOString().split("T")[0];
};

// ─────────────────────────────────────────────────────────────────────────────
//  EXPRESS APP
// ─────────────────────────────────────────────────────────────────────────────

const app = express();
app.use(cors());
app.use(express.json());

// ─────────────────────────────────────────────────────────────────────────────
//  ROUTE 1: Capture Context Token
//  POST /api/capture-context
// ─────────────────────────────────────────────────────────────────────────────

app.post("/api/capture-context", async (req, res) => {
  const { totalAmount, origin, environment = "dev" } = req.body;
  console.log(`\n📥 [capture-context] env=${environment} amount=${totalAmount} origin=${origin}`);

  const cfg = BAE_CONFIG[environment] ?? BAE_CONFIG.dev;

  try {
    const baeRes = await fetch(cfg.captureUrl, {
      method:  "POST",
      headers: HEADERS(cfg.authKey),
      body: JSON.stringify({
        targetOrigins: [origin, "https://ziadqula28.github.io"],
        totalAmount,
        currency: "JOD",
      }),
    });

    const data = await baeRes.text();
    console.log(`✅ [capture-context] BAE responded ${baeRes.status}`);
    return res.status(baeRes.status).send(data);
  } catch (err) {
    console.error(`❌ [capture-context] Error:`, err.message);
    return res.status(500).json({ error: err.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
//  ROUTE 2: Process Final Payment (standard card / 3DS path)
//  POST /api/process-payment
//  Body: { transientToken, environment, isSubscription, totalAmount }
//
//  The BAE /hostedCheckout response contains the customerTokenId (TMS token)
//  which is the reusable token for future MIT charges on standard cards.
// ─────────────────────────────────────────────────────────────────────────────

app.post("/api/process-payment", async (req, res) => {
  const { transientToken, environment = "dev", isSubscription = false, totalAmount = "1.00" } = req.body;
  console.log(`\n📥 [process-payment] env=${environment} isSubscription=${isSubscription} amount=${totalAmount}`);

  const cfg = BAE_CONFIG[environment] ?? BAE_CONFIG.dev;

  try {
    const baeRes = await fetch(cfg.processUrl, {
      method:  "POST",
      headers: HEADERS(cfg.authKey),
      body: JSON.stringify({ token: transientToken, companyId: cfg.companyId }),
    });

    const rawData = await baeRes.text();
    console.log(`✅ [process-payment] BAE responded ${baeRes.status}`);
    console.log(`📦 [process-payment] BAE raw response: ${rawData}`);

    // ── Save to Firestore if this is a subscription ───────────────────────────
    if (isSubscription && baeRes.ok) {
      try {
        const parsed = JSON.parse(rawData);
        console.log(`🔍 [process-payment] BAE response keys: ${Object.keys(parsed).join(", ")}`);

        // TMS token from BAE hostedCheckout (standard card path)
        const customerTokenId = parsed.customerTokenId
          || parsed.paymentToken
          || parsed.token
          || null;

        if (customerTokenId) {
          const subId = "sub_" + Math.random().toString(36).substr(2, 9);
          await db.collection("subscriptions").doc(subId).set({
            userId:           subId,
            customerTokenId,                   // reusable TMS token for MIT
            tokenType:        "TMS",
            planAmount:       totalAmount,
            currency:         "JOD",
            status:           "active",
            environment,
            nextBillingDate:  futureDateString(1), // ← change to 30 for production
            createdAt:        admin.firestore.FieldValue.serverTimestamp(),
            updatedAt:        admin.firestore.FieldValue.serverTimestamp(),
          });
          console.log(`💾 [process-payment] Subscription saved: ${subId} | TMS token: ${customerTokenId}`);
        } else {
          console.warn(`⚠️ [process-payment] No customerTokenId in BAE response. Keys: ${Object.keys(parsed).join(", ")}`);
        }
      } catch (e) {
        console.error(`❌ [process-payment] Firestore save error:`, e.message);
      }
    }

    return res.status(baeRes.status).send(rawData);
  } catch (err) {
    console.error(`❌ [process-payment] Error:`, err.message);
    return res.status(500).json({ error: err.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
//  ROUTE 3: Save Wallet Subscription (Google Pay / Apple Pay path)
//  POST /api/save-subscription
//  Body: { completeResultJwt, totalAmount, environment }
//
//  IMPORTANT ARCHITECTURE NOTE:
//  Wallet payments (Google Pay / Apple Pay) do NOT return a reusable TMS token
//  inside the up.complete() JWT. Instead they return a transaction receipt with:
//    - id:                                    the CyberSource transaction ID
//    - details.processorInformation.networkTransactionId: the network MIT reference
//
//  For CyberSource MIT, the networkTransactionId from the original CIT is the
//  reference used in subsequent charges — not a TMS customerTokenId.
//  ⚠️ Confirm with BAE what the exact MIT payload looks like for wallet tokens.
// ─────────────────────────────────────────────────────────────────────────────

app.post("/api/save-subscription", async (req, res) => {
  const { completeResultJwt, totalAmount = "1.00", environment = "prod" } = req.body;
  console.log(`\n📥 [save-subscription] env=${environment} amount=${totalAmount}`);

  try {
    const payloadB64 = completeResultJwt.split(".")[1];
    const decoded    = JSON.parse(Buffer.from(payloadB64, "base64").toString("utf8"));

    console.log(`🔍 [save-subscription] Decoded JWT keys: ${Object.keys(decoded).join(", ")}`);

    // ── Extract available identifiers from the wallet authorization receipt ───
    const transactionId      = decoded.id || null;
    const networkTxnId       = decoded.details?.processorInformation?.networkTransactionId || null;
    const reconciliationId   = decoded.reconciliationId || null;
    const approvalCode       = decoded.details?.processorInformation?.approvalCode || null;
    const cardType           = decoded.details?.paymentInformation?.card?.type || null;

    console.log(`🔍 [save-subscription] transactionId=${transactionId} networkTxnId=${networkTxnId}`);

    if (!transactionId && !networkTxnId) {
      console.warn(`⚠️ [save-subscription] No usable token identifiers found.`);
      return res.status(400).json({ error: "No usable token found in wallet JWT." });
    }

    // ── Save to Firestore with all available references ──────────────────────
    const subId = "sub_" + Math.random().toString(36).substr(2, 9);
    await db.collection("subscriptions").doc(subId).set({
      userId:            subId,
      // For wallet payments: use networkTransactionId as the MIT reference
      // (confirm exact MIT payload format with BAE before going live)
      customerTokenId:   networkTxnId || transactionId,
      tokenType:         "NETWORK_TXN_ID",   // distinguishes from TMS token
      transactionId,
      networkTxnId,
      reconciliationId,
      approvalCode,
      cardType,
      planAmount:        totalAmount,
      currency:          "JOD",
      status:            "active",
      environment,
      nextBillingDate:   futureDateString(1), // ← change to 30 for production
      createdAt:         admin.firestore.FieldValue.serverTimestamp(),
      updatedAt:         admin.firestore.FieldValue.serverTimestamp(),
    });

    console.log(`💾 [save-subscription] Wallet subscription saved: ${subId} | networkTxnId: ${networkTxnId}`);
    return res.status(201).json({ message: "Subscription saved.", subscriptionId: subId });

  } catch (err) {
    console.error(`❌ [save-subscription] Error:`, err.message);
    return res.status(500).json({ error: err.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
//  ROUTE 4: Cancel Subscription
//  POST /api/cancel-subscription
// ─────────────────────────────────────────────────────────────────────────────

app.post("/api/cancel-subscription", async (req, res) => {
  const { subscriptionId } = req.body;
  console.log(`\n📥 [cancel-subscription] id=${subscriptionId}`);
  if (!subscriptionId) return res.status(400).json({ error: "Missing subscriptionId." });

  try {
    await db.collection("subscriptions").doc(subscriptionId).update({
      status:    "cancelled",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    console.log(`🚫 [cancel-subscription] Cancelled: ${subscriptionId}`);
    return res.status(200).json({ message: "Subscription cancelled." });
  } catch (err) {
    console.error(`❌ [cancel-subscription] Error:`, err.message);
    return res.status(500).json({ error: "Failed to cancel." });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
//  ROUTE 5: Manual billing trigger (for testing)
//  POST /api/run-billing-cycle
// ─────────────────────────────────────────────────────────────────────────────

app.post("/api/run-billing-cycle", async (req, res) => {
  console.log(`\n🔧 [run-billing-cycle] Manual trigger`);
  const results = await runDailyBillingCycle();
  return res.status(200).json({ message: "Billing cycle complete.", results });
});

// =============================================================================
//  MIT SCHEDULER — Daily Subscription Billing
//  Runs every day at 00:00 Asia/Amman
// =============================================================================

async function runDailyBillingCycle() {
  const today   = todayString();
  const results = { charged: 0, failed: 0 };

  console.log(`\n⏰ [${new Date().toISOString()}] Billing cycle started — due date: ${today}`);

  try {
    const snapshot = await db
      .collection("subscriptions")
      .where("status",          "==", "active")
      .where("nextBillingDate", "<=", today)
      .get();

    if (snapshot.empty) {
      console.log("✅ No subscriptions due today.");
      return results;
    }

    console.log(`📋 ${snapshot.size} subscription(s) to process.`);

    for (const doc of snapshot.docs) {
      const sub   = doc.data();
      const subId = doc.id;
      const env   = sub.environment ?? "prod";
      const cfg   = BAE_CONFIG[env];

      console.log(`\n💸 Charging ${subId} | tokenType=${sub.tokenType} | amount=${sub.planAmount} JOD`);

      try {
        const chargeRes = await fetch(cfg.mitUrl, {
          method:  "POST",
          headers: HEADERS(cfg.authKey),
          body: JSON.stringify({
            customerTokenId:              sub.customerTokenId,
            amount:                       sub.planAmount,
            currency:                     "JOD",
            initiatorType:                "MERCHANT",
            merchantInitiatedTransaction: true,
            companyId:                    cfg.companyId,
          }),
        });

        const chargeBody = await chargeRes.text();

        if (chargeRes.ok) {
          const nextBillingDate = futureDateString(1); // ← change to 30 for production
          await db.collection("subscriptions").doc(subId).update({
            nextBillingDate,
            status:    "active",
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
          console.log(`  ✅ Charged. Next billing: ${nextBillingDate}`);
          results.charged++;
        } else {
          await db.collection("subscriptions").doc(subId).update({
            status:          "failed",
            lastFailureCode: String(chargeRes.status),
            lastFailureData: chargeBody,
            updatedAt:       admin.firestore.FieldValue.serverTimestamp(),
          });
          console.error(`  ❌ Charge failed HTTP ${chargeRes.status}: ${chargeBody}`);
          results.failed++;
        }
      } catch (chargeErr) {
        await db.collection("subscriptions").doc(subId).update({
          status:          "failed",
          lastFailureCode: "NETWORK_ERROR",
          lastFailureData: chargeErr.message,
          updatedAt:       admin.firestore.FieldValue.serverTimestamp(),
        });
        console.error(`  ❌ Network error for ${subId}:`, chargeErr.message);
        results.failed++;
      }
    }
  } catch (queryErr) {
    console.error("Fatal billing cycle error:", queryErr);
  }

  console.log(`\n📊 Done — Charged: ${results.charged} | Failed: ${results.failed}`);
  return results;
}

cron.schedule("0 0 * * *", () => {
  runDailyBillingCycle();
}, { timezone: "Asia/Amman" });

console.log("🕛 Daily billing cron registered (00:00 Asia/Amman).");

// ─────────────────────────────────────────────────────────────────────────────
//  START SERVER
// ─────────────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n🚀 BAE Subscription Engine running on http://localhost:${PORT}\n`);
});
