# Helix AdsCodes – MongoDB (xapads_usp)

Node app connected to **MongoDB Atlas**: Project **USP** → Cluster **USP-cluster** → Database **xapads_usp**.

## Setup

1. Copy `.env.example` to `.env` and set `MONGODB_URI` (or use the existing `.env` with your connection string).
2. Install and run:

```bash
npm install
npm start
```

## Platform-based processing

The app exposes **POST /process** to process data from MongoDB based on a `platform` value.

**Payload:**
```json
{ "platform": "ctv" }
```
Valid values: `ctv`, `mobile`, `website`, `all`.

**Logic:**
- **Platform → inventory collections:**  
  `ctv` → `pix_inv_ctv_data` · `mobile` → `pix_inv_mobile_data` · `website` → `pix_inv_web_data` · `all` → all three.
- **From selected `pix_inv_*` collection(s):** extract unique `app_bundle`, `app_name`, `app_ads_txt_url` and include in the output JSON.
- **From `adsdocs`:** fetch `name`, `adlines`, `platform`; include only documents whose `platform` array contains the payload platform (for `all`, documents with any of ctv/mobile/website).

Writes **`output/approved_app_bundles.json`**, **`output/enabled_app_ads_txt_url.json`**, and **`output/adlines_match.csv`** (adlines YES/NO per app). **POST /match-adlines** can regenerate the CSV from the two JSON files only.

**Example:**
```bash
curl -X POST http://localhost:3000/process -H "Content-Type: application/json" -d '{"platform":"ctv"}'
```

## Project layout

- **`src/config/db.js`** – MongoDB connection and helpers (`connect`, `getDb`, `getCollection`, `getClient`, `close`).
- **`src/processor.js`** – Platform-based extraction from `pix_inv_*` and `adsdocs`; writes combined result to a JSON file.
- **`src/index.js`** – Connects to MongoDB and starts HTTP server (POST `/process`).

## Working with collections (tables)

After `await connect()`, use:

```js
const { getCollection } = require('./config/db');
const campaigns = getCollection('campaigns');
const cursor = campaigns.find({});
const docs = await cursor.toArray();
```
