import express, { Request, Response } from "express";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import axios, { AxiosInstance } from "axios";
import { z } from "zod";
import dotenv from "dotenv";
import { fileURLToPath } from "url";
import { dirname, join, extname } from "path";
import { readFileSync, existsSync } from "fs";

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: join(__dirname, "..", ".env") });

const SERVER_API_KEY = process.env.API_KEY ?? "";
const PORT = parseInt(process.env.PORT ?? "3000");
const DEFAULT_API_VERSION = process.env.FB_API_VERSION ?? "v21.0";
const DEFAULT_PAGE_TOKEN = process.env.FB_PAGE_ACCESS_TOKEN ?? "";
const DEFAULT_PAGE_ID = process.env.FB_PAGE_ID ?? "";
const DEFAULT_APP_ID = process.env.FB_APP_ID ?? "";
const DEFAULT_APP_SECRET = process.env.FB_APP_SECRET ?? "";

interface Credentials {
  pageToken: string;
  pageId: string;
  appId: string;
  appSecret: string;
  apiVersion: string;
}

function getCredentials(req: Request): Credentials {
  return {
    pageToken: (req.headers["x-fb-page-access-token"] as string) || DEFAULT_PAGE_TOKEN,
    pageId: (req.headers["x-fb-page-id"] as string) || DEFAULT_PAGE_ID,
    appId: (req.headers["x-fb-app-id"] as string) || DEFAULT_APP_ID,
    appSecret: (req.headers["x-fb-app-secret"] as string) || DEFAULT_APP_SECRET,
    apiVersion: (req.headers["x-fb-api-version"] as string) || DEFAULT_API_VERSION,
  };
}

interface FbPost {
  id: string;
  message?: string;
  story?: string;
  created_time?: string;
  scheduled_publish_time?: number;
  is_published?: boolean;
  full_picture?: string;
  permalink_url?: string;
}

interface FbError {
  message: string;
  type: string;
  code: number;
}

function extractError(err: unknown): string {
  if (axios.isAxiosError(err) && err.response?.data?.error) {
    const e = err.response.data.error as FbError;
    return "Facebook API hiba (" + e.code + "): " + e.message;
  }
  return err instanceof Error ? err.message : String(err);
}

function createClient(creds: Credentials): AxiosInstance {
  return axios.create({
    baseURL: "https://graph.facebook.com/" + creds.apiVersion,
    params: { access_token: creds.pageToken },
    timeout: 30000,
  });
}

function assertCredentials(creds: Credentials): void {
  const missing: string[] = [];
  if (!creds.pageToken) missing.push("FB_PAGE_ACCESS_TOKEN (x-fb-page-access-token header)");
  if (!creds.pageId) missing.push("FB_PAGE_ID (x-fb-page-id header)");
  if (missing.length) throw new Error("Hianyzo hitelesito adatok: " + missing.join(", "));
}

async function uploadPhotoToFacebook(
  apiVersion: string,
  pageId: string,
  pageToken: string,
  imageBuffer: Buffer,
  mimeType: string,
  message: string,
  published: boolean,
  scheduledTs?: number
): Promise<{ id: string; post_id?: string }> {
  const ext = mimeType.split("/")[1] ?? "png";
  const fd = new globalThis.FormData();
  fd.append("source", new Blob([new Uint8Array(imageBuffer)], { type: mimeType }), "photo." + ext);
  fd.append("message", message);
  fd.append("published", String(published));
  fd.append("access_token", pageToken);
  if (!published && scheduledTs !== undefined) {
    fd.append("scheduled_publish_time", String(scheduledTs));
  }
  const url = "https://graph.facebook.com/" + apiVersion + "/" + pageId + "/photos";
  const fetchRes = await fetch(url, { method: "POST", body: fd });
  if (!fetchRes.ok) {
    const errBody = await fetchRes.text();
    throw new Error("Facebook API hiba (" + fetchRes.status + "): " + errBody);
  }
  return fetchRes.json() as Promise<{ id: string; post_id?: string }>;
}

function createMcpServer(creds: Credentials): McpServer {
  const server = new McpServer({ name: "meta-marketing-agent", version: "2.3.0" });

  server.tool("list_posts", "Visszaadja az oldal legutobb bejegyzeseit.", {
    limit: z.number().int().min(1).max(100).optional().default(10),
    include_scheduled: z.boolean().optional().default(false),
  }, async ({ limit, include_scheduled }) => {
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      const fields = "id,message,story,created_time,is_published,scheduled_publish_time,full_picture,permalink_url";
      const requests = [
        client.get("/" + creds.pageId + "/posts", { params: { fields, limit } }),
        ...(include_scheduled ? [client.get("/" + creds.pageId + "/scheduled_posts", { params: { fields, limit } })] : []),
      ];
      const results = await Promise.all(requests);
      const published: FbPost[] = results[0].data.data ?? [];
      const scheduled: FbPost[] = include_scheduled && results[1] ? results[1].data.data ?? [] : [];
      const fmt = (p: FbPost, type: string) => ({
        id: p.id, type,
        message: p.message ?? p.story ?? "(nincs szoveg)",
        created_time: p.created_time ?? null,
        scheduled_publish_time: p.scheduled_publish_time ? new Date(p.scheduled_publish_time * 1000).toISOString() : null,
        is_published: p.is_published ?? true,
        full_picture: p.full_picture ?? null,
        permalink_url: p.permalink_url ?? null,
      });
      const all = [...published.map(p => fmt(p, "kozzetett")), ...scheduled.map(p => fmt(p, "utemezett"))];
      return { content: [{ type: "text", text: JSON.stringify({ total: all.length, posts: all }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("get_post", "Egy adott Facebook bejegyzes reszleteit adja vissza.", {
    post_id: z.string().min(1),
  }, async ({ post_id }) => {
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      const fields = "id,message,story,created_time,is_published,scheduled_publish_time,full_picture,permalink_url,likes.summary(true),comments.summary(true),shares";
      const { data } = await client.get("/" + post_id, { params: { fields } });
      return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("create_post", "Uj bejegyzest tesz koze az oldalon kepel vagy anelkul, vagy utemezi. A kep megadhato base64 kodolt stringkent (image_base64), nyilvanos URL-kent (image_url), vagy link-kent.", {
    message: z.string().min(1).max(63206),
    image_base64: z.string().optional(),
    image_mime_type: z.enum(["image/png", "image/jpeg", "image/gif", "image/webp"]).optional().default("image/png"),
    image_url: z.string().url().optional(),
    image_path: z.string().optional(),
    link: z.string().url().optional(),
    published: z.boolean().optional().default(true),
    scheduled_publish_time: z.string().optional(),
    privacy: z.enum(["EVERYONE", "FRIENDS", "ONLY_ME"]).optional().default("EVERYONE"),
  }, async ({ message, image_base64, image_mime_type, image_url, image_path, link, published, scheduled_publish_time, privacy }) => {
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      let scheduledTs: number | undefined;
      if (!published && scheduled_publish_time) {
        scheduledTs = Math.floor(new Date(scheduled_publish_time).getTime() / 1000);
        if (isNaN(scheduledTs)) throw new Error("Ervenytelen scheduled_publish_time formatum.");
      }

      if (image_base64) {
        const buffer = Buffer.from(image_base64, "base64");
        const mimeType = image_mime_type ?? "image/png";
        const resData = await uploadPhotoToFacebook(
          creds.apiVersion, creds.pageId, creds.pageToken,
          buffer, mimeType, message, published, scheduledTs
        );
        return { content: [{ type: "text", text: JSON.stringify({ success: true, action: published ? "Kozzetve (base64 kep)" : "Utemezve (base64 kep)", photo_id: resData.id, post_id: resData.post_id ?? null }, null, 2) }] };
      }

      if (image_path) {
        if (!existsSync(image_path)) throw new Error("A fajl nem talalhato: " + image_path);
        const fileBuffer = readFileSync(image_path);
        const ext = extname(image_path).toLowerCase().replace(".", "") || "jpeg";
        const mimeType = ext === "png" ? "image/png" : ext === "gif" ? "image/gif" : "image/jpeg";
        const resData = await uploadPhotoToFacebook(
          creds.apiVersion, creds.pageId, creds.pageToken,
          fileBuffer, mimeType, message, published, scheduledTs
        );
        return { content: [{ type: "text", text: JSON.stringify({ success: true, action: published ? "Kozzetve (lokalis kep)" : "Utemezve (lokalis kep)", photo_id: resData.id, post_id: resData.post_id ?? null }, null, 2) }] };
      }

      if (image_url) {
        const photoParams: Record<string, unknown> = { message, url: image_url, published };
        if (scheduledTs !== undefined) photoParams.scheduled_publish_time = scheduledTs;
        const { data } = await client.post<{ id: string; post_id?: string }>("/" + creds.pageId + "/photos", photoParams);
        return { content: [{ type: "text", text: JSON.stringify({ success: true, action: published ? "Kozzetve (URL-kep)" : "Utemezve (URL-kep)", photo_id: data.id, post_id: data.post_id ?? null }, null, 2) }] };
      }

      const params: Record<string, unknown> = { message, published, privacy: JSON.stringify({ value: privacy ?? "EVERYONE" }) };
      if (link) params.link = link;
      if (scheduledTs !== undefined) params.scheduled_publish_time = scheduledTs;
      const { data } = await client.post<{ id: string }>("/" + creds.pageId + "/feed", params);
      return { content: [{ type: "text", text: JSON.stringify({ success: true, action: published ? "Kozzetve" : "Utemezve", post_id: data.id }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("update_post", "Meglevo bejegyzes szoveget modositja.", {
    post_id: z.string().min(1),
    message: z.string().min(1).max(63206),
  }, async ({ post_id, message }) => {
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      const { data } = await client.post<{ success: boolean }>("/" + post_id, { message });
      return { content: [{ type: "text", text: JSON.stringify({ success: data.success, post_id }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("delete_post", "Torol egy bejegyzest az oldalrol. A muvelet visszavonhatatlan.", {
    post_id: z.string().min(1),
  }, async ({ post_id }) => {
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      const { data } = await client.delete<{ success: boolean }>("/" + post_id);
      return { content: [{ type: "text", text: JSON.stringify({ success: data.success, deleted_post_id: post_id }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("publish_scheduled_post", "Egy korabban utemezett bejegyzest azonnal kozzétesz.", {
    post_id: z.string().min(1),
  }, async ({ post_id }) => {
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      const { data } = await client.post<{ success: boolean }>("/" + post_id, { is_published: true });
      return { content: [{ type: "text", text: JSON.stringify({ success: data.success, published_post_id: post_id }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("get_page_info", "Visszaadja az oldal alapadatait.", {}, async () => {
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      const fields = "id,name,category,fan_count,followers_count,about,website,phone,email,link";
      const { data } = await client.get("/" + creds.pageId, { params: { fields } });
      return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("check_token", "Ellenorzi a Page Access Token ervenyet.", {}, async () => {
    if (!creds.appId || !creds.appSecret)
      return { content: [{ type: "text", text: "FB_APP_ID es FB_APP_SECRET szukseges." }], isError: true };
    const client = createClient(creds);
    try {
      const appToken = creds.appId + "|" + creds.appSecret;
      const { data } = await client.get("/debug_token", { params: { input_token: creds.pageToken, access_token: appToken } });
      return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("get_post_insights", "Visszaadja egy bejegyzes statisztikait.", {
    post_id: z.string().min(1),
  }, async ({ post_id }) => {
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      const metrics = ["post_impressions","post_impressions_unique","post_engaged_users","post_clicks","post_reactions_by_type_total"].join(",");
      const [insightsRes, postRes] = await Promise.all([
        client.get("/" + post_id + "/insights", { params: { metric: metrics } }),
        client.get("/" + post_id, { params: { fields: "reactions.summary(true),likes.summary(true),comments.summary(true),shares,message,created_time" } }),
      ]);
      const insightsMap: Record<string, unknown> = {};
      for (const item of insightsRes.data.data ?? []) insightsMap[item.name] = item.values?.[0]?.value ?? item.values;
      return { content: [{ type: "text", text: JSON.stringify({
        post_id, message: postRes.data.message ?? null, created_time: postRes.data.created_time ?? null,
        reactions_total: postRes.data.reactions?.summary?.total_count ?? 0,
        likes_total: postRes.data.likes?.summary?.total_count ?? 0,
        comments_total: postRes.data.comments?.summary?.total_count ?? 0,
        shares_total: postRes.data.shares?.count ?? 0,
        insights: insightsMap,
      }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("get_page_insights", "Visszaadja az oldal osszesitett statisztikait.", {
    period: z.enum(["day","week","days_28","month","lifetime"]).optional().default("week"),
    since: z.string().optional(),
    until: z.string().optional(),
  }, async ({ period, since, until }) => {
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      const metrics = ["page_impressions","page_impressions_unique","page_engaged_users","page_post_engagements","page_fan_count_delta","page_views_total"].join(",");
      const params: Record<string, unknown> = { metric: metrics, period };
      if (since) params.since = Math.floor(new Date(since).getTime() / 1000);
      if (until) params.until = Math.floor(new Date(until).getTime() / 1000);
      const [insightsRes, pageRes] = await Promise.all([
        client.get("/" + creds.pageId + "/insights", { params }),
        client.get("/" + creds.pageId, { params: { fields: "fan_count,followers_count,name" } }),
      ]);
      const insightsMap: Record<string, unknown> = {};
      for (const item of insightsRes.data.data ?? []) insightsMap[item.name] = item.values ?? item.value;
      return { content: [{ type: "text", text: JSON.stringify({
        page_name: pageRes.data.name, page_id: creds.pageId,
        fan_count: pageRes.data.fan_count ?? 0, followers_count: pageRes.data.followers_count ?? 0,
        period, insights: insightsMap,
      }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  return server;
}

const app = express();
app.use(express.json({ limit: "20mb" }));

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", [
    "Content-Type","Accept","Mcp-Session-Id","MCP-Protocol-Version",
    "x-api-key","x-fb-page-access-token","x-fb-page-id","x-fb-app-id","x-fb-app-secret","x-fb-api-version",
    "x-message","x-published","x-scheduled-time",
  ].join(", "));
  if (req.method === "OPTIONS") { res.sendStatus(204); return; }
  next();
});

function requireApiKey(req: Request, res: Response, next: () => void): void {
  if (!SERVER_API_KEY) { next(); return; }
  const key = req.headers["x-api-key"] as string;
  if (key !== SERVER_API_KEY) { res.status(401).json({ error: "Ervenytelen API kulcs." }); return; }
  next();
}

app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "meta-marketing-agent", version: "2.3.0" });
});

// REST endpoint: POST /upload-image
// Accept raw binary image in body, credentials in headers
// curl -X POST .../upload-image -H "Content-Type: image/png" -H "x-message: TEXT" -H "x-fb-page-id: ID" -H "x-fb-page-access-token: TOKEN" --data-binary @image.png
app.post("/upload-image", requireApiKey, express.raw({ type: ["image/*", "application/octet-stream"], limit: "10mb" }), async (req: Request, res: Response) => {
  const creds = getCredentials(req);
  try {
    assertCredentials(creds);
    const imageBuffer = req.body as Buffer;
    if (!imageBuffer || imageBuffer.length === 0) {
      res.status(400).json({ success: false, error: "Nincs kepfajl a request body-ban. Kuldd a kepet raw binary-kent (Content-Type: image/png stb.)" });
      return;
    }
    const mimeType = ((req.headers["content-type"] as string) ?? "image/jpeg").split(";")[0].trim();
    const message = (req.headers["x-message"] as string) ?? "";
    const published = (req.headers["x-published"] as string) !== "false";
    let scheduledTs: number | undefined;
    const scheduledTime = req.headers["x-scheduled-time"] as string;
    if (!published && scheduledTime) {
      scheduledTs = Math.floor(new Date(scheduledTime).getTime() / 1000);
      if (isNaN(scheduledTs)) {
        res.status(400).json({ success: false, error: "Ervenytelen x-scheduled-time formatum." });
        return;
      }
    }
    const result = await uploadPhotoToFacebook(
      creds.apiVersion, creds.pageId, creds.pageToken,
      imageBuffer, mimeType, message, published, scheduledTs
    );
    res.json({ success: true, photo_id: result.id, post_id: result.post_id ?? null });
  } catch (err) {
    res.status(500).json({ success: false, error: extractError(err) });
  }
});

app.post("/mcp", requireApiKey, async (req: Request, res: Response) => {
  const creds = getCredentials(req);
  const server = createMcpServer(creds);
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  res.on("close", () => { transport.close(); server.close(); });
  try {
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    if (!res.headersSent) res.status(500).json({ error: "Belso szerverhiba", details: String(err) });
  }
});

app.get("/mcp", requireApiKey, async (_req: Request, res: Response) => {
  res.status(405).json({ error: "A szerver stateless modban fut, GET nem tamogatott." });
});

app.delete("/mcp", requireApiKey, async (_req: Request, res: Response) => {
  res.status(405).json({ error: "Session kezeles nem tamogatott." });
});

if (!process.env.VERCEL) {
  app.listen(PORT, "0.0.0.0", () => {
    console.log("Meta Marketing Agent fut: http://0.0.0.0:" + PORT + "/mcp");
    console.log("API key: " + (SERVER_API_KEY ? "BE" : "KI"));
  });
}

export default app;
