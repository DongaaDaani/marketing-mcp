import express, { Request, Response } from "express";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { randomUUID } from "crypto";
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

// ---------------------------------------------------------------------------
// SZERVEROLDALI OLDAL-NYILVANTARTAS
//
// Minden Facebook oldal hitelesito adata a szerver kornyezeti valtozoiban van,
// nem a kliens headereiben. Az oldalt az URL ?p= parametere valasztja ki:
//   /mcp?p=hu  ->  FB_PAGE_TOKEN_HU + FB_PAGE_ID_HU
//   /mcp?p=de  ->  FB_PAGE_TOKEN_DE + FB_PAGE_ID_DE
//
// Igy a plugin .mcp.json fajlba nem kell titkot tenni, es a mukodes nem fugg
// attol, hogy a kliens tovabbitja-e a custom headereket.
// ---------------------------------------------------------------------------
interface PageCreds { token: string; id: string; name?: string; }

// A nyilvantartas DINAMIKUS: a kornyezeti valtozokbol epul fel, nem kodbol.
// Uj oldal hozzaadasa = 2 (opcionalisan 3) env valtozo, KODMODOSITAS NELKUL:
//   FB_PAGE_TOKEN_<KULCS>   (kotelezo)  pl. FB_PAGE_TOKEN_AIP_CZ
//   FB_PAGE_ID_<KULCS>      (kotelezo)  pl. FB_PAGE_ID_AIP_CZ
//   FB_PAGE_NAME_<KULCS>    (opcionalis) pl. "All In Packaging Cesko"
// A tool 'page' parametere a kulcs kisbetus formaja: aip_cz
type TenantPages = Record<string, PageCreds>;

// Ha a keres nem ad meg ceget (?t=), ez a ceg lesz hasznalva.
// EZ BIZTOSITJA A VISSZAFELE KOMPATIBILITAST: a mar kiadott pluginok URL-je
// nem tartalmaz ?t= parametert, azok tovabbra is ehhez a ceghez tartoznak.
const DEFAULT_TENANT = (process.env.DEFAULT_TENANT ?? "nordtek").toLowerCase();

// TENANTS[ceg][oldalkulcs] = { token, id, name }
const TENANTS: Record<string, TenantPages> = {};

function tenantBucket(t: string): TenantPages {
  if (!TENANTS[t]) TENANTS[t] = {};
  return TENANTS[t];
}

for (const envKey of Object.keys(process.env)) {
  // (A) CEGES forma — ket alulvonas valasztja el a ceget es az oldalkulcsot:
  //       FB_PAGE_TOKEN__<CEG>__<OLDALKULCS>
  //       FB_PAGE_ID__<CEG>__<OLDALKULCS>
  //       FB_PAGE_NAME__<CEG>__<OLDALKULCS>   (opcionalis)
  //     A <CEG> csak betu/szam lehet (alulvonas nem), igy az elvalasztas egyertelmu.
  //     Pl.: FB_PAGE_TOKEN__ACME__SHOP_DE  ->  ceg "acme", oldal "shop_de"
  const mt = /^FB_PAGE_TOKEN__([A-Z0-9]+)__([A-Z0-9_]+)$/.exec(envKey);
  if (mt) {
    const T = mt[1];
    const K = mt[2];
    const token = process.env[envKey] ?? "";
    const id = process.env[`FB_PAGE_ID__${T}__${K}`] ?? "";
    const name = process.env[`FB_PAGE_NAME__${T}__${K}`];
    if (token && id) tenantBucket(T.toLowerCase())[K.toLowerCase()] = { token, id, name };
    continue; // FONTOS: ne essen at a regi mintara
  }

  // (B) REGI forma — VALTOZATLAN, a DEFAULT_TENANT ceghez kerul:
  //       FB_PAGE_TOKEN_<OLDALKULCS>
  const m = /^FB_PAGE_TOKEN_([A-Z0-9_]+)$/.exec(envKey);
  if (!m) continue;
  const up = m[1];
  const token = process.env[envKey] ?? "";
  const id = process.env[`FB_PAGE_ID_${up}`] ?? "";
  const name = process.env[`FB_PAGE_NAME_${up}`];
  if (token && id) tenantBucket(DEFAULT_TENANT)[up.toLowerCase()] = { token, id, name };
}

const TENANT_KEYS = Object.keys(TENANTS).sort();

/** Feloldja, melyik ceg keresese ez. null = ismeretlen ceg-azonosito. */
function resolveTenant(req: Request): { key: string; pages: TenantPages } | null {
  const raw = req.query?.t;
  const val = Array.isArray(raw) ? raw[0] : raw;
  const key = (typeof val === "string" && val.trim() ? val.trim() : DEFAULT_TENANT).toLowerCase();
  const pages = TENANTS[key];
  if (!pages) return null;
  return { key, pages };
}

function resolvePage(req: Request): PageCreds | null {
  const t = resolveTenant(req);
  if (!t) return null;
  const raw = req.query?.p;
  const key = (Array.isArray(raw) ? raw[0] : raw);
  if (typeof key !== "string") return null;
  return t.pages[key.toLowerCase()] ?? null;
}
const DEFAULT_APP_ID = process.env.FB_APP_ID ?? "";
const DEFAULT_APP_SECRET = process.env.FB_APP_SECRET ?? "";

// DO Spaces (S3-compatible CDN storage)
const SPACES_KEY    = process.env.DO_SPACES_KEY ?? "";
const SPACES_SECRET = process.env.DO_SPACES_SECRET ?? "";
const SPACES_BUCKET = process.env.DO_SPACES_BUCKET ?? "allinhoreca-media";
const SPACES_REGION = process.env.DO_SPACES_REGION ?? "ams3";
const SPACES_ENDPOINT = `https://${SPACES_REGION}.digitaloceanspaces.com`;
const SPACES_CDN    = process.env.DO_SPACES_CDN ?? `https://${SPACES_BUCKET}.${SPACES_REGION}.cdn.digitaloceanspaces.com`;

let s3Client: S3Client | null = null;
if (SPACES_KEY && SPACES_SECRET) {
  s3Client = new S3Client({
    endpoint: SPACES_ENDPOINT,
    region: SPACES_REGION,
    credentials: { accessKeyId: SPACES_KEY, secretAccessKey: SPACES_SECRET },
    forcePathStyle: false,
  });
}

async function uploadToSpaces(buffer: Buffer, mimeType: string): Promise<string> {
  if (!s3Client) throw new Error("DO Spaces nincs konfigurálva. Állítsd be: DO_SPACES_KEY, DO_SPACES_SECRET, DO_SPACES_BUCKET, DO_SPACES_REGION");
  const ext = mimeType.split("/")[1]?.replace("jpeg", "jpg") ?? "jpg";
  const key = `uploads/${randomUUID()}.${ext}`;
  await s3Client.send(new PutObjectCommand({
    Bucket: SPACES_BUCKET,
    Key: key,
    Body: buffer,
    ContentType: mimeType,
    ACL: "public-read",
  }));
  return `${SPACES_CDN}/${key}`;
}

interface Credentials {
  pageToken: string;
  pageId: string;
  appId: string;
  appSecret: string;
  apiVersion: string;
}

function getCredentials(req: Request): Credentials {
  // Sorrend: 1) szerveroldali oldal-nyilvantartas (?p=xx)
  //          2) kliens header (visszafele kompatibilitas)
  //          3) egyetlen env default
  const page = resolvePage(req);
  return {
    pageToken: page?.token || (req.headers["x-fb-page-access-token"] as string) || DEFAULT_PAGE_TOKEN,
    pageId: page?.id || (req.headers["x-fb-page-id"] as string) || DEFAULT_PAGE_ID,
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
  if (creds.pageToken && creds.pageId) return;
  throw new Error(
    "Nincs megadva melyik oldalra szol a muvelet. Add meg a 'page' parametert " +
    "(a lehetoseget a list_pages tool adja vissza)."
  );
}

// ---------------------------------------------------------------------------
// KETLEPESES KEPES POSZT — VALODI BEJEGYZES (Post), NEM ALBUMFOTO
//
// HIBA volt: a kepet kozvetlenul a /photos vegponton publikaltuk. Az igy
// letrejovo objektum egy ALBUMFOTO (photo.php?fbid=...), NEM valodi oldal-
// bejegyzes (Post). A Facebook nem general hozza rendes "sztorit", ezert:
//   - a kep nem jelenik meg a "Bejegyzesek" kozt, csak a fotoalbumban
//   - a kovetok HIRFOLYAMABA soha nem jut el
//   - a valasz post_id-ja null, csak photo_id jott vissza
//
// HELYES eljaras (Facebook Graph API ajanlas):
//   1. /{page-id}/photos  published=false          -> photo_id (nem publikus)
//   2. /{page-id}/feed    attached_media[0]=...    -> VALODI Post
// ---------------------------------------------------------------------------

/** 1. lepes: kep feltoltese PUBLIKALATLANUL, nyers binarisbol. Visszaad: photo_id */
async function uploadUnpublishedPhotoBinary(
  apiVersion: string,
  pageId: string,
  pageToken: string,
  imageBuffer: Buffer,
  mimeType: string
): Promise<string> {
  const ext = mimeType.split("/")[1] ?? "png";
  const fd = new globalThis.FormData();
  fd.append("source", new Blob([new Uint8Array(imageBuffer)], { type: mimeType }), "photo." + ext);
  fd.append("published", "false");
  fd.append("access_token", pageToken);
  const url = "https://graph.facebook.com/" + apiVersion + "/" + pageId + "/photos";
  const r = await fetch(url, { method: "POST", body: fd });
  if (!r.ok) throw new Error("Facebook API hiba (kep feltoltes " + r.status + "): " + (await r.text()));
  const data = (await r.json()) as { id: string };
  if (!data.id) throw new Error("A Facebook nem adott vissza photo_id-t.");
  return data.id;
}

/** 1. lepes URL-bol: kep feltoltese PUBLIKALATLANUL. Visszaad: photo_id */
async function uploadUnpublishedPhotoByUrl(
  client: AxiosInstance,
  pageId: string,
  imageUrl: string
): Promise<string> {
  const { data } = await client.post<{ id: string }>("/" + pageId + "/photos", {
    url: imageUrl,
    published: false,
  });
  if (!data.id) throw new Error("A Facebook nem adott vissza photo_id-t.");
  return data.id;
}

/** 2. lepes: VALODI bejegyzes letrehozasa a /feed vegponton, csatolt fotoval */
async function createFeedPostWithMedia(
  client: AxiosInstance,
  pageId: string,
  message: string,
  photoIds: string[],
  published: boolean,
  scheduledTs?: number
): Promise<{ id: string }> {
  const params: Record<string, unknown> = {
    message,
    published,
    attached_media: photoIds.map((id) => ({ media_fbid: id })),
  };
  if (!published && scheduledTs !== undefined) params.scheduled_publish_time = scheduledTs;
  const { data } = await client.post<{ id: string }>("/" + pageId + "/feed", params);
  return data;
}


// Az oldalvalaszto parameter semaja — MINDEN tool megkapja.
// Igy egyetlen connector kezeli mind a 7 oldalt: nem kell 7 kulon MCP kapcsolat,
// amibol a kliensnel gyakran csak nehany epult fel.
function createMcpServer(req: Request): McpServer {
  const server = new McpServer({ name: "meta-marketing-agent", version: "4.0.0" });

  // A keresbol feloldjuk a ceget. Nincs ?t= -> DEFAULT_TENANT (visszafele kompatibilis).
  // Ismeretlen ?t= -> tenant null, es MINDEN muvelet beszedes hibat ad (nincs csendes
  // visszaesés mas ceg adataira).
  const tenant = resolveTenant(req);
  const pages: TenantPages = tenant?.pages ?? {};
  const pageKeys = Object.keys(pages).sort();

  const pageDesc =
    pageKeys.length > 0
      ? "Melyik Facebook oldal. Elerheto kulcsok: " +
        pageKeys.map((k) => {
          const n = pages[k].name;
          return n ? `${k} (${n})` : k;
        }).join(", ") +
        ". Kotelezo megadni. A pontos listat a list_pages tool adja vissza."
      : "Ehhez a ceghez egyetlen oldal sincs beallitva.";

  const pageParam = (
    pageKeys.length > 0 ? z.enum(pageKeys as [string, ...string[]]) : z.string()
  )
    .optional()
    .describe(pageDesc);

  // Oldalankenti hitelesites feloldasa: elsodleges a tool 'page' parametere,
  // masodlagos az URL ?p= parametere, vegul a header / env default.
  const credsFor = (page?: string): Credentials => {
    if (!tenant) {
      // Szandekosan NEM listazzuk a tobbi ceg nevet — az mas ugyfel adata.
      throw new Error(
        "Ismeretlen ceg-azonosito a ?t= parameterben. Ellenorizd a plugin .mcp.json URL-jet."
      );
    }
    const base = getCredentials(req);
    if (page) {
      const pc = pages[page.toLowerCase()];
      if (!pc) {
        throw new Error(
          `Ismeretlen oldal: '${page}'. Elerheto oldalak: ${pageKeys.join(", ") || "(egy sincs)"}`
        );
      }
      return { ...base, pageToken: pc.token, pageId: pc.id };
    }
    return base;
  };

  server.tool(
    "list_pages",
    "Kilistazza az OSSZES elerheto Facebook oldalt: a 'page' parameterhez hasznalhato kulcsot, az oldal nevet es a Facebook page ID-t. Ezt hasznald, ha nem tudod milyen oldalak vannak, vagy ha a felhasznalo azt kerdezi mely oldalakat kezeljuk.",
    {},
    async () => {
      if (!tenant) {
        return {
          content: [{ type: "text", text: JSON.stringify({
            error: "Ismeretlen ceg-azonosito a ?t= parameterben. Ellenorizd a plugin .mcp.json URL-jet.",
          }, null, 2) }],
          isError: true,
        };
      }
      const list = pageKeys.map((k) => ({
        page: k,
        name: pages[k].name ?? null,
        page_id: pages[k].id,
      }));
      return {
        content: [
          { type: "text", text: JSON.stringify({ ceg: tenant.key, total: list.length, pages: list }, null, 2) },
        ],
      };
    }
  );

  server.tool("list_posts", "Visszaadja az oldal legutobb bejegyzeseit.", {
    page: pageParam,
    limit: z.number().int().min(1).max(100).optional().default(10),
    include_scheduled: z.boolean().optional().default(false),
  }, async ({ page, limit, include_scheduled }) => {
    const creds = credsFor(page);
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
    page: pageParam,
    post_id: z.string().min(1),
  }, async ({ page, post_id }) => {
    const creds = credsFor(page);
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
    page: pageParam,
    message: z.string().min(1).max(63206),
    image_base64: z.string().optional(),
    image_mime_type: z.enum(["image/png", "image/jpeg", "image/gif", "image/webp"]).optional().default("image/png"),
    image_url: z.string().url().optional(),
    image_path: z.string().optional(),
    link: z.string().url().optional(),
    published: z.boolean().optional().default(true),
    scheduled_publish_time: z.string().optional(),
    privacy: z.enum(["EVERYONE", "FRIENDS", "ONLY_ME"]).optional().default("EVERYONE"),
  }, async ({ page, message, image_base64, image_mime_type, image_url, image_path, link, published, scheduled_publish_time, privacy }) => {
    const creds = credsFor(page);
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
        // 1. publikalatlan fotofeltoltes -> 2. valodi Post a /feed vegponton
        const photoId = await uploadUnpublishedPhotoBinary(
          creds.apiVersion, creds.pageId, creds.pageToken, buffer, mimeType
        );
        const post = await createFeedPostWithMedia(
          client, creds.pageId, message, [photoId], published, scheduledTs
        );
        return { content: [{ type: "text", text: JSON.stringify({ success: true, action: published ? "Kozzetve (valodi bejegyzes, base64 kep)" : "Utemezve (valodi bejegyzes, base64 kep)", post_id: post.id, photo_id: photoId }, null, 2) }] };
      }

      if (image_path) {
        if (!existsSync(image_path)) throw new Error("A fajl nem talalhato: " + image_path);
        const fileBuffer = readFileSync(image_path);
        const ext = extname(image_path).toLowerCase().replace(".", "") || "jpeg";
        const mimeType = ext === "png" ? "image/png" : ext === "gif" ? "image/gif" : "image/jpeg";
        const photoId = await uploadUnpublishedPhotoBinary(
          creds.apiVersion, creds.pageId, creds.pageToken, fileBuffer, mimeType
        );
        const post = await createFeedPostWithMedia(
          client, creds.pageId, message, [photoId], published, scheduledTs
        );
        return { content: [{ type: "text", text: JSON.stringify({ success: true, action: published ? "Kozzetve (valodi bejegyzes, lokalis kep)" : "Utemezve (valodi bejegyzes, lokalis kep)", post_id: post.id, photo_id: photoId }, null, 2) }] };
      }

      if (image_url) {
        // 1. publikalatlan fotofeltoltes URL-bol -> 2. valodi Post a /feed vegponton
        const photoId = await uploadUnpublishedPhotoByUrl(client, creds.pageId, image_url);
        const post = await createFeedPostWithMedia(
          client, creds.pageId, message, [photoId], published, scheduledTs
        );
        return { content: [{ type: "text", text: JSON.stringify({ success: true, action: published ? "Kozzetve (valodi bejegyzes, URL-kep)" : "Utemezve (valodi bejegyzes, URL-kep)", post_id: post.id, photo_id: photoId }, null, 2) }] };
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
    page: pageParam,
    post_id: z.string().min(1),
    message: z.string().min(1).max(63206),
  }, async ({ page, post_id, message }) => {
    const creds = credsFor(page);
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
    page: pageParam,
    post_id: z.string().min(1),
  }, async ({ page, post_id }) => {
    const creds = credsFor(page);
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
    page: pageParam,
    post_id: z.string().min(1),
  }, async ({ page, post_id }) => {
    const creds = credsFor(page);
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      const { data } = await client.post<{ success: boolean }>("/" + post_id, { is_published: true });
      return { content: [{ type: "text", text: JSON.stringify({ success: data.success, published_post_id: post_id }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("get_page_info", "Visszaadja az oldal alapadatait.", { page: pageParam }, async ({ page }) => {
    const creds = credsFor(page);
    assertCredentials(creds);
    const client = createClient(creds);
    try {
      const fields = "id,name,category,fan_count,followers_count,about,website,link";
      const { data } = await client.get("/" + creds.pageId, { params: { fields } });
      return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("check_token", "Ellenorzi a Page Access Token ervenyet.", { page: pageParam }, async ({ page }) => {
    const creds = credsFor(page);
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
    page: pageParam,
    post_id: z.string().min(1),
  }, async ({ page, post_id }) => {
    const creds = credsFor(page);
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

  server.tool("upload_image", "Kepet tolt fel a DO Spaces CDN-re es visszaadja a nyilvanos URL-t. Ezt hasznald create_post elott ha helyi kepet szeretnel posztolni image_url-ken keresztul. A base64 csak a SZOVEGET kodold, a KEPET ne!", {
    image_base64: z.string().min(1),
    image_mime_type: z.enum(["image/jpeg", "image/png", "image/gif", "image/webp"]).optional().default("image/jpeg"),
  }, async ({ image_base64, image_mime_type }) => {
    try {
      const buffer = Buffer.from(image_base64, "base64");
      const url = await uploadToSpaces(buffer, image_mime_type ?? "image/jpeg");
      return { content: [{ type: "text", text: JSON.stringify({ success: true, url, tip: "Hasznald ezt: create_post(image_url='" + url + "')" }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: "Hiba: " + extractError(err) }], isError: true };
    }
  });

  server.tool("get_page_insights", "Visszaadja az oldal osszesitett statisztikait.", {
    page: pageParam,
    period: z.enum(["day","week","days_28","month","lifetime"]).optional().default("week"),
    since: z.string().optional(),
    until: z.string().optional(),
  }, async ({ page, period, since, until }) => {
    const creds = credsFor(page);
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

// MCP-specifikus hitelesites.
// KRITIKUS: a /mcp vegpont SOHA nem adhat 401-et. Az MCP spec szerint a 401 azt
// jelenti "OAuth hitelesites kell", amire a kliens (Cowork / Claude Desktop) OAuth
// discovery + Dynamic Client Registration folyamatot indit. Mivel ez a szerver
// statikus API kulcsot hasznal, a regisztracio elbukik es a connector nem tud
// csatlakozni: "Couldn't register with facebook-XX's sign-in service".
//
// Ezert a kezfogas (initialize, tools/list, ping) kulcs nelkul is atmegy — ez csak
// a tool-semakat adja vissza, adatot nem. A tenyleges muveletek (tools/call) viszont
// kulcsot igenyelnek, es hiba eseten JSON-RPC hibat adunk HTTP 200-ban, nem 401-et.
const OPEN_MCP_METHODS = new Set([
  "initialize",
  "ping",
  "tools/list",
  "resources/list",
  "prompts/list",
  "resources/templates/list",
]);

function requireApiKeyMcp(req: Request, res: Response, next: () => void): void {
  if (!SERVER_API_KEY) { next(); return; }

  const body = req.body as { method?: string; id?: unknown } | undefined;
  const method = body?.method ?? "";

  if (OPEN_MCP_METHODS.has(method) || method.startsWith("notifications/")) {
    next();
    return;
  }

  const key = req.headers["x-api-key"] as string;
  if (key !== SERVER_API_KEY) {
    res.status(200).json({
      jsonrpc: "2.0",
      id: body?.id ?? null,
      error: { code: -32001, message: "Ervenytelen vagy hianyzo API kulcs (x-api-key)." },
    });
    return;
  }
  next();
}

app.get("/health", (req: Request, res: Response) => {
  // Alapbol csak osszesito adat — nem listazunk ki minden ceget es oldalt
  // nyilvanosan. Egy konkret ceg oldalai: /health?t=<ceg>
  // Token SOHA nem kerul ki.
  const base = {
    status: "ok",
    service: "meta-marketing-agent",
    version: "4.0.0",
    default_tenant: DEFAULT_TENANT,
    total_tenants: TENANT_KEYS.length,
    total_pages: TENANT_KEYS.reduce((n, t) => n + Object.keys(TENANTS[t]).length, 0),
  };
  const raw = req.query?.t;
  const val = Array.isArray(raw) ? raw[0] : raw;
  if (typeof val !== "string" || !val.trim()) { res.json(base); return; }
  const key = val.trim().toLowerCase();
  const pages = TENANTS[key];
  if (!pages) { res.json({ ...base, tenant: key, error: "Ismeretlen ceg-azonosito." }); return; }
  res.json({
    ...base,
    tenant: key,
    pages: Object.fromEntries(
      Object.keys(pages).sort().map((k) => [k, { id: pages[k].id, name: pages[k].name ?? null }])
    ),
  });
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
    // x-message-b64: base64-kódolt üzenet (ékezetek + emojik biztonságos átviteléhez) — PREFERÁLT
    // x-message: sima szöveg (visszafelé kompatibilitás)
    const msgB64 = req.headers["x-message-b64"] as string;
    const message = msgB64
      ? Buffer.from(msgB64, "base64").toString("utf-8")
      : (req.headers["x-message"] as string) ?? "";
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
    const photoId = await uploadUnpublishedPhotoBinary(
      creds.apiVersion, creds.pageId, creds.pageToken, imageBuffer, mimeType
    );
    const post = await createFeedPostWithMedia(
      createClient(creds), creds.pageId, message, [photoId], published, scheduledTs
    );
    res.json({ success: true, post_id: post.id, photo_id: photoId });
  } catch (err) {
    res.status(500).json({ success: false, error: extractError(err) });
  }
});

// REST endpoint: POST /store-image
// Képet fogad (raw binary), feltölti DO Spaces CDN-re, visszaad publikus URL-t.
// Ezt használja az /upload-ui web form és a curl feltöltő szkript.
app.post("/store-image", requireApiKey, express.raw({ type: ["image/*", "application/octet-stream"], limit: "20mb" }), async (req: Request, res: Response) => {
  try {
    const imageBuffer = req.body as Buffer;
    if (!imageBuffer || imageBuffer.length === 0) {
      res.status(400).json({ success: false, error: "Nincs képfájl a request body-ban." });
      return;
    }
    const mimeType = ((req.headers["content-type"] as string) ?? "image/jpeg").split(";")[0].trim();
    const url = await uploadToSpaces(imageBuffer, mimeType);
    res.json({ success: true, url, tip: "Használd ezt: create_post(image_url='" + url + "')" });
  } catch (err) {
    res.status(500).json({ success: false, error: extractError(err) });
  }
});

// Web UI: GET /upload-ui?k={api_key}
// Drag-drop képfeltöltő oldal böngészőből — visszaad CDN URL-t amit Claude-nak adhatsz
app.get("/upload-ui", (_req: Request, res: Response) => {
  const apiKey = SERVER_API_KEY;
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(`<!DOCTYPE html>
<html lang="hu">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Kép feltöltő – Allinhoreca Media</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:Arial,sans-serif;background:#faf7f4;min-height:100vh;display:flex;align-items:center;justify-content:center}
  .card{background:#fff;border-radius:12px;padding:36px;max-width:540px;width:100%;box-shadow:0 4px 24px rgba(0,0,0,.08)}
  h1{color:#B84C00;font-size:22px;margin-bottom:8px}
  p{color:#666;font-size:14px;margin-bottom:24px}
  .dz{border:3px dashed #ddd;border-radius:10px;padding:48px 24px;text-align:center;cursor:pointer;transition:all .2s;background:#fdfaf8}
  .dz:hover,.dz.over{border-color:#B84C00;background:#fff5f0}
  .dz .icon{font-size:48px;margin-bottom:12px}
  .dz .label{color:#999;font-size:15px}
  input[type=file]{display:none}
  .progress{display:none;margin-top:20px;background:#f0f0f0;border-radius:6px;height:8px;overflow:hidden}
  .progress-bar{height:100%;background:#B84C00;width:0;transition:width .3s}
  .result{display:none;margin-top:20px;background:#f0fff0;border:1px solid #b2dfdb;border-radius:8px;padding:16px}
  .result .url{word-break:break-all;font-size:13px;color:#333;margin:8px 0 14px}
  .copy-btn{background:#B84C00;color:#fff;border:none;padding:10px 20px;border-radius:6px;cursor:pointer;font-size:14px;width:100%}
  .copy-btn:hover{background:#963d00}
  .error{display:none;margin-top:20px;background:#fff0f0;border:1px solid #ffcdd2;border-radius:8px;padding:16px;color:#c62828;font-size:14px}
  .copied{background:#388e3c!important}
</style>
</head>
<body>
<div class="card">
  <h1>📷 Allinhoreca Kép Feltöltő</h1>
  <p>Húzd ide a képet vagy kattints. A kép feltöltődik a CDN-re, majd visszakapsz egy URL-t amit Claude-nak adhatsz.</p>
  <div class="dz" id="dz">
    <div class="icon">🖼️</div>
    <div class="label">Húzd ide vagy kattints a kiválasztáshoz</div>
    <input type="file" id="fi" accept="image/*">
  </div>
  <div class="progress" id="prog"><div class="progress-bar" id="bar"></div></div>
  <div class="result" id="res">
    <b>✅ Feltöltve! Másold be ezt az URL-t Claude-nak:</b>
    <div class="url" id="url"></div>
    <button class="copy-btn" id="copy-btn">📋 URL Másolása</button>
  </div>
  <div class="error" id="err"></div>
</div>
<script>
const dz=document.getElementById('dz'),fi=document.getElementById('fi');
const prog=document.getElementById('prog'),bar=document.getElementById('bar');
const res=document.getElementById('res'),urlEl=document.getElementById('url');
const errEl=document.getElementById('err'),copyBtn=document.getElementById('copy-btn');
let lastUrl='';

async function upload(file){
  errEl.style.display='none'; res.style.display='none';
  prog.style.display='block'; bar.style.width='20%';
  try{
    bar.style.width='60%';
    const buf=await file.arrayBuffer();
    bar.style.width='80%';
    const resp=await fetch('/store-image',{
      method:'POST',
      headers:{'Content-Type':file.type,'x-api-key':'${apiKey}'},
      body:buf
    });
    const data=await resp.json();
    bar.style.width='100%';
    setTimeout(()=>{prog.style.display='none';},400);
    if(data.url){
      lastUrl=data.url;
      urlEl.textContent=data.url;
      res.style.display='block';
    } else {
      errEl.textContent='Hiba: '+(data.error||'ismeretlen hiba');
      errEl.style.display='block';
    }
  } catch(e){
    prog.style.display='none';
    errEl.textContent='Hálózati hiba: '+e.message;
    errEl.style.display='block';
  }
}

fi.onchange=e=>e.target.files[0]&&upload(e.target.files[0]);
dz.onclick=()=>fi.click();
dz.ondragover=e=>{e.preventDefault();dz.classList.add('over')};
dz.ondragleave=()=>dz.classList.remove('over');
dz.ondrop=e=>{e.preventDefault();dz.classList.remove('over');e.dataTransfer.files[0]&&upload(e.dataTransfer.files[0])};
copyBtn.onclick=()=>{
  navigator.clipboard.writeText(lastUrl).then(()=>{
    copyBtn.textContent='✅ Másolva!';
    copyBtn.classList.add('copied');
    setTimeout(()=>{copyBtn.textContent='📋 URL Másolása';copyBtn.classList.remove('copied');},2000);
  });
};
</script>
</body>
</html>`);
});

app.post("/mcp", requireApiKeyMcp, async (req: Request, res: Response) => {
  const server = createMcpServer(req);
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  res.on("close", () => { transport.close(); server.close(); });
  try {
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    if (!res.headersSent) res.status(500).json({ error: "Belso szerverhiba", details: String(err) });
  }
});

app.get("/mcp", async (_req: Request, res: Response) => {
  res.status(405).json({ error: "A szerver stateless modban fut, GET nem tamogatott." });
});

app.delete("/mcp", async (_req: Request, res: Response) => {
  res.status(405).json({ error: "Session kezeles nem tamogatott." });
});

if (!process.env.VERCEL) {
  app.listen(PORT, "0.0.0.0", () => {
    console.log("Meta Marketing Agent fut: http://0.0.0.0:" + PORT + "/mcp");
    console.log("API key: " + (SERVER_API_KEY ? "BE" : "KI"));
  });
}

export default app;
