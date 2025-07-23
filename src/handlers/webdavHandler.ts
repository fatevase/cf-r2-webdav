// 文件名：src/handlers/webdavHandler.ts
import { listAll, fromR2Object, make_resource_path, generatePropfindResponse } from '../utils/webdavUtils';
import { logger } from '../utils/logger';
import { generateHTML, generateErrorHTML } from '../utils/templates';
import { WebDAVProps, Env } from '../types';
import { authenticate } from '../utils/auth';

const SUPPORT_METHODS = ["OPTIONS", "PROPFIND", "MKCOL", "GET", "HEAD", "PUT", "COPY", "MOVE", "DELETE"];
const DAV_CLASS = "1, 2";

export async function handleWebDAV(request: Request, env: Env): Promise<Response> {
  const { BUCKET, BUCKET_NAME } = env;  // 从 env 中获取 BUCKET 和 BUCKET_NAME

  try {
    switch (request.method) {
      // 原来的处理逻辑不变
      case "OPTIONS":
        return handleOptions();
      case "HEAD":
        return await handleHead(request, BUCKET);
      case "GET":
        return await handleGet(request, BUCKET);
      case "PUT":
        return await handlePut(request, BUCKET);
      case "DELETE":
        return await handleDelete(request, BUCKET);
      case "MKCOL":
        return await handleMkcol(request, BUCKET);
      case "PROPFIND":
        return await handlePropfind(request, BUCKET);
      case "COPY":
        return await handleCopy(request, BUCKET);
      case "MOVE":
        return await handleMove(request, BUCKET);
      default:
        return new Response("Method Not Allowed", {
          status: 405,
          headers: {
            Allow: SUPPORT_METHODS.join(", "),
            DAV: DAV_CLASS
          }
        });
    }
  } catch (error) { 
    const err = error as Error;
    logger.error("Error in WebDAV handling:", err.message);
    return new Response(generateErrorHTML("Internal Server Error", err.message), {
      status: 500,
      headers: { "Content-Type": "text/html; charset=utf-8" }
    });
  }
}

function handleOptions(): Response {
  return new Response(null, {
    status: 200,
    headers: {
      Allow: SUPPORT_METHODS.join(", "),
      DAV: DAV_CLASS,
      "Access-Control-Allow-Methods": SUPPORT_METHODS.join(", "),
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Authorization, Content-Type, Depth, Overwrite, Destination, Range",
      "Access-Control-Expose-Headers": "Content-Type, Content-Length, DAV, ETag, Last-Modified, Location, Date, Content-Range",
      "Access-Control-Allow-Credentials": "true",
      "Access-Control-Max-Age": "86400"
    }
  });
}

async function handleHead(request: Request, bucket: R2Bucket): Promise<Response> {
  const resource_path = make_resource_path(request);
  const object = await bucket.head(resource_path);

  if (!object) {
    return new Response(null, { status: 404 });
  }

  return new Response(null, {
    status: 200,
    headers: {
      "Content-Type": object.httpMetadata?.contentType ?? "application/octet-stream",
      "Content-Length": object.size.toString(),
      "ETag": object.etag,
      "Last-Modified": object.uploaded.toUTCString()
    }
  });
}

async function handleGet(request: Request, bucket: R2Bucket): Promise<Response> {
  const resource_path = make_resource_path(request);

  if (request.url.endsWith("/")) {
    return await handleDirectory(bucket, resource_path);
  } else {
    return await handleFile(bucket, resource_path);
  }
}

async function handleDirectory(bucket: R2Bucket, resource_path: string): Promise<Response> {
  let items = [];
  if (resource_path !== "") {
    items.push({ name: "📁 ..", href: "../" });
  }

  try {
    // 仅获取当前目录的直接子项（不递归）
    for await (const object of listAll(bucket, resource_path)) {
      if (object.key === resource_path) continue;
      
      const isDirectory = object.customMetadata?.resourcetype === "collection";
      const href = isDirectory ? `/${object.key}` : `/${object.key}`;
      const displayName = object.displayname || object.key.split("/").pop() || object.key;
      
      items.push({ 
        name: `${isDirectory ? "📁 " : "📄 "}${displayName}`, 
        href 
      });
    }
  } catch (error) {
    logger.error("Error listing objects:", error.message);
    return new Response(generateErrorHTML("Error listing directory contents", error.message), {
      status: 500,
      headers: { "Content-Type": "text/html; charset=utf-8" }
    });
  }

  const page = generateHTML(`WebDAV File Browser[${bucketName}]`, items);
  return new Response(page, {
    status: 200,
    headers: { "Content-Type": "text/html; charset=utf-8" }
  });
}

async function handleFile(bucket: R2Bucket, resource_path: string): Promise<Response> {
  try {
    const object = await bucket.get(resource_path);
    if (!object) {
      return new Response("Not Found", { status: 404 });
    }
    return new Response(object.body, {
      status: 200,
      headers: {
        "Content-Type": object.httpMetadata?.contentType ?? "application/octet-stream",
        "Content-Length": object.size.toString(),
        "ETag": object.etag,
        "Last-Modified": object.uploaded.toUTCString()
      }
    });
  } catch (error) { 
    const err = error as Error;
    logger.error("Error getting object:", err.message);
    return new Response(generateErrorHTML("Error retrieving file", err.message), {
      status: 500,
      headers: { "Content-Type": "text/html; charset=utf-8" }
    });
  }
}

async function handlePut(request: Request, bucket: R2Bucket): Promise<Response> {
  const resource_path = make_resource_path(request);

  try {
    const body = await request.arrayBuffer();
    await bucket.put(resource_path, body, {
      httpMetadata: {
        contentType: request.headers.get("Content-Type") || "application/octet-stream",
      },
    });
    return new Response("Created", { status: 201 });
  } catch (error) { 
    const err = error as Error;
    logger.error("Error uploading file:", err.message);
    return new Response(generateErrorHTML("Error uploading file", err.message), {
      status: 500,
      headers: { "Content-Type": "text/html; charset=utf-8" }
    });
  }
}

async function handleDelete(request: Request, bucket: R2Bucket): Promise<Response> {
  const resource_path = make_resource_path(request);
  
  try {
    await bucket.delete(resource_path);
    
    return new Response(null, { status: 204 });
    
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    
    if (
      // check for common "not found" errors
      errorMessage.includes("not found") ||
      errorMessage.includes("does not exist") ||
      errorMessage.includes("404")
    ) {
      logger.info(`Resource already deleted: ${resource_path}`);
      return new Response(null, { status: 204 }); // 资源已删除，视为成功
    }
    
    logger.error("Unexpected error deleting object:", errorMessage);
    return new Response(null, { status: 500 });
  }
}

// 辅助函数：递归创建父目录
async function ensureParentDirectories(bucket: R2Bucket, path: String): Promise<void> {
  // 提取父目录路径（如 path 是 a/b/c/，则 parent 是 a/b/）
  const parts = path.split("/").filter(Boolean);
  if (parts.length <= 1) return; // 根目录或一级目录无需处理
  
  let parentPath = "";
  for (let i = 0; i < parts.length - 1; i++) {
    parentPath += parts[i] + "/" + "_$folder$";
    const exists = await bucket.head(parentPath);
    if (!exists) {
      await bucket.put(parentPath, new Uint8Array(), {
        customMetadata: { resourcetype: "collection" }
      });
    }
  }
}

async function handleMkcol(request: Request, bucket: R2Bucket): Promise<Response> {
  const resource_path = make_resource_path(request);
  if (resource_path === "") {
    return new Response("Method Not Allowed", { status: 405 });
  }
  
  const normalizedPath = resource_path.endsWith("/") 
    ? resource_path 
    : resource_path + "/";
  
  try {
    // 确保父目录存在
    // await ensureParentDirectories(bucket, normalizedPath);
    // 创建隐藏的标记文件，而非零字节对象
    const markerFileKey = `${normalizedPath}_$folder$`;
    const existing = await bucket.head(markerFileKey);
    if (existing) {
      return new Response("Alraedy Exist", { status: 201 });
    }
    
    await bucket.put(markerFileKey, new Uint8Array(), {
      customMetadata: { resourcetype: "collection" }
    });
    
    return new Response("Created", { status: 201 });
  } catch (error) {
    const err = error as Error;
    logger.error("Error creating collection:", err.message);
    return new Response(generateErrorHTML("Error creating collection", err.message), {
      status: 500,
      headers: { "Content-Type": "text/html; charset=utf-8" }
    });
  }
}

async function handlePropfind(request: Request, bucket: R2Bucket): Promise<Response> {
  const resource_path = make_resource_path(request);
  const depth = request.headers.get("Depth") || "infinity";
  try {
    const props = [];
    if (depth !== "0") {
      // Depth: 1 或 infinity 保持不变，遍历子资源
      for await (const object of listAll(bucket, resource_path)) {
        props.push(fromR2Object(object));
      }
    } else {

      // 非根目录：用 listAll 检测是否存在隐含文件夹或显式对象
      const prefix = resource_path; // 待检测的文件夹前缀（如 "tabby"）
      const listOptions = { prefix, delimiter: "/", maxKeys: 1 }; // 只查1项，提高效率
      const listResult = await bucket.list(listOptions);

      // 推断文件夹是否存在：
      // 1. 显式存在：delimitedPrefixes 包含 "tabby/"（显式文件夹对象）
      // 2. 隐含存在：objects 包含 "tabby/file.txt" 等子文件（无显式文件夹对象）
      const folderExists = 
        (listResult.delimitedPrefixes?.length || 0) > 0 || 
        (listResult.objects?.length || 0) > 0;

      if (folderExists) {
        // 文件夹存在（显式或隐含），构造元数据
        props.push({
          displayname: resource_path.split("/").pop() || resource_path, // 文件夹名称
          creationdate: new Date().toUTCString(), // 可用子文件最早创建时间推断
          getcontentlength: "0", // 逻辑文件夹大小为 0
          getcontenttype: "", 
          getetag: `"implicit-folder-${resource_path}"`, // 生成唯一 ETag
          getlastmodified: new Date().toUTCString(), // 可用子文件最新修改时间推断
          resourcetype: "collection" // 标记为文件夹
        });
      } else {
        // 无任何子资源，确实不存在
        return new Response("Not Found", { status: 404 });
      }

    }

    const xml = generatePropfindResponse(resource_path, props);
    return new Response(xml, {
      status: 207,
      headers: { "Content-Type": "application/xml; charset=utf-8" }
    });
  } catch (error) {
    const err = error as Error;
    logger.error("Error in PROPFIND:", err.message);
    return new Response(generateErrorHTML("Error in PROPFIND", err.message), {
      status: 500,
      headers: { "Content-Type": "application/xml; charset=utf-8" }
    });
  }
}

async function handleCopy(request: Request, bucket: R2Bucket): Promise<Response> {
  const sourcePath = make_resource_path(request);
  const destinationHeader = request.headers.get("Destination");
  if (!destinationHeader) {
    return new Response("Bad Request: Destination header is missing", { status: 400 });
  }
  const destinationUrl = new URL(destinationHeader);
  const destinationPath = make_resource_path(new Request(destinationUrl));

  try {
    const sourceObject = await bucket.get(sourcePath);
    if (!sourceObject) {
      return new Response("Not Found", { status: 404 });
    }

    await bucket.put(destinationPath, sourceObject.body, {
      httpMetadata: sourceObject.httpMetadata,
      customMetadata: sourceObject.customMetadata
    });

    return new Response("Created", { status: 201 });
  } catch (error) { 
    const err = error as Error;
    logger.error("Error copying object:", err.message);
    return new Response(generateErrorHTML("Error copying file", err.message), {
      status: 500,
      headers: { "Content-Type": "text/html; charset=utf-8" }
    });
  }
}

async function handleMove(request: Request, bucket: R2Bucket): Promise<Response> {
  const sourcePath = make_resource_path(request);
  const destinationHeader = request.headers.get("Destination");
  if (!destinationHeader) {
    return new Response("Bad Request: Destination header is missing", { status: 400 });
  }
  const destinationUrl = new URL(destinationHeader);
  const destinationPath = make_resource_path(new Request(destinationUrl));

  try {
    const sourceObject = await bucket.get(sourcePath);
    if (!sourceObject) {
      return new Response("Not Found", { status: 404 });
    }

    await bucket.put(destinationPath, sourceObject.body, {
      httpMetadata: sourceObject.httpMetadata,
      customMetadata: sourceObject.customMetadata
    });

    await bucket.delete(sourcePath);
    return new Response("No Content", { status: 204 });
  } catch (error) { 
    const err = error as Error;
    logger.error("Error moving object:", err.message);
    return new Response(generateErrorHTML("Error moving file", err.message), {
      status: 500,
      headers: { "Content-Type": "text/html; charset=utf-8" }
    });
  }
}
