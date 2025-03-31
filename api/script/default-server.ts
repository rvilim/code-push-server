// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import * as api from "./api";
import { AzureStorage } from "./storage/azure-storage";
import { fileUploadMiddleware } from "./file-upload-manager";
import { JsonStorage } from "./storage/json-storage";
import { RedisManager } from "./redis-manager";
import { Storage } from "./storage/storage";
import { Response } from "express";
const { DefaultAzureCredential } = require("@azure/identity");
const { SecretClient } = require("@azure/keyvault-secrets");
// --- Add Firestore/S3 imports ---
import { FirestoreStorage, FirestoreStorageConfig } from "./storage/firestore-storage";
import { S3StorageConfig } from "./storage/s3-storage";
import { S3Storage } from "./storage/s3-storage";
// --- End imports ---

import * as bodyParser from "body-parser";
const domain = require("express-domain-middleware");
import * as express from "express";
import * as q from "q";

interface Secret {
  id: string;
  value: string;
}

function bodyParserErrorHandler(err: any, req: express.Request, res: express.Response, next: Function): void {
  // ... (original code) ...
  if (err) {
    if (err.message === "invalid json" || (err.name === "SyntaxError" && ~err.stack.indexOf("body-parser"))) {
      req.body = null;
      next();
    } else {
      next(err);
    }
  } else {
    next();
  }
}

export function start(done: (err?: any, server?: express.Express, storage?: Storage) => void, useJsonStorage_DEPRECATED?: boolean): void {
  let storage: Storage;
  // --- Removed isKeyVaultConfigured and keyvaultClient variables here ---

  // Read storage backend choice from .env, default to 'azure' if not specified
  const storageBackend = process.env.STORAGE_BACKEND || 'azure';
  // Note: useJsonStorage parameter is deprecated in favor of STORAGE_BACKEND=json
  if (useJsonStorage_DEPRECATED && storageBackend !== 'json') {
    console.warn("Warning: useJsonStorage parameter is deprecated. Use STORAGE_BACKEND=json in .env instead.");
  }


  q<void>(null)
    .then(async () => {
      // ***********************************************
      // Replace original storage initialization block
      // with the new switch statement
      // ***********************************************
      console.log(`Attempting to initialize storage backend: ${storageBackend}`);

      switch (storageBackend.toLowerCase()) {
        case 'json':
          console.log("Initializing JSON storage (for testing/development).");
          storage = new JsonStorage();
          break;

        case 'azure':
          console.log("Initializing Azure storage.");
          const azureAccount = process.env.AZURE_STORAGE_ACCOUNT;
          const keyVaultAccount = process.env.AZURE_KEYVAULT_ACCOUNT;
          let azureKey = process.env.AZURE_STORAGE_ACCESS_KEY;

          if (!azureAccount) {
            throw new Error("Azure storage selected, but AZURE_STORAGE_ACCOUNT environment variable is not set.");
          }

          if (keyVaultAccount && !azureKey) {
            // If KeyVault is specified and direct key isn't, try KeyVault
            console.log(`Azure KeyVault configured (${keyVaultAccount}), attempting to retrieve storage key...`);
            try {
              // DefaultAzureCredential will use environment variables (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
              // or managed identity, or Azure CLI login etc.
              const credential = new DefaultAzureCredential();
              const url = `https://${keyVaultAccount}.vault.azure.net`;
              const keyvaultClient = new SecretClient(url, credential); // Define keyvaultClient locally
              // Assume secret name is 'storage-<accountname>' unless specified otherwise
              const secretName = process.env.AZURE_KEYVAULT_SECRET_NAME || `storage-${azureAccount}`;
              console.log(`Fetching secret '${secretName}' from KeyVault ${keyVaultAccount}.`);
              const secret = await keyvaultClient.getSecret(secretName);
              azureKey = secret.value;
              if (!azureKey) {
                throw new Error(`Secret '${secretName}' retrieved from KeyVault but has no value.`);
              }
              console.log(`Successfully retrieved secret '${secretName}' from KeyVault.`);
            } catch (kvError) {
              console.error(`Failed to retrieve secret from KeyVault: ${kvError}`);
              throw new Error(`Failed to retrieve secret from KeyVault '${keyVaultAccount}': ${kvError.message}. Ensure credentials are valid and secret exists.`);
            }
          } else if (!azureKey) {
            // If KeyVault isn't configured and key isn't set directly, fail
            throw new Error("Azure storage selected, but AZURE_STORAGE_ACCESS_KEY is not set (and AZURE_KEYVAULT_ACCOUNT is not configured or failed).");
          }
          // If we reach here, we have the azureAccount and azureKey (either directly or from KeyVault)
          storage = new AzureStorage(azureAccount, azureKey);
          break;

        case 'firestore':
          console.log("Initializing Firestore storage with S3 backend.");
          // Read necessary env vars for Firestore and S3
          const gcpProjectId = process.env.GCP_PROJECT_ID;
          const gcpKeyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS; // Optional
          const s3Endpoint = process.env.S3_ENDPOINT;
          const s3Region = process.env.S3_REGION;
          const s3AccessKey = process.env.S3_ACCESS_KEY_ID;
          const s3SecretKey = process.env.S3_SECRET_ACCESS_KEY;
          const s3ForcePath = process.env.S3_FORCE_PATH_STYLE === 'true';
          const s3PublicUrl = process.env.S3_PUBLIC_URL_BASE; // Optional
          const packageBucket = process.env.CODEPUSH_PACKAGE_BUCKET;
          const historyBucket = process.env.CODEPUSH_HISTORY_BUCKET;
          // --- Read max history length from env ---
          const maxHistoryStr = process.env.CODEPUSH_MAX_HISTORY || "50"; // Default to 50 if not set
          // --- Read Firestore database ID from env ---
          const firestoreDbId = process.env.FIRESTORE_DATABASE_ID; // Optional, can be undefined

          // Validate required configuration for this backend
          const missingConfig: string[] = [];
          if (!gcpProjectId) missingConfig.push("GCP_PROJECT_ID");
          if (!s3Endpoint) missingConfig.push("S3_ENDPOINT");
          if (!s3Region) missingConfig.push("S3_REGION");
          if (!s3AccessKey) missingConfig.push("S3_ACCESS_KEY_ID");
          if (!s3SecretKey) missingConfig.push("S3_SECRET_ACCESS_KEY");
          if (!packageBucket) missingConfig.push("CODEPUSH_PACKAGE_BUCKET");
          if (!historyBucket) missingConfig.push("CODEPUSH_HISTORY_BUCKET");
          // Optional: Validate maxHistory is a number?
          const maxHistory = parseInt(maxHistoryStr, 10);
          if (isNaN(maxHistory)) {
            missingConfig.push("CODEPUSH_MAX_HISTORY (must be a number)");
          }

          if (missingConfig.length > 0) {
            throw new Error(`Firestore storage selected, but the following environment variables are missing: ${missingConfig.join(', ')}`);
          }

          // Construct S3 config part
          const s3Config: S3StorageConfig = {
            endpoint: s3Endpoint,
            region: s3Region,
            accessKeyId: s3AccessKey,
            secretAccessKey: s3SecretKey,
            forcePathStyle: s3ForcePath,
            publicUrlBase: s3PublicUrl // Can be undefined
          };

          // Construct Firestore config
          const firestoreConfig: FirestoreStorageConfig = {
            serviceAccount: gcpKeyFile, // Use correct property name if changed in FirestoreStorageConfig
            // --- Pass the database ID from env (will be undefined if not set) ---
            databaseId: firestoreDbId,
            // --- Add bucket names and history length from env ---
            packageBucketName: packageBucket,
            historyBucketName: historyBucket,
            maxPackageHistoryLength: maxHistory
          };

          // --- Create the S3 Storage instance ---
          const s3Storage = new S3Storage(s3Config); // Instantiate S3Storage

          // --- Pass both configs to FirestoreStorage ---
          storage = new FirestoreStorage(firestoreConfig, s3Storage); // Pass s3Storage instance
          break;

        default:
          // Handle invalid backend selection
          throw new Error(`Invalid STORAGE_BACKEND specified: '${storageBackend}'. Valid options are 'azure', 'firestore', 'json'.`);
      }

      // --- End of storage initialization block ---
      console.log(`Storage backend '${storageBackend}' initialized successfully.`);
      return storage.checkHealth(); // Perform health check after initialization
    })
    .then(() => {
      // ***********************************************
      // The rest of the original code for creating the app
      // and adding middleware remains largely the same
      // ***********************************************
      console.log("Storage health check passed. Creating Express app...");
      const app = express();
      const auth = api.auth({ storage: storage }); // Use the initialized storage
      const appInsights = api.appInsights();
      const redisManager = new RedisManager();

      // First, to wrap all requests and catch all exceptions.
      app.use(domain);

      // Monkey-patch res.send and res.setHeader to no-op after the first call and prevent "already sent" errors.
      app.use((req: express.Request, res: express.Response, next: (err?: any) => void): any => {
        // ... (original monkey-patch code) ...
        const originalSend = res.send;
        const originalSetHeader = res.setHeader;
        res.setHeader = (name: string, value: string | number | readonly string[]): Response => {
          if (!res.headersSent) {
            originalSetHeader.apply(res, [name, value]);
          }

          return {} as Response;
        };

        res.send = (body: any) => {
          if (res.headersSent) {
            return res;
          }

          return originalSend.apply(res, [body]);
        };

        next();
      });

      if (process.env.LOGGING === "true") {
        // ... (original logging code) ...
        app.use((req: express.Request, res: express.Response, next: (err?: any) => void): any => {
          console.log(); // Newline to mark new request
          console.log(`[REST] Received ${req.method} request at ${req.originalUrl}`);
          next();
        });
      }

      // Enforce a timeout on all requests.
      app.use(api.requestTimeoutHandler());

      // Before other middleware which may use request data that this middleware modifies.
      app.use(api.inputSanitizer());

      // body-parser must be before the Application Insights router.
      app.use(bodyParser.urlencoded({ extended: true }));
      const jsonOptions: any = { limit: "10kb", strict: true };
      if (process.env.LOG_INVALID_JSON_REQUESTS === "true") {
        // ... (original jsonOptions.verify code) ...
        jsonOptions.verify = (req: express.Request, res: express.Response, buf: Buffer, encoding: string) => {
          if (buf && buf.length) {
            (<any>req).rawBody = buf.toString();
          }
        };
      }

      app.use(bodyParser.json(jsonOptions));

      // If body-parser throws an error, catch it and set the request body to null.
      app.use(bodyParserErrorHandler);

      // Before all other middleware to ensure all requests are tracked.
      app.use(appInsights.router());

      app.get("/", (req: express.Request, res: express.Response, next: (err?: Error) => void): any => {
        res.send("Welcome to the CodePush REST API!");
      });

      app.set("etag", false);
      app.set("views", __dirname + "/views");
      app.set("view engine", "ejs");
      app.use("/auth/images/", express.static(__dirname + "/views/images"));
      app.use(api.headers({ origin: process.env.CORS_ORIGIN || "http://localhost:4000" }));
      app.use(api.health({ storage: storage, redisManager: redisManager })); // Pass initialized storage

      if (process.env.DISABLE_ACQUISITION !== "true") {
        app.use("/v0.1/public", api.acquisition({ storage: storage, redisManager: redisManager })); // Pass initialized storage
      }

      if (process.env.DISABLE_MANAGEMENT !== "true") {
        if (process.env.DEBUG_DISABLE_AUTH === "true") {
          // ... (original debug auth code) ...
          app.use((req, res, next) => {
            let userId: string = "default";
            if (process.env.DEBUG_USER_ID) {
              userId = process.env.DEBUG_USER_ID;
            } else {
              console.log("No DEBUG_USER_ID environment variable configured. Using 'default' as user id");
            }

            req.user = {
              id: userId,
            };

            next();
          });
        } else {
          app.use(auth.router());
        }
        // Pass initialized storage to api.management, but not to fileUploadMiddleware
        app.use(auth.authenticate, fileUploadMiddleware, api.management({ storage: storage, redisManager: redisManager })); // Corrected call
      } else {
        app.use(auth.legacyRouter());
      }

      // Error handler needs to be the last middleware so that it can catch all unhandled exceptions
      app.use(appInsights.errorHandler);

      // --- Removed the KeyVault credential refresh interval block ---
      // (KeyVault handling is now part of the initial Azure storage setup)

      console.log("Express app configured.");
      done(null, app, storage); // Pass the configured app and storage back
    })
    .catch((err) => { // Use catch instead of done for promise errors
      console.error(`Error during server startup with ${storageBackend} backend:`, err);
      if (done) {
        done(err); // Inform the callback
      }
      process.exit(1); // Exit on startup failure
    });
  // Removed the final .done() as .catch() handles errors
}