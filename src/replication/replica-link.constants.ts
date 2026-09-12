/**
 * Replica-link: a persistent control channel between the master and its replicas.
 *
 * Replicas are typically behind NAT / a Cloudflare tunnel and are NOT reachable from
 * the master. So the REPLICA dials OUT to the master (Socket.IO client → the master's
 * public URL) and keeps the connection open. The master can then ask a replica "do you
 * have this file?" over the existing socket and get a live, authoritative answer — used
 * by the file-backups API instead of inferring presence from sync timestamps.
 *
 * The same socket carries deployment propagation: after the master finishes its own
 * pipeline it orders every connected replica to deploy itself, and the replicas stream
 * their step status and log output back over the link (see DEPLOY_EVENT below).
 */

export const REPLICA_LINK_NAMESPACE = "/replication/replica-link";

/** Master → replica: "do you currently have this file?" (ack-based request/response). */
export const HAS_FILE_EVENT = "has-file";

/** cloud-dir-relative path (same key mirror disks and replicas use). */
export interface HasFileRequest {
   path: string;
}

export interface ReplicaMirrorReport {
   /** The mirror-dir root as configured on the replica. */
   dir: string;
   present: boolean;
}

/** Replica → master ack payload for a HAS_FILE_EVENT. */
export interface HasFileReply {
   /** True if the file exists in the replica's own cloud-dir. */
   cloudDir: boolean;
   /** Per configured mirror disk on the replica. */
   mirrors: ReplicaMirrorReport[];
}

/* ------------------------------------------------------------------ *
 * Deployment propagation (master → replica order, replica → master logs)
 * ------------------------------------------------------------------ */

/**
 * Master → replica: "deploy yourself now" (ack-based; the ack only reports whether the
 * order was ACCEPTED, the run itself is reported progressively over DEPLOY_PROGRESS_EVENT).
 *
 * Deliberately NOT a remote-exec channel: the order carries no commands. Each replica runs
 * a fixed, built-in pipeline (see ReplicaDeployRunner), so a compromised master can trigger a
 * deployment but cannot choose what gets executed. The ack reports that pipeline back so the
 * master can render the replica's timeline before any step starts.
 */
export const DEPLOY_EVENT = "deploy";

/** Replica → master: progressive status + log output for an accepted deployment order. */
export const DEPLOY_PROGRESS_EVENT = "deploy-progress";

export interface ReplicaDeployRequest {
   /** Correlates the order with every progress frame it produces. */
   runId: string;
   /** The master's project slug that triggered this — informational, for the replica's logs. */
   project: string;
   /** Git branch the master deployed — informational. */
   branch?: string;
   /** Who triggered the master deployment — informational. */
   triggeredBy?: string;
   /**
    * IMAGE deployment. When present, the replica does not build anything: it downloads each
    * artifact from the master, verifies it, swaps the image and recreates the container.
    *
    * Absent means the legacy source-based path (the replica runs its own fixed pipeline). Kept
    * optional so a replica without the updater still works during the transition.
    */
   images?: ReplicaImageRef[];
}

/** One image the replica should be running after this deployment. */
export interface ReplicaImageRef {
   /** Compose service name on the replica, e.g. "shado-cloud". */
   service: string;
   /**
    * Docker image ID the master built. Verified after `docker load` — content-addressed, so a
    * tampered or truncated artifact cannot masquerade as this image.
    */
   imageId: string;
   /**
    * SHA-256 of the exported tarball, verified BEFORE anything is loaded. Checking the archive
    * first means a corrupt or substituted download never reaches the Docker daemon.
    */
   tarSha256: string;
   /** Uncompressed artifact size in bytes, for progress reporting. */
   size: number;
   /** Opaque artifact id to download from the master: GET /replication/image/:artifact */
   artifact: string;
}

/** One step of the replica's own, locally-configured pipeline. */
export interface ReplicaDeployStepInfo {
   step: string;
   name: string;
}

/** Replica → master ack for a DEPLOY_EVENT. */
export interface ReplicaDeployAck {
   accepted: boolean;
   /** Why the order was refused (already deploying, disabled, no pipeline configured, …). */
   reason?: string;
   /** The directory the replica will deploy in — shown in the UI. */
   workDir?: string;
   /** The replica's pipeline, so the master can render the timeline before any step starts. */
   steps?: ReplicaDeployStepInfo[];
}

export type ReplicaStepStatus = "pending" | "running" | "success" | "failed";

export type ReplicaDeployPhase = "step_start" | "step_output" | "step_complete" | "finished";

/** Replica → master progress frame. One `finished` frame terminates a run. */
export interface ReplicaDeployProgress {
   runId: string;
   phase: ReplicaDeployPhase;
   /** Step id — set on step_start / step_output / step_complete. */
   step?: string;
   /** Status of `step` — set on step_complete. */
   status?: ReplicaStepStatus;
   /** Log chunk (already ANSI-stripped) — set on step_output. */
   output?: string;
   /** Failure reason — set on step_complete(failed) and on finished(ok: false). */
   error?: string;
   /** finished only: did the whole pipeline succeed? */
   ok?: boolean;
   /**
    * finished only: the replica is about to restart itself, so this is the last frame it
    * can send. The master then waits for the replica to reconnect to confirm success.
    */
   restarting?: boolean;
   /** Epoch ms on the replica. */
   at: number;
}

/** Handshake auth the replica presents when connecting to the master. */
export interface ReplicaLinkAuth {
   /** Per-connection HMAC headers (same scheme as ServiceKeyGuard/signServiceHeaders). */
   "x-service-timestamp": string;
   "x-service-nonce": string;
   "x-service-signature": string;
   /** The replica's device name (os.hostname()) — combined with its IP to identify it. */
   deviceName: string;
   /** Number of mirror disks configured on the replica (informational). */
   mirrorDirs: number;
   /**
    * Which half of the replica is connecting.
    *
    * Two clients share this namespace per replica host: the APP answers file queries, and the
    * UPDATER applies deployments. Routing by role is not cosmetic — without it a deploy order
    * would fan out to both, and the app would start a source deployment while the updater
    * swapped its image underneath it.
    *
    * Absent means "app", so a replica running an older build still behaves as before.
    */
   role?: ReplicaLinkRole;
}

export type ReplicaLinkRole = "app" | "updater";
