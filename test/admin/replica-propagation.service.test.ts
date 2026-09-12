import { Subject } from "rxjs";
import { ReplicaPropagationService, type PropagationCallbacks } from "src/admin/replica-propagation.service";
import type { ReplicaLinkInfo, ReplicaProgressEvent, ReplicaDispatchResult } from "src/replication/replica-link.registry";
import type { ReplicaDeployProgress, ReplicaDeployRequest } from "src/replication/replica-link.constants";

/**
 * Fake ReplicaLinkRegistry: lets a test control which replicas are "connected", what they
 * answer to a deployment order, and what they stream back afterwards.
 */
function makeRegistry() {
   const deployProgress$ = new Subject<ReplicaProgressEvent>();
   const replicaOnline$ = new Subject<ReplicaLinkInfo>();
   let connected: ReplicaLinkInfo[] = [];
   let acks: Record<string, ReplicaDispatchResult["ack"] | undefined> = {};

   const registry = {
      deployProgress$,
      replicaOnline$,
      list: (role: string = "app") => connected.filter((r) => role === "any" || r.role === role),
      connectedCount: () => connected.filter((r) => r.role === "app").length,
      dispatchDeploy: jest.fn(async (req: ReplicaDeployRequest, _timeoutMs?: number): Promise<ReplicaDispatchResult[]> =>
         // Mirrors the real registry: an image order goes to updaters, a source order to apps.
         connected.filter((r) => r.role === (req.images?.length ? "updater" : "app")).map((replica) => {
            const ack = acks[replica.id];
            return ack ? { replica, ack } : { replica, ack: null, error: "Did not acknowledge the deployment order within 20s" };
         }),
      ),
   };

   return {
      registry,
      deployProgress$,
      replicaOnline$,
      /** The runId of the order that was actually dispatched, for building progress frames. */
      lastRunId: () => registry.dispatchDeploy.mock.calls[0][0].runId,
      /** Simulate a replica dialing in (optionally after a delay, mimicking reconnect). */
      connect(replica: ReplicaLinkInfo, ack: ReplicaDispatchResult["ack"] = { accepted: true, workDir: "/app", steps: [{ step: "git_pull", name: "Git Pull" }] }) {
         connected = [...connected, replica];
         acks[replica.id] = ack ?? undefined;
         replicaOnline$.next(replica);
      },
      disconnect(id: string) {
         connected = connected.filter((r) => r.id !== id);
      },
      setAck(id: string, ack: ReplicaDispatchResult["ack"] | undefined) {
         acks[id] = ack;
      },
      reset() {
         connected = [];
         acks = {};
      },
   };
}

function replicaInfo(
   id: string,
   deviceName = `box-${id}`,
   ip = `10.0.0.${id.replace(/\D/g, "") || 1}`,
   role: "app" | "updater" = "app",
): ReplicaLinkInfo {
   return { id, ip, deviceName, mirrorDirs: 0, connectedAt: Date.now(), role };
}

function makeCallbacks() {
   const logs: string[] = [];
   const updates: string[] = [];
   const outputs: { replicaId: string; output: string }[] = [];
   const cb: PropagationCallbacks = {
      onLog: (line) => logs.push(line),
      onDispatch: () => undefined,
      onReplicaUpdate: (r) => updates.push(`${r.id}:${r.status}`),
      onReplicaOutput: (replicaId, output) => outputs.push({ replicaId, output }),
   };
   return { cb, logs, updates, outputs };
}

/** Fast-running propagation options; the real defaults are minutes long. */
const fastOpts = {
   deploymentId: "deploy_1",
   project: "backend",
   waitForReplicasMs: 300,
   replicaSettleMs: 20,
   dispatchTimeoutMs: 100,
   timeoutMs: 500,
   restartGraceMs: 150,
};

function frame(partial: Partial<ReplicaDeployProgress>): ReplicaDeployProgress {
   return { runId: "", phase: "step_output", at: Date.now(), ...partial } as ReplicaDeployProgress;
}

describe("ReplicaPropagationService", () => {
   let harness: ReturnType<typeof makeRegistry>;
   let service: ReplicaPropagationService;

   beforeEach(() => {
      harness = makeRegistry();
      service = new ReplicaPropagationService(harness.registry as any);
   });

   describe("waiting for replicas", () => {
      it("gives up after waitForReplicasMs when no replica ever connects", async () => {
         const { cb, logs } = makeCallbacks();

         const state = await service.propagate(fastOpts, cb);

         expect(state.replicas).toEqual([]);
         expect(state.finishedAt).toBeDefined();
         expect(harness.registry.dispatchDeploy).not.toHaveBeenCalled();
         expect(logs.join("")).toContain("no replica app reconnected");
         expect(logs.join("")).toContain("nothing to propagate to");
      });

      it("waits for a replica that reconnects during the window and deploys to it", async () => {
         const { cb, logs } = makeCallbacks();
         // Mimic a replica re-dialing after the primary's restart dropped the link.
         setTimeout(() => harness.connect(replicaInfo("s1")), 60);

         const state = await service.propagate(fastOpts, cb);

         expect(harness.registry.dispatchDeploy).toHaveBeenCalledTimes(1);
         expect(state.replicas.map((r) => r.id)).toEqual(["s1"]);
         expect(logs.join("")).toContain("waiting up to");
         expect(logs.join("")).toContain("Replica app online: box-s1");
      });

      it("collects replicas that reconnect within the settle window, not just the first", async () => {
         const { cb } = makeCallbacks();
         harness.connect(replicaInfo("s1"));
         setTimeout(() => harness.connect(replicaInfo("s2")), 10);

         const state = await service.propagate(fastOpts, cb);

         expect(state.replicas.map((r) => r.id).sort()).toEqual(["s1", "s2"]);
      });
   });

   describe("dispatch outcomes", () => {
      it("marks a replica that refuses the order as rejected, with its reason", async () => {
         const { cb } = makeCallbacks();
         harness.connect(replicaInfo("s1"), { accepted: false, reason: "git is not installed in this replica's container image" });

         const state = await service.propagate(fastOpts, cb);

         expect(state.replicas[0].status).toBe("rejected");
         expect(state.replicas[0].reason).toContain("git is not installed");
         expect(service.allSucceeded(state)).toBe(false);
         expect(service.summarize(state)).toBe("0/1 replicas deployed");
      });

      it("marks a replica that never acknowledges as unreachable", async () => {
         const { cb } = makeCallbacks();
         harness.connect(replicaInfo("s1"), null);

         const state = await service.propagate(fastOpts, cb);

         expect(state.replicas[0].status).toBe("unreachable");
         expect(state.replicas[0].reason).toContain("Did not acknowledge");
      });
   });

   describe("progress streaming", () => {
      it("applies step transitions and log output onto the replica's state", async () => {
         const { cb, outputs } = makeCallbacks();
         const info = replicaInfo("s1");
         harness.connect(info);

         const promise = service.propagate(fastOpts, cb);

         // Let the wait window settle and dispatch happen, then stream a full run.
         await new Promise((r) => setTimeout(r, 60));
         const runId = harness.lastRunId();
         harness.deployProgress$.next({ replica: info, progress: frame({ runId, phase: "step_start", step: "git_pull" }) });
         harness.deployProgress$.next({ replica: info, progress: frame({ runId, phase: "step_output", step: "git_pull", output: "Already up to date.\n" }) });
         harness.deployProgress$.next({ replica: info, progress: frame({ runId, phase: "step_complete", step: "git_pull", status: "success" }) });
         harness.deployProgress$.next({ replica: info, progress: frame({ runId, phase: "finished", ok: true }) });

         const state = await promise;

         expect(state.replicas[0].status).toBe("success");
         expect(state.replicas[0].steps[0].status).toBe("success");
         expect(state.replicas[0].output).toContain("Already up to date.");
         expect(outputs).toEqual([{ replicaId: "s1", output: "Already up to date.\n" }]);
         expect(service.allSucceeded(state)).toBe(true);
      });

      it("ignores progress frames from a different run", async () => {
         const { cb } = makeCallbacks();
         const info = replicaInfo("s1");
         harness.connect(info);

         const promise = service.propagate(fastOpts, cb);
         await new Promise((r) => setTimeout(r, 60));

         harness.deployProgress$.next({ replica: info, progress: frame({ runId: "some_other_run", phase: "finished", ok: false, error: "boom" }) });
         const runId = harness.lastRunId();
         harness.deployProgress$.next({ replica: info, progress: frame({ runId, phase: "finished", ok: true }) });

         const state = await promise;
         expect(state.replicas[0].status).toBe("success");
      });

      it("marks a failed pipeline as failed with the reported error", async () => {
         const { cb } = makeCallbacks();
         const info = replicaInfo("s1");
         harness.connect(info);

         const promise = service.propagate(fastOpts, cb);
         await new Promise((r) => setTimeout(r, 60));
         const runId = harness.lastRunId();
         harness.deployProgress$.next({ replica: info, progress: frame({ runId, phase: "finished", ok: false, error: "Build: Process exited with code 1" }) });

         const state = await promise;
         expect(state.replicas[0].status).toBe("failed");
         expect(state.replicas[0].reason).toContain("exited with code 1");
      });
   });

   describe("restart confirmation", () => {
      it("confirms success only once the replica reconnects to the link", async () => {
         const { cb } = makeCallbacks();
         const info = replicaInfo("s1");
         harness.connect(info);

         const promise = service.propagate(fastOpts, cb);
         await new Promise((r) => setTimeout(r, 60));
         const runId = harness.lastRunId();

         harness.deployProgress$.next({ replica: info, progress: frame({ runId, phase: "finished", ok: true, restarting: true }) });
         // The replica drops off, then comes back with a NEW socket id — matched on device+ip.
         harness.disconnect("s1");
         setTimeout(() => harness.connect({ ...info, id: "s1-reconnected" }), 30);

         const state = await promise;
         expect(state.replicas[0].status).toBe("success");
         expect(state.replicas[0].output).toContain("reconnected to the replica-link");
      });

      it("fails a replica that never comes back after its restart step", async () => {
         const { cb } = makeCallbacks();
         const info = replicaInfo("s1");
         harness.connect(info);

         const promise = service.propagate(fastOpts, cb);
         await new Promise((r) => setTimeout(r, 60));
         const runId = harness.lastRunId();

         harness.deployProgress$.next({ replica: info, progress: frame({ runId, phase: "finished", ok: true, restarting: true }) });
         harness.disconnect("s1");

         const state = await promise;
         expect(state.replicas[0].status).toBe("failed");
         expect(state.replicas[0].reason).toContain("Did not reconnect");
      });
   });

   describe("role routing", () => {
      /**
       * The app container is the thing being replaced by an image deployment, so it must not also
       * receive the order — otherwise two processes deploy the same replica at once, in different
       * ways. Source orders go the other way, to the app that deploys itself.
       */
      const image = { service: "shado-cloud", imageId: "sha256:new", tarSha256: "b".repeat(64), size: 1024, artifact: "b".repeat(32) };

      it("sends an image order to the updater, not the app", async () => {
         const { cb } = makeCallbacks();
         harness.connect(replicaInfo("app1", "box", "10.0.0.1", "app"));
         harness.connect(replicaInfo("upd1", "box", "10.0.0.1", "updater"));

         const promise = service.propagate({ ...fastOpts, images: [image] }, cb);
         await new Promise((r) => setTimeout(r, 60));
         const runId = harness.lastRunId();
         harness.deployProgress$.next({
            replica: replicaInfo("upd1", "box", "10.0.0.1", "updater"),
            progress: frame({ runId, phase: "finished", ok: true }),
         });

         const state = await promise;
         expect(state.replicas.map((r) => r.id)).toEqual(["upd1"]);
         expect(harness.registry.dispatchDeploy.mock.calls[0][0].images).toHaveLength(1);
      });

      it("sends a source order to the app, not the updater", async () => {
         const { cb } = makeCallbacks();
         harness.connect(replicaInfo("app1", "box", "10.0.0.1", "app"));
         harness.connect(replicaInfo("upd1", "box", "10.0.0.1", "updater"));

         const promise = service.propagate(fastOpts, cb);
         await new Promise((r) => setTimeout(r, 60));
         const runId = harness.lastRunId();
         harness.deployProgress$.next({
            replica: replicaInfo("app1", "box", "10.0.0.1", "app"),
            progress: frame({ runId, phase: "finished", ok: true }),
         });

         const state = await promise;
         expect(state.replicas.map((r) => r.id)).toEqual(["app1"]);
         expect(harness.registry.dispatchDeploy.mock.calls[0][0].images).toBeUndefined();
      });

      it("waits for an updater, not an app, when deploying images", async () => {
         const { cb, logs } = makeCallbacks();
         // An app is already online; the updater is still reconnecting. Waiting on the app would
         // dispatch to nobody.
         harness.connect(replicaInfo("app1", "box", "10.0.0.1", "app"));
         setTimeout(() => harness.connect(replicaInfo("upd1", "box", "10.0.0.1", "updater")), 60);

         const promise = service.propagate({ ...fastOpts, images: [image] }, cb);
         await new Promise((r) => setTimeout(r, 140));
         const runId = harness.lastRunId();
         harness.deployProgress$.next({
            replica: replicaInfo("upd1", "box", "10.0.0.1", "updater"),
            progress: frame({ runId, phase: "finished", ok: true }),
         });

         const state = await promise;
         expect(logs.join("")).toContain("No replica updater online yet");
         expect(state.replicas.map((r) => r.id)).toEqual(["upd1"]);
      });

      it("reports the image manifest in the step log", async () => {
         const { cb, logs } = makeCallbacks();
         harness.connect(replicaInfo("upd1", "box", "10.0.0.1", "updater"));

         const promise = service.propagate({ ...fastOpts, images: [image] }, cb);
         await new Promise((r) => setTimeout(r, 60));
         harness.deployProgress$.next({
            replica: replicaInfo("upd1", "box", "10.0.0.1", "updater"),
            progress: frame({ runId: harness.lastRunId(), phase: "finished", ok: true }),
         });
         await promise;

         expect(logs.join("")).toContain("Image deployment — 1 image(s) to replace");
         expect(logs.join("")).toContain("shado-cloud: sha256:new");
      });

      it("reports one host with both roles when app and updater share a machine", () => {
         harness.connect(replicaInfo("app1", "box", "10.0.0.1", "app"));
         harness.connect(replicaInfo("upd1", "box", "10.0.0.1", "updater"));

         const hosts = service.connectedReplicas();
         expect(hosts).toHaveLength(1);
         expect(hosts[0].deviceName).toBe("box");
         expect(hosts[0].roles.sort()).toEqual(["app", "updater"]);
      });

      it("reports a freshly provisioned replica that has only an updater", () => {
         // The case that presented as "no replicas online" while the operator's replica sat there
         // connected: an image has not been delivered yet, so there is no app half.
         harness.connect(replicaInfo("upd1", "fresh-box", "10.0.0.9", "updater"));

         const hosts = service.connectedReplicas();
         expect(hosts).toHaveLength(1);
         expect(hosts[0].roles).toEqual(["updater"]);
      });

      it("keeps distinct hosts separate", () => {
         harness.connect(replicaInfo("upd1", "box-a", "10.0.0.1", "updater"));
         harness.connect(replicaInfo("upd2", "box-b", "10.0.0.2", "updater"));

         expect(service.connectedReplicas().map((h) => h.deviceName).sort()).toEqual(["box-a", "box-b"]);
      });

      it("explains the mismatch when updaters are connected but the order carries no image", async () => {
         const { cb, logs } = makeCallbacks();
         harness.connect(replicaInfo("upd1", "box", "10.0.0.1", "updater"));

         // No images => targets "app", of which there are none, while an updater waits.
         await service.propagate(fastOpts, cb);

         const text = logs.join("");
         expect(text).toContain("1 replica updater(s) ARE connected: box");
         expect(text).toContain("Add a \"Build Replica Image\" step BEFORE this one");
      });

      it("explains the mismatch when an image is sent but no updater is running", async () => {
         const { cb, logs } = makeCallbacks();
         harness.connect(replicaInfo("app1", "box", "10.0.0.1", "app"));

         await service.propagate({ ...fastOpts, images: [image] }, cb);

         const text = logs.join("");
         expect(text).toContain("1 replica app(s) ARE connected: box");
         expect(text).toContain("Provision them with the shado-replica package");
      });
   });

   it("times out a replica that accepts the order then goes silent", async () => {
      const { cb } = makeCallbacks();
      harness.connect(replicaInfo("s1"));

      const state = await service.propagate(fastOpts, cb);

      expect(state.replicas[0].status).toBe("timeout");
      expect(state.replicas[0].reason).toContain("Went quiet");
   });
});
