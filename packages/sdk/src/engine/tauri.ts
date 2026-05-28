import { RecordId, Uuid } from "@surrealdb/sqon";
import {
    CallTerminatedError,
    ConnectionUnavailableError,
    UnexpectedServerResponseError,
} from "../errors";
import { parseRpcError } from "../internal/parse-error";
import type { LiveAction, LiveMessage, RpcRequest, RpcResponse } from "../types";
import { LIVE_ACTIONS } from "../types/live";
import type { ConnectionState, EngineEvents, SurrealEngine } from "../types/surreal";
import { Features } from "../utils";
import { ChannelIterator } from "../utils/channel-iterator";
import { Publisher } from "../utils/publisher";
import { RpcEngine } from "./rpc";

type Response = Record<string, unknown>;

type TauriMessage = {
    type: "Binary";
    data: number[];
};

type LiveChannels = Record<string, [LiveMessage]>;

type TauriCore = typeof import("@tauri-apps/api/core");

interface LivePayload {
    id: Uuid;
    action: LiveAction;
    result: LiveMessage;
    record: RecordId;
}

interface Call<T> {
    request: object;
    resolve: (value: T) => void;
    reject: (error: Error) => void;
}

async function loadTauriCore(): Promise<TauriCore> {
    return await import("@tauri-apps/api/core");
}

export async function getBucketFolderAllowlist(): Promise<string[]> {
    const { invoke } = await loadTauriCore();
    return await invoke<string[]>("surreal_bridge_bucket_folder_allowlist");
}

export async function getBridgeHealth(): Promise<void> {
    const { invoke } = await loadTauriCore();
    await invoke<void>("surreal_bridge_health");
}

/**
 * An engine that communicates with the embedded Tauri bridge over invoke + channels.
 */
export class TauriEngine extends RpcEngine implements SurrealEngine {
    #publisher = new Publisher<EngineEvents>();
    #calls = new Map<string, Call<unknown>>();
    #subscriptions = new Publisher<LiveChannels>();
    #connectionId: number | undefined;
    #active = false;
    #terminated = false;

    features = new Set([
        Features.LiveQueries,
        Features.RefreshTokens,
        Features.Sessions,
        Features.Transactions,
        Features.Api,
        Features.ExportImportRaw,
        Features.SurrealML,
    ]);

    async getBucketFolderAllowlist(): Promise<string[]> {
        return await getBucketFolderAllowlist();
    }

    override async health(): Promise<void> {
        await getBridgeHealth();
    }

    subscribe<K extends keyof EngineEvents>(
        event: K,
        listener: (...payload: EngineEvents[K]) => void,
    ): () => void {
        return this.#publisher.subscribe(event, listener);
    }

    open(state: ConnectionState): void {
        this._state = state;
        this.#terminated = false;
        this.#active = false;

        void this.#connect();
    }

    async close(): Promise<void> {
        if (this.#terminated) {
            return;
        }

        const connectionId = this.#connectionId;

        this.#terminated = true;
        this.#active = false;
        this.#connectionId = undefined;
        this._state = undefined;

        if (connectionId !== undefined) {
            try {
                const { invoke } = await loadTauriCore();
                await invoke("surreal_bridge_disconnect", { id: connectionId });
            } catch {
                // Ignore disconnect errors during teardown.
            }
        }

        for (const { reject } of this.#calls.values()) {
            reject(new CallTerminatedError());
        }
        this.#calls.clear();

        this.#publisher.publish("disconnected");
    }

    ready(): void {
        for (const { request } of this.#calls.values()) {
            void this.#sendRaw(request);
        }
    }

    override send<Method extends string, Params extends unknown[] | undefined, Result>(
        request: RpcRequest<Method, Params>,
    ): Promise<Result> {
        return new Promise((resolve, reject) => {
            if (!this.#active || this.#connectionId === undefined) {
                reject(new ConnectionUnavailableError());
                return;
            }

            const id = this._context.uniqueId();
            const call: Call<Result> = {
                request: { id, ...request },
                resolve,
                reject,
            };

            this.#calls.set(id, call as Call<unknown>);

            this.#sendRaw(call.request).catch((error) => {
                this.#calls.delete(id);
                reject(error instanceof Error ? error : new Error(String(error)));
            });
        });
    }

    override liveQuery(id: Uuid): AsyncIterable<LiveMessage> {
        const channel = new ChannelIterator<LiveMessage>(() => {
            unsub1();
            unsub2();
        });

        const unsub1 = this.#subscriptions.subscribe(id.toString(), (msg) => {
            channel.submit(msg);
        });
        const unsub2 = this.#publisher.subscribe("disconnected", () => {
            channel.cancel();
        });

        return channel;
    }

    async #connect(): Promise<void> {
        try {
            const { Channel, invoke } = await loadTauriCore();
            const onMessage = new Channel<TauriMessage>();

            onMessage.onmessage = (message) => {
                this.#handleMessage(message);
            };

            const id = await invoke<number>("surreal_bridge_connect", {
                onMessage,
            });

            if (this.#terminated) {
                await invoke("surreal_bridge_disconnect", { id });
                return;
            }

            this.#connectionId = id;
            this.#active = true;
            this.#publisher.publish("connected");
        } catch (error) {
            this.#active = false;
            this.#connectionId = undefined;
            this.#publisher.publish(
                "error",
                error instanceof Error ? error : new Error(String(error)),
            );
            this.#publisher.publish("disconnected");
        }
    }

    async #sendRaw(request: object): Promise<void> {
        if (this.#connectionId === undefined) {
            throw new ConnectionUnavailableError();
        }

        const { invoke } = await loadTauriCore();
        const payload = new Uint8Array(this._context.codecs.cbor.encode(request));

        await invoke("surreal_bridge_send", {
            id: this.#connectionId,
            data: Array.from(payload),
        });
    }

    #handleMessage(message: TauriMessage): void {
        if (message.type !== "Binary") {
            this.#publisher.publish(
                "error",
                new UnexpectedServerResponseError(`unexpected message type: ${message.type}`),
            );
            return;
        }

        try {
            const buffer = new Uint8Array(message.data);
            const decoded = this._context.codecs.cbor.decode<Response>(buffer);

            if (
                typeof decoded === "object" &&
                decoded != null &&
                Object.getPrototypeOf(decoded) === Object.prototype
            ) {
                this.#handleRpcResponse(decoded);
            } else {
                throw new UnexpectedServerResponseError(decoded);
            }
        } catch (error) {
            this.#publisher.publish(
                "error",
                error instanceof Error ? error : new Error(String(error)),
            );
        }
    }

    #handleRpcResponse({ id, ...res }: Response): void {
        if (typeof id === "string") {
            try {
                const response = res as RpcResponse<unknown>;
                const { resolve, reject } = this.#calls.get(id) ?? {};

                if (response.error) {
                    reject?.(parseRpcError(response.error));
                } else {
                    resolve?.(response.result);
                }
            } finally {
                this.#calls.delete(id);
            }
            return;
        }

        if (isLiveMessage(res.result)) {
            this.#subscriptions.publish(res.result.id.toString(), {
                queryId: res.result.id,
                action: res.result.action,
                recordId: res.result.record,
                value: res.result.result,
            });
            return;
        }

        this.#publisher.publish("error", new UnexpectedServerResponseError(res));
    }
}

function isLiveMessage(v: unknown): v is LivePayload {
    if (typeof v !== "object") return false;
    if (v === null) return false;
    if (!("id" in v && "action" in v && "result" in v && "record" in v)) return false;

    if (!(v.id instanceof Uuid)) return false;
    if (!LIVE_ACTIONS.includes(v.action as LiveAction)) return false;
    if (typeof v.result !== "object") return false;
    if (v.result === null) return false;
    if (!(v.record instanceof RecordId)) return false;

    return true;
}
