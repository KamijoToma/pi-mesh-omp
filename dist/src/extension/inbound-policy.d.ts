import type { MeshFrame } from "../protocol/envelope.js";
import type { InboundBroadcastPolicy } from "../shared/config.js";
export declare function classifyInbound(frame: MeshFrame, selfAlias: string, policy?: InboundBroadcastPolicy, matchedReply?: boolean): InboundBroadcastPolicy;
