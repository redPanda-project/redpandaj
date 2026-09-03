<!-- Redpanda wire registry - GENERATED FILE, do not edit by hand.
     Sources: im.redpanda.core.Command, im.redpanda.flaschenpost.FlaschenpostV2,
     src/main/proto/*.proto
     Regenerate: mvn -q compile && java -cp target/classes im.redpanda.core.WireRegistry
     Verified by: im.redpanda.core.WireRegistryTest -->

## Top-level commands (`im.redpanda.core.Command`)

First byte of every frame on a peer connection.

| Constant | Dec | Hex |
| --- | ---: | --- |
| `REQUEST_PUBLIC_KEY` | 1 | `0x01` |
| `SEND_PUBLIC_KEY` | 2 | `0x02` |
| `ACTIVATE_ENCRYPTION` | 3 | `0x03` |
| `PING` | 5 | `0x05` |
| `PONG` | 6 | `0x06` |
| `REQUEST_PEERLIST` | 7 | `0x07` |
| `SEND_PEERLIST` | 8 | `0x08` |
| `UPDATE_REQUEST_TIMESTAMP` | 9 | `0x09` |
| `UPDATE_ANSWER_TIMESTAMP` | 10 | `0x0A` |
| `UPDATE_REQUEST_CONTENT` | 11 | `0x0B` |
| `UPDATE_ANSWER_CONTENT` | 12 | `0x0C` |
| `ANDROID_UPDATE_REQUEST_TIMESTAMP` | 13 | `0x0D` |
| `ANDROID_UPDATE_ANSWER_TIMESTAMP` | 14 | `0x0E` |
| `ANDROID_UPDATE_REQUEST_CONTENT` | 15 | `0x0F` |
| `ANDROID_UPDATE_ANSWER_CONTENT` | 16 | `0x10` |
| `KADEMLIA_STORE` | 120 | `0x78` |
| `KADEMLIA_GET` | 121 | `0x79` |
| `KADEMLIA_GET_ANSWER` | 122 | `0x7A` |
| `JOB_ACK` | 130 | `0x82` |
| `FLASCHENPOST_PUT` | 141 | `0x8D` |
| `FLASCHENPOST_V2` | 142 | `0x8E` |
| `OUTBOUND_REGISTER_OH_REQ` | 150 | `0x96` |
| `OUTBOUND_REGISTER_OH_RES` | 151 | `0x97` |
| `OUTBOUND_FETCH_REQ` | 152 | `0x98` |
| `OUTBOUND_FETCH_RES` | 153 | `0x99` |
| `OUTBOUND_REVOKE_OH_REQ` | 154 | `0x9A` |
| `OUTBOUND_REVOKE_OH_RES` | 155 | `0x9B` |
| `OUTBOUND_ACK_FETCH_REQ` | 156 | `0x9C` |
| `OUTBOUND_ACK_FETCH_RES` | 157 | `0x9D` |
| `FLASCHENPOST_PUT_RES` | 158 | `0x9E` |
| `OUTBOUND_SUBSCRIBE_REQ` | 159 | `0x9F` |
| `OUTBOUND_SUBSCRIBE_RES` | 160 | `0xA0` |
| `OUTBOUND_NOTIFY` | 161 | `0xA1` |

## Garlic layer commands (`im.redpanda.flaschenpost.FlaschenpostV2`)

First byte of a decrypted garlic layer, inside a `FLASCHENPOST_V2` (142) packet.

| Constant | Dec | Hex |
| --- | ---: | --- |
| `CMD_FORWARD` | 1 | `0x01` |
| `CMD_DELIVER` | 2 | `0x02` |
| `CMD_DELIVER_TAGGED` | 3 | `0x03` |
| `CMD_DELIVER_ACKED` | 4 | `0x04` |
| `CMD_RECORD_STORE` | 5 | `0x05` |
| `CMD_RECORD_LOOKUP` | 6 | `0x06` |

## Protobuf definitions (`src/main/proto`)

| File | Kind | Name |
| --- | --- | --- |
| `commands.proto` | message | `KademliaIdProto` |
| `commands.proto` | message | `NodeIdProto` |
| `commands.proto` | message | `PeerInfoProto` |
| `commands.proto` | message | `SendPeerList` |
| `commands.proto` | message | `Ping` |
| `commands.proto` | message | `Pong` |
| `commands.proto` | message | `RequestPeerList` |
| `commands.proto` | message | `KademliaGet` |
| `commands.proto` | message | `KademliaGetAnswer` |
| `commands.proto` | message | `KademliaStore` |
| `commands.proto` | message | `JobAck` |
| `commands.proto` | message | `FlaschenpostPut` |
| `commands.proto` | message | `PandaMessage` |
| `outbound.proto` | enum | `Status` |
| `outbound.proto` | message | `RegisterOhRequest` |
| `outbound.proto` | message | `RegisterOhResponse` |
| `outbound.proto` | message | `FetchRequest` |
| `outbound.proto` | message | `MailItem` |
| `outbound.proto` | message | `FetchResponse` |
| `outbound.proto` | message | `AckFetchRequest` |
| `outbound.proto` | message | `AckFetchResponse` |
| `outbound.proto` | message | `FlaschenpostPutResponse` |
| `outbound.proto` | message | `OhNodeRecord` |
| `outbound.proto` | message | `RoutingAck` |
| `outbound.proto` | message | `RevokeOhRequest` |
| `outbound.proto` | message | `RevokeOhResponse` |
| `outbound.proto` | message | `SubscribeRequest` |
| `outbound.proto` | message | `SubscribeResponse` |
| `outbound.proto` | message | `Notify` |
