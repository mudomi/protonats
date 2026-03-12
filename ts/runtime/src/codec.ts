import { toBinary, fromBinary } from "@bufbuild/protobuf";
import type { MessageShape, DescMessage } from "@bufbuild/protobuf";

/** Encode a protobuf message to binary. */
export function encode<T extends DescMessage>(schema: T, msg: MessageShape<T>): Uint8Array {
  return toBinary(schema, msg);
}

/** Decode binary data into a protobuf message. */
export function decode<T extends DescMessage>(schema: T, data: Uint8Array): MessageShape<T> {
  return fromBinary(schema, data);
}
