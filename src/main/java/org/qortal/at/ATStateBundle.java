package org.qortal.at;

import org.qortal.crypto.Crypto;
import org.qortal.repository.DataException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable, checksummed collection of the complete AT payloads produced by one block.
 * One database row is written for the whole collection, avoiding one indexed row per AT.
 */
public final class ATStateBundle {
	private static final int MAGIC = 0x51415450; // QATP
	private static final short FORMAT_VERSION = 1;
	private static final int HASH_LENGTH = 32;
	private static final int MAX_ENTRY_COUNT = 100_000;
	private static final int MAX_PAYLOAD_LENGTH = 1024 * 1024;
	// Fits the inline VARBINARY(16M) repository column including the envelope.
	private static final int MAX_BODY_LENGTH = 15 * 1024 * 1024;

	private ATStateBundle() {
	}

	public static byte[] serialize(Map<String, Entry> entries) throws DataException {
		if (entries == null || entries.size() > MAX_ENTRY_COUNT)
			throw new DataException("Invalid AT state bundle entry count");

		try {
			ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(Math.max(4096, entries.size() * 256));
			DataOutputStream body = new DataOutputStream(bodyBytes);
			body.writeInt(entries.size());
			for (Map.Entry<String, Entry> item : entries.entrySet()) {
				String address = item.getKey();
				Entry entry = item.getValue();
				if (address == null || address.isEmpty() || address.length() > 64 || entry == null
						|| entry.hash == null || entry.hash.length != HASH_LENGTH || entry.data == null
						|| entry.data.length > MAX_PAYLOAD_LENGTH || !Arrays.equals(entry.hash, Crypto.digest(entry.data)))
					throw new DataException("Invalid AT state bundle entry");
				body.writeUTF(address);
				body.write(entry.hash);
				body.writeInt(entry.data.length);
				body.write(entry.data);
			}
			body.flush();
			byte[] rawBody = bodyBytes.toByteArray();
			if (rawBody.length > MAX_BODY_LENGTH)
				throw new DataException("AT state bundle exceeds maximum size");

			ByteArrayOutputStream envelopeBytes = new ByteArrayOutputStream(rawBody.length + 42);
			DataOutputStream envelope = new DataOutputStream(envelopeBytes);
			envelope.writeInt(MAGIC);
			envelope.writeShort(FORMAT_VERSION);
			envelope.writeInt(rawBody.length);
			envelope.write(rawBody);
			envelope.write(Crypto.digest(rawBody));
			envelope.flush();
			return envelopeBytes.toByteArray();
		} catch (IOException e) {
			throw new DataException("Unable to serialize AT state bundle", e);
		}
	}

	public static Map<String, Entry> deserialize(byte[] bytes) throws DataException {
		if (bytes == null || bytes.length < 4 + 2 + 4 + HASH_LENGTH)
			throw new DataException("AT state bundle is truncated");

		try {
			DataInputStream envelope = new DataInputStream(new ByteArrayInputStream(bytes));
			if (envelope.readInt() != MAGIC)
				throw new DataException("Unknown AT state bundle format");
			short version = envelope.readShort();
			if (version != FORMAT_VERSION)
				throw new DataException("Unsupported AT state bundle version: " + version);
			int bodyLength = envelope.readInt();
			if (bodyLength < 4 || bodyLength > MAX_BODY_LENGTH || bodyLength != envelope.available() - HASH_LENGTH)
				throw new DataException("Invalid AT state bundle length");
			byte[] rawBody = new byte[bodyLength];
			envelope.readFully(rawBody);
			byte[] checksum = new byte[HASH_LENGTH];
			envelope.readFully(checksum);
			if (envelope.available() != 0 || !Arrays.equals(checksum, Crypto.digest(rawBody)))
				throw new DataException("AT state bundle checksum mismatch");

			DataInputStream body = new DataInputStream(new ByteArrayInputStream(rawBody));
			int count = body.readInt();
			if (count < 0 || count > MAX_ENTRY_COUNT)
				throw new DataException("Invalid AT state bundle entry count");
			Map<String, Entry> entries = new LinkedHashMap<>(count);
			for (int i = 0; i < count; ++i) {
				String address = body.readUTF();
				if (address.isEmpty() || address.length() > 64)
					throw new DataException("Invalid AT state bundle address");
				byte[] hash = new byte[HASH_LENGTH];
				body.readFully(hash);
				int length = body.readInt();
				if (length < 0 || length > MAX_PAYLOAD_LENGTH || length > body.available())
					throw new DataException("Invalid AT state bundle payload length");
				byte[] data = new byte[length];
				body.readFully(data);
				if (!Arrays.equals(hash, Crypto.digest(data)) || entries.put(address, new Entry(hash, data)) != null)
					throw new DataException("Invalid or duplicate AT state bundle entry");
			}
			if (body.available() != 0)
				throw new DataException("AT state bundle has trailing data");
			return Collections.unmodifiableMap(entries);
		} catch (EOFException e) {
			throw new DataException("AT state bundle is truncated", e);
		} catch (IOException e) {
			throw new DataException("Unable to deserialize AT state bundle", e);
		}
	}

	public static final class Entry {
		public final byte[] hash;
		public final byte[] data;

		public Entry(byte[] hash, byte[] data) {
			this.hash = hash.clone();
			this.data = data.clone();
		}
	}
}
