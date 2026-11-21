package org.qortal.network.message;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.arbitrary.ArbitraryDataFile;
import org.qortal.arbitrary.QdnByteArrayPool;
import org.qortal.network.Network;
import org.qortal.repository.DataException;
import org.qortal.transform.Transformer;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class ArbitraryDataFileMessage extends Message {

	private static final Logger LOGGER = LogManager.getLogger(ArbitraryDataFileMessage.class);

	private static final int MAGIC_LENGTH = 4;
	private static final int TYPE_LENGTH = 4;
	private static final int HAS_ID_LENGTH = 1;
	private static final int ID_LENGTH = 4;
	private static final int DATA_SIZE_LENGTH = 4;
	private static final int CHECKSUM_LENGTH = 4;
	private static final int MAX_DATA_SIZE = 10 * 1024 * 1024; // 10MB

	private byte[] signature;
	private ArbitraryDataFile arbitraryDataFile;
	
	// Shallow prefetch: async disk read when message enters sendQueue
	// Reduces blocking in writeChannel() by having data ready when needed
	private volatile CompletableFuture<byte[]> prefetchFuture;

	/**
	 * Constructor for outgoing messages.
	 * Data is NOT loaded or serialized here - deferred until toBytes() is called.
	 * This prevents memory issues when queuing many large chunk messages.
	 */
	public ArbitraryDataFileMessage(byte[] signature, ArbitraryDataFile arbitraryDataFile) {
		super(MessageType.ARBITRARY_DATA_FILE);
		this.signature = signature;
		this.arbitraryDataFile = arbitraryDataFile;
		// Set empty dataBytes to pass checkValidOutgoing() - actual serialization happens in toBytes()
		this.dataBytes = EMPTY_DATA_BYTES;
		this.checksumBytes = null;
	}

	private ArbitraryDataFileMessage(int id, byte[] signature, ArbitraryDataFile arbitraryDataFile) {
		super(id, MessageType.ARBITRARY_DATA_FILE);

		this.signature = signature;
		this.arbitraryDataFile = arbitraryDataFile;
	}

	public byte[] getSignature() {
		return this.signature;
	}

	public ArbitraryDataFile getArbitraryDataFile() {
		return this.arbitraryDataFile;
	}

	/**
	 * Override checkValidOutgoing() to validate lazy-loaded message.
	 * We check for ArbitraryDataFile instead of dataBytes since serialization is deferred.
	 */
	@Override
	public void checkValidOutgoing() throws MessageException {
		if (this.arbitraryDataFile == null)
			throw new MessageException("Missing arbitrary data file");
		// dataBytes is set to EMPTY_DATA_BYTES for lazy loading, so base class check will pass
		super.checkValidOutgoing();
	}

	/**
	 * Starts async prefetch of chunk data from disk.
	 * Called when message enters sendQueue to reduce blocking in writeChannel().
	 * 
	 * @return true if prefetch was started, false if already in progress or complete
	 */
	public synchronized boolean startPrefetch() {
		if (this.arbitraryDataFile == null || this.arbitraryDataFile.hasInMemoryContent()
				|| this.prefetchFuture != null)
			return false;

		this.prefetchFuture = CompletableFuture.supplyAsync(() -> {
			try {
				return this.arbitraryDataFile.getBytes();
			} catch (Exception e) {
				LOGGER.warn("Prefetch failed for message {}: {}", getId(), e.getMessage());
				return null;
			}
		});
		return true;
	}
	
	/**
	 * Returns true if prefetch is complete and data is ready.
	 */
	public boolean isPrefetchReady() {
		CompletableFuture<byte[]> future = this.prefetchFuture;
		return future != null && future.isDone() && !future.isCompletedExceptionally()
				&& future.getNow(null) != null;
	}

	/**
	 * Override toBytes() to serialize on-demand, loading data only when actually sending.
	 * Uses prefetched data if available to avoid blocking disk I/O.
	 * This prevents memory issues when queuing many large chunk messages.
	 */
	@Override
	public byte[] toBytes() throws MessageException {
		if (this.arbitraryDataFile == null) {
			throw new MessageException("Missing arbitrary data file");
		}

		// If an asynchronous disk read was started, wait for that same read instead of
		// issuing a second synchronous read when the write queue reaches this message.
		CompletableFuture<byte[]> future = this.prefetchFuture;
		byte[] data = null;
		if (future != null) {
			try {
				data = future.join();
			} catch (CompletionException e) {
				LOGGER.debug("Prefetch completion failed for message {}: {}", getId(), e.getMessage());
			}
		}
		this.prefetchFuture = null;
		if (data == null)
			data = this.arbitraryDataFile.getBytes();
		if (data == null)
			throw new MessageException("Unable to load arbitrary data file");
		
		// Clear fileContent after loading - we no longer need it in memory
		// The serialized message will be sent, and data can be reloaded from disk if needed
		arbitraryDataFile.clearFileContent();

		int dataLength = this.signature.length + Integer.BYTES + data.length;

		// Calculate full message length
		int messageLength = MAGIC_LENGTH + TYPE_LENGTH + HAS_ID_LENGTH;
		messageLength += this.hasId() ? ID_LENGTH : 0;
		messageLength += DATA_SIZE_LENGTH + CHECKSUM_LENGTH + dataLength;

		if (messageLength > MAX_DATA_SIZE)
			throw new MessageException(String.format("About to send message with length %d larger than allowed %d", messageLength, MAX_DATA_SIZE));

		// Allocate the wire message once. The previous ByteArrayOutputStream path copied
		// the chunk into a payload buffer, copied that buffer out, copied it into the full
		// message, and then copied the full message out again.
		byte[] messageBytes = new byte[messageLength];
		ByteBuffer output = ByteBuffer.wrap(messageBytes);
		output.put(Network.getInstance().getMessageMagic());
		output.putInt(this.type.value);
		output.put((byte) (this.hasId() ? 1 : 0));
		if (this.hasId())
			output.putInt(this.id);
		output.putInt(dataLength);

		int checksumOffset = output.position();
		output.position(checksumOffset + CHECKSUM_LENGTH);
		int dataOffset = output.position();
		output.put(this.signature);
		output.putInt(data.length);
		output.put(data);

		ByteBuffer payload = ByteBuffer.wrap(messageBytes, dataOffset, dataLength).slice();
		byte[] checksum = Message.generateChecksum(payload);
		System.arraycopy(checksum, 0, messageBytes, checksumOffset, CHECKSUM_LENGTH);
		return messageBytes;
	}

	public static Message fromByteBuffer(int id, ByteBuffer byteBuffer) throws MessageException {
		byte[] signature = new byte[Transformer.SIGNATURE_LENGTH];
		byteBuffer.get(signature);

		int dataLength = byteBuffer.getInt();
		if (dataLength < 0)
			throw new MessageException("Invalid arbitrary data file length: " + dataLength);

		if (byteBuffer.remaining() < dataLength)
			throw new BufferUnderflowException();

		byte[] data = QdnByteArrayPool.acquire(dataLength);
		byteBuffer.get(data, 0, dataLength);

		try {
			ArbitraryDataFile arbitraryDataFile = ArbitraryDataFile.fromReceivedBytes(
					data, dataLength, signature, () -> QdnByteArrayPool.release(data, dataLength));
			return new ArbitraryDataFileMessage(id, signature, arbitraryDataFile);
		} catch (DataException e) {
			QdnByteArrayPool.release(data, dataLength);
			LOGGER.info("Unable to process received file: {}", e.getMessage());
			throw new MessageException("Unable to process received file: " + e.getMessage(), e);
		}
	}

}
