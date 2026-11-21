package org.qortal.network.message;

import com.google.common.primitives.Ints;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.block.Block;
import org.qortal.data.block.BlockData;
import org.qortal.network.Network;
import org.qortal.repository.BlockArchiveReader.SerializedBlock;
import org.qortal.transform.TransformationException;
import org.qortal.transform.Transformer;
import org.qortal.transform.block.BlockTransformation;
import org.qortal.transform.block.BlockTransformer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BlocksMessage extends Message {

    private static final Logger LOGGER = LogManager.getLogger(BlocksMessage.class);

    private List<Block> blocks;

    /** Leave room for message framing around the BLOCKS data payload. */
    public static int getMaximumPayloadSize() {
        return MAX_DATA_SIZE - 64;
    }

    public BlocksMessage(List<Block> blocks) {
        super(MessageType.BLOCKS);

        this.blocks = blocks;

        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();

            bytes.write(Ints.toByteArray(this.blocks.size()));

            for (Block block : this.blocks) {
                bytes.write(Ints.toByteArray(block.getBlockData().getHeight()));
                bytes.write(BlockTransformer.toBytesV2(block));
            }
            LOGGER.trace(String.format("Total length of %d blocks is %d bytes", this.blocks.size(), bytes.size()));

            this.dataBytes = bytes.toByteArray();
        } catch (IOException | TransformationException e) {
            this.dataBytes = null;
            this.checksumBytes = null;
            return;
        }

        if (this.dataBytes.length > 0)
            this.checksumBytes = Message.generateChecksum(this.dataBytes);
        else
            this.checksumBytes = null;
    }

    /**
     * Build an outgoing BLOCKS response from archive records. V2 records are copied
     * directly; legacy V1 records are converted only when encountered.
     */
    public BlocksMessage(byte[] parentSignature, List<SerializedBlock> serializedBlocks) throws TransformationException {
        super(MessageType.BLOCKS);

        if (parentSignature == null || parentSignature.length != BlockTransformer.BLOCK_SIGNATURE_LENGTH)
            throw new TransformationException("Invalid parent signature for archived blocks");

        ByteArrayOutputStream records = new ByteArrayOutputStream();
        byte[] expectedReference = parentSignature;
        Integer previousHeight = null;
        int count = 0;

        try {
            for (SerializedBlock serializedBlock : serializedBlocks) {
                if (serializedBlock == null)
                    throw new TransformationException("Null archived block");
                if (previousHeight != null && serializedBlock.getHeight() != previousHeight + 1)
                    throw new TransformationException("Non-consecutive archived block heights");

                byte[] v2Bytes;
                switch (serializedBlock.getSerializationVersion()) {
                    case 1: {
                        BlockTransformation transformation = BlockTransformer.fromByteBuffer(
                                ByteBuffer.wrap(serializedBlock.getBytes()));
                        transformation.getBlockData().setHeight(serializedBlock.getHeight());
                        Block block = new Block(null, transformation.getBlockData(), transformation.getTransactions(),
                                transformation.getAtStates());
                        v2Bytes = BlockTransformer.toBytesV2(block);
                        break;
                    }

                    case 2:
                        v2Bytes = serializedBlock.getBytes();
                        break;

                    default:
                        throw new TransformationException("Unsupported block archive serialization version");
                }

                byte[] signature = validateLinkAndExtractSignature(v2Bytes, expectedReference);
                int nextSize = records.size() + Ints.BYTES + v2Bytes.length;
                if (Ints.BYTES + nextSize > getMaximumPayloadSize())
                    break;

                records.write(Ints.toByteArray(serializedBlock.getHeight()));
                records.write(v2Bytes);
                expectedReference = signature;
                previousHeight = serializedBlock.getHeight();
                ++count;
            }

            ByteArrayOutputStream bytes = new ByteArrayOutputStream(Ints.BYTES + records.size());
            bytes.write(Ints.toByteArray(count));
            records.writeTo(bytes);
            this.dataBytes = bytes.toByteArray();
            this.checksumBytes = Message.generateChecksum(this.dataBytes);
        } catch (IOException e) {
            throw new AssertionError("IOException shouldn't occur with ByteArrayOutputStream", e);
        }
    }

    private static byte[] validateLinkAndExtractSignature(byte[] blockBytes, byte[] expectedReference)
            throws TransformationException {
        int minimumLength = Integer.BYTES + Long.BYTES + BlockTransformer.BLOCK_SIGNATURE_LENGTH
                + Transformer.PUBLIC_KEY_LENGTH + 2 * Transformer.SIGNATURE_LENGTH;
        if (blockBytes == null || blockBytes.length < minimumLength)
            throw new TransformationException("Archived block bytes are too short");

        ByteBuffer buffer = ByteBuffer.wrap(blockBytes);
        buffer.getInt(); // block version
        buffer.getLong(); // timestamp

        byte[] reference = new byte[BlockTransformer.BLOCK_SIGNATURE_LENGTH];
        buffer.get(reference);
        if (!java.util.Arrays.equals(reference, expectedReference))
            throw new TransformationException("Archived block does not reference its expected parent");

        buffer.position(buffer.position() + Transformer.PUBLIC_KEY_LENGTH);
        byte[] transactionsSignature = new byte[Transformer.SIGNATURE_LENGTH];
        buffer.get(transactionsSignature);
        byte[] minterSignature = new byte[Transformer.SIGNATURE_LENGTH];
        buffer.get(minterSignature);

        byte[] signature = new byte[BlockTransformer.BLOCK_SIGNATURE_LENGTH];
        System.arraycopy(minterSignature, 0, signature, 0, minterSignature.length);
        System.arraycopy(transactionsSignature, 0, signature, minterSignature.length, transactionsSignature.length);
        return signature;
    }

    private BlocksMessage(int id, List<Block> blocks) {
        super(id, MessageType.BLOCKS);

        this.blocks = blocks;
    }

    public List<Block> getBlocks() {
        return this.blocks;
    }

    public static Message fromByteBuffer(int id, ByteBuffer bytes) throws MessageException {

        int count = bytes.getInt();
        if (count < 0 || count > Network.MAX_BLOCKS_PER_REPLY)
            return null;

        List<Block> blocks = new ArrayList<>();

        for (int i = 0; i < count; ++i) {
            int height = bytes.getInt();

            try {
                BlockTransformation blockTransformation = BlockTransformer.fromByteBufferV2(bytes);
                BlockData blockData = blockTransformation.getBlockData();
                blockData.setHeight(height);

                // We are unable to obtain a valid Repository instance here, so set it to null and we will attach it later
                Block block = new Block(null, blockData, blockTransformation.getTransactions(), blockTransformation.getAtStatesHash());
                blocks.add(block);

            } catch (TransformationException e) {
                LOGGER.warn(String.format("Received garbled BLOCKS message: %s", e.getMessage()));
                return null;
            }

        }

        if (bytes.hasRemaining())
            return null;

        return new BlocksMessage(id, blocks);
    }

}
