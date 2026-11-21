package org.qortal.repository;

import com.google.common.primitives.Ints;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.data.block.BlockArchiveData;
import org.qortal.settings.Settings;
import org.qortal.transform.TransformationException;
import org.qortal.transform.block.BlockTransformation;
import org.qortal.transform.block.BlockTransformer;
import org.qortal.utils.Triple;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.qortal.transform.Transformer.INT_LENGTH;

public class BlockArchiveReader {

    private static BlockArchiveReader instance;
    private volatile List<ArchiveFileInfo> fileListCache;

    private static final Logger LOGGER = LogManager.getLogger(BlockArchiveReader.class);

    public BlockArchiveReader() {

    }

    private static final class ArchiveFileInfo {
        private final String filename;
        private final int startHeight;
        private final int endHeight;

        private ArchiveFileInfo(String filename, int startHeight, int endHeight) {
            this.filename = filename;
            this.startHeight = startHeight;
            this.endHeight = endHeight;
        }
    }

    /** Immutable serialized block read directly from an archive file. */
    public static final class SerializedBlock {
        private final byte[] bytes;
        private final int serializationVersion;
        private final int height;

        private SerializedBlock(byte[] bytes, int serializationVersion, int height) {
            this.bytes = bytes;
            this.serializationVersion = serializationVersion;
            this.height = height;
        }

        public byte[] getBytes() {
            return this.bytes;
        }

        public int getSerializationVersion() {
            return this.serializationVersion;
        }

        public int getHeight() {
            return this.height;
        }

        public int getWireSize() {
            return INT_LENGTH + this.bytes.length;
        }
    }

    public static synchronized BlockArchiveReader getInstance() {
        if (instance == null) {
            instance = new BlockArchiveReader();
        }

        return instance;
    }

    private synchronized void fetchFileList() {
        if (this.fileListCache != null)
            return;

        Path archivePath = Paths.get(Settings.getInstance().getRepositoryPath(), "archive").toAbsolutePath();
        File archiveDirFile = archivePath.toFile();
        String[] files = archiveDirFile.list();
        List<ArchiveFileInfo> fileInfos = new ArrayList<>();

        if (files != null) {
            for (String file : files) {
                Path filePath = Paths.get(file);
                String filename = filePath.getFileName().toString();

                // Parse the filename
                if (filename == null || !filename.endsWith(".dat") || !filename.contains("-")) {
                    // Not a usable file
                    continue;
                }
                try {
                    // Remove the extension and split into two parts
                    String[] parts = filename.substring(0, filename.lastIndexOf('.')).split("-");
                    if (parts.length != 2)
                        continue;

                    int startHeight = Integer.parseInt(parts[0]);
                    int endHeight = Integer.parseInt(parts[1]);
                    if (startHeight <= 0 || endHeight < startHeight)
                        continue;

                    fileInfos.add(new ArchiveFileInfo(filename, startHeight, endHeight));
                } catch (NumberFormatException e) {
                    LOGGER.debug("Ignoring unrecognized block archive filename {}", filename);
                }
            }
        }

        fileInfos.sort(Comparator.comparingInt((ArchiveFileInfo info) -> info.startHeight)
                .thenComparingInt(info -> info.endHeight)
                .thenComparing(info -> info.filename));
        this.fileListCache = List.copyOf(fileInfos);
    }

    public Integer fetchSerializationVersionForHeight(int height) {
        if (this.fileListCache == null) {
            this.fetchFileList();
        }

        Triple<byte[], Integer, Integer> serializedBlock = this.fetchSerializedBlockBytesForHeight(height);
        if (serializedBlock == null) {
            return null;
        }
        Integer serializationVersion = serializedBlock.getB();
        return serializationVersion;
    }

    public BlockTransformation fetchBlockAtHeight(int height) {
        if (this.fileListCache == null) {
            this.fetchFileList();
        }

        Triple<byte[], Integer, Integer> serializedBlock = this.fetchSerializedBlockBytesForHeight(height);
        if (serializedBlock == null) {
            return null;
        }
        byte[] serializedBytes = serializedBlock.getA();
        Integer serializationVersion = serializedBlock.getB();
        if (serializedBytes == null || serializationVersion == null) {
            return null;
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(serializedBytes);
        BlockTransformation blockInfo = null;
        try {
            switch (serializationVersion) {
                case 1:
                    blockInfo = BlockTransformer.fromByteBuffer(byteBuffer);
                    break;

                case 2:
                    blockInfo = BlockTransformer.fromByteBufferV2(byteBuffer);
                    break;

                default:
                    // Invalid serialization version
                    return null;
            }

            if (blockInfo != null && blockInfo.getBlockData() != null) {
                // Block height is stored outside of the main serialized bytes, so it
                // won't be set automatically.
                blockInfo.getBlockData().setHeight(height);
            }
        } catch (TransformationException e) {
            return null;
        }
        return blockInfo;
    }

    public BlockTransformation fetchBlockWithSignature(byte[] signature, Repository repository) {

        if (this.fileListCache == null) {
            this.fetchFileList();
        }

        Integer height = this.fetchHeightForSignature(signature, repository);
        if (height != null) {
            return this.fetchBlockAtHeight(height);
        }
        return null;
    }

    public List<BlockTransformation> fetchBlocksFromRange(int startHeight, int endHeight) {

        List<BlockTransformation> blockInfoList = new ArrayList<>();

        for (int height = startHeight; height <= endHeight; height++) {
            BlockTransformation blockInfo = this.fetchBlockAtHeight(height);
            if (blockInfo == null) {
                return blockInfoList;
            }
            blockInfoList.add(blockInfo);
        }
        return blockInfoList;
    }

    public Integer fetchHeightForSignature(byte[] signature, Repository repository) {
        // Lookup the height for the requested signature
        try {
            BlockArchiveData archivedBlock = repository.getBlockArchiveRepository().getBlockArchiveDataForSignature(signature);
            if (archivedBlock == null) {
                return null;
            }
            return archivedBlock.getHeight();

        } catch (DataException e) {
            return null;
        }
    }

    public int fetchHeightForTimestamp(long timestamp, Repository repository) {
        // Lookup the height for the requested signature
        try {
            return repository.getBlockArchiveRepository().getHeightFromTimestamp(timestamp);

        } catch (DataException e) {
            return 0;
        }
    }

    private ArchiveFileInfo getFileForHeight(int height) {
        if (this.fileListCache == null) {
            this.fetchFileList();
        }

        List<ArchiveFileInfo> files = this.fileListCache;
        int low = 0;
        int high = files.size() - 1;
        int candidate = -1;

        // Select the file with the greatest start height not above the requested
        // height. This makes old one-height boundary overlaps deterministic.
        while (low <= high) {
            int middle = (low + high) >>> 1;
            ArchiveFileInfo info = files.get(middle);
            if (info.startHeight <= height) {
                candidate = middle;
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }

        if (candidate < 0)
            return null;

        ArchiveFileInfo info = files.get(candidate);
        return height <= info.endHeight ? info : null;
    }

    private String getFilenameForHeight(int height) {
        ArchiveFileInfo info = this.getFileForHeight(height);
        return info == null ? null : info.filename;
    }

    public Triple<byte[], Integer, Integer> fetchSerializedBlockBytesForSignature(byte[] signature, boolean includeHeightPrefix, Repository repository) {
        if (this.fileListCache == null) {
            this.fetchFileList();
        }

        Integer height = this.fetchHeightForSignature(signature, repository);
        if (height != null) {
            Triple<byte[], Integer, Integer> serializedBlock = this.fetchSerializedBlockBytesForHeight(height);
            if (serializedBlock == null) {
                return null;
            }
            byte[] blockBytes = serializedBlock.getA();
            Integer version = serializedBlock.getB();
            if (blockBytes == null || version == null) {
                return null;
            }

            // When responding to a peer with a BLOCK message, we must prefix the byte array with the block height
            // This mimics the toData() method in BlockMessage and CachedBlockMessage
            if (includeHeightPrefix) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream(blockBytes.length + INT_LENGTH);
                try {
                    bytes.write(Ints.toByteArray(height));
                    bytes.write(blockBytes);
                    return new Triple<>(bytes.toByteArray(), version, height);

                } catch (IOException e) {
                    return null;
                }
            }
            return new Triple<>(blockBytes, version, height);
        }
        return null;
    }

    public Triple<byte[], Integer, Integer> fetchSerializedBlockBytesForHeight(int height) {
        String filename = this.getFilenameForHeight(height);
        if (filename == null) {
            // We don't have this block in the archive
            // Invalidate the file list cache in case it is out of date
            this.invalidateFileListCache();
            return null;
        }

        Path filePath = Paths.get(Settings.getInstance().getRepositoryPath(), "archive", filename).toAbsolutePath();
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(filePath.toString(), "r");
            // Get info about this file (the "fixed length header")
            final int version = file.readInt(); // Do not remove or comment out, as it is moving the file pointer
            final int startHeight = file.readInt(); // Do not remove or comment out, as it is moving the file pointer
            final int endHeight = file.readInt(); // Do not remove or comment out, as it is moving the file pointer
            file.readInt(); // Block count (unused) // Do not remove or comment out, as it is moving the file pointer
            final int variableHeaderLength = file.readInt(); // Do not remove or comment out, as it is moving the file pointer
            final int fixedHeaderLength = (int)file.getFilePointer();
            // End of fixed length header

            // Make sure the version is one we recognize
            if (version != 1 && version != 2) {
                LOGGER.info("Error: unknown version in file {}: {}", filename, version);
                return null;
            }

            // Verify that the block is within the reported range
            if (height < startHeight || height > endHeight) {
                LOGGER.info("Error: requested height {} but the range of file {} is {}-{}",
                        height, filename, startHeight, endHeight);
                return null;
            }

            // Seek to the location of the block index in the variable length header
            final int locationOfBlockIndexInVariableHeaderSegment = (height - startHeight) * INT_LENGTH;
            file.seek(fixedHeaderLength + locationOfBlockIndexInVariableHeaderSegment);

            // Read the value to obtain the index of this block in the data segment
            int locationOfBlockInDataSegment = file.readInt();

            // Now seek to the block data itself
            int dataSegmentStartIndex = fixedHeaderLength + variableHeaderLength + INT_LENGTH; // Confirmed correct
            file.seek(dataSegmentStartIndex + locationOfBlockInDataSegment);

            // Read the block metadata
            int blockHeight = file.readInt(); // Do not remove or comment out, as it is moving the file pointer
            int blockLength = file.readInt(); // Do not remove or comment out, as it is moving the file pointer

            // Ensure the block height matches the one requested
            if (blockHeight != height) {
                LOGGER.info("Error: height {} does not match requested: {}", blockHeight, height);
                return null;
            }

            // Now retrieve the block's serialized bytes
            byte[] blockBytes = new byte[blockLength];
            file.readFully(blockBytes);

            return new Triple<>(blockBytes, version, height);

        } catch (FileNotFoundException e) {
            LOGGER.info("File {} not found: {}", filename, e.getMessage());
            return null;
        } catch (IOException e) {
            LOGGER.info("Unable to read block {} from archive: {}", height, e.getMessage());
            return null;
        }
        finally {
            // Close the file
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    // Failed to close, but no need to handle this
                }
            }
        }
    }

    /**
     * Read a bounded, consecutive range without reopening an archive file for every block.
     * Returned blocks retain their archive serialization version so callers can use V2 bytes
     * directly and convert legacy V1 bytes only when necessary.
     */
    public List<SerializedBlock> fetchSerializedBlocksFromHeight(int startHeight, int maximumBlockCount,
                                                                  int maximumWireBytes) {
        if (startHeight <= 0 || maximumBlockCount <= 0 || maximumWireBytes <= INT_LENGTH)
            return Collections.emptyList();

        List<SerializedBlock> blocks = new ArrayList<>(maximumBlockCount);
        int nextHeight = startHeight;
        int wireBytes = INT_LENGTH; // BLOCKS message count prefix
        boolean refreshedFileList = false;

        while (blocks.size() < maximumBlockCount) {
            ArchiveFileInfo fileInfo = this.getFileForHeight(nextHeight);
            if (fileInfo == null && !refreshedFileList) {
                this.invalidateFileListCache();
                fileInfo = this.getFileForHeight(nextHeight);
                refreshedFileList = true;
            }
            if (fileInfo == null)
                break;

            Path filePath = Paths.get(Settings.getInstance().getRepositoryPath(), "archive", fileInfo.filename).toAbsolutePath();
            try (RandomAccessFile file = new RandomAccessFile(filePath.toString(), "r")) {
                int version = file.readInt();
                int fileStartHeight = file.readInt();
                int fileEndHeight = file.readInt();
                int blockCount = file.readInt();
                int variableHeaderLength = file.readInt();
                int fixedHeaderLength = (int) file.getFilePointer();

                if ((version != 1 && version != 2)
                        || fileStartHeight != fileInfo.startHeight
                        || fileEndHeight != fileInfo.endHeight
                        || blockCount <= 0
                        || variableHeaderLength < 0
                        || (long) variableHeaderLength < (long) blockCount * INT_LENGTH) {
                    LOGGER.warn("Invalid block archive header in {}", fileInfo.filename);
                    break;
                }

                long dataSegmentStart = (long) fixedHeaderLength + variableHeaderLength + INT_LENGTH;
                long fileLength = file.length();
                if (dataSegmentStart > fileLength) {
                    LOGGER.warn("Invalid block archive data offset in {}", fileInfo.filename);
                    break;
                }

                int lastHeightInFile = Math.min(fileInfo.endHeight, fileEndHeight);
                while (nextHeight <= lastHeightInFile && blocks.size() < maximumBlockCount) {
                    int blockIndex = nextHeight - fileStartHeight;
                    if (blockIndex < 0 || blockIndex >= blockCount) {
                        LOGGER.warn("Invalid block archive index for height {} in {}", nextHeight, fileInfo.filename);
                        return blocks;
                    }

                    long indexPosition = (long) fixedHeaderLength
                            + (long) blockIndex * INT_LENGTH;
                    if (indexPosition < fixedHeaderLength || indexPosition + INT_LENGTH > dataSegmentStart) {
                        LOGGER.warn("Invalid block archive index for height {} in {}", nextHeight, fileInfo.filename);
                        return blocks;
                    }

                    file.seek(indexPosition);
                    int blockOffset = file.readInt();
                    long blockPosition = dataSegmentStart + blockOffset;
                    if (blockOffset < 0 || blockPosition < dataSegmentStart || blockPosition + 2L * INT_LENGTH > fileLength) {
                        LOGGER.warn("Invalid block archive offset for height {} in {}", nextHeight, fileInfo.filename);
                        return blocks;
                    }

                    file.seek(blockPosition);
                    int storedHeight = file.readInt();
                    int blockLength = file.readInt();
                    if (storedHeight != nextHeight || blockLength <= 0
                            || blockPosition + 2L * INT_LENGTH + blockLength > fileLength) {
                        LOGGER.warn("Invalid block archive record for height {} in {}", nextHeight, fileInfo.filename);
                        return blocks;
                    }

                    long nextWireBytes = (long) INT_LENGTH + blockLength;
                    if ((long) wireBytes + nextWireBytes > maximumWireBytes)
                        return blocks;

                    byte[] blockBytes = new byte[blockLength];
                    file.readFully(blockBytes);
                    blocks.add(new SerializedBlock(blockBytes, version, nextHeight));
                    wireBytes += (int) nextWireBytes;
                    ++nextHeight;
                }
            } catch (FileNotFoundException e) {
                LOGGER.info("Archive file {} disappeared while serving blocks", fileInfo.filename);
                this.invalidateFileListCache();
                break;
            } catch (IOException e) {
                LOGGER.warn("Unable to read archive range at height {} from {}: {}",
                        nextHeight, fileInfo.filename, e.getMessage());
                break;
            }
        }

        return blocks;
    }

    public int getHeightOfLastArchivedBlock() {
        if (this.fileListCache == null) {
            this.fetchFileList();
        }

        List<ArchiveFileInfo> files = this.fileListCache;
        return files.isEmpty() ? 0 : files.get(files.size() - 1).endHeight;
    }

    public synchronized void invalidateFileListCache() {
        this.fileListCache = null;
    }

}
