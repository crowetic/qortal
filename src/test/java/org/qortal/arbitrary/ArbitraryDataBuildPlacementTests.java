package org.qortal.arbitrary;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFileAttributeView;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class ArbitraryDataBuildPlacementTests {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void sameFilesystemPlacementRenamesFilesInsteadOfCopying() throws Exception {
        Path source = this.temporary.newFolder("source").toPath();
        Path file = Files.writeString(source.resolve("payload"), "complete");
        Object originalKey = Files.readAttributes(file, BasicFileAttributes.class).fileKey();
        Path destination = this.temporary.getRoot().toPath().resolve("destination");
        ArbitraryDataReader.moveBuiltDirectory(source, destination);
        assertFalse(Files.exists(source));
        assertEquals("complete", Files.readString(destination.resolve("payload")));
        if (originalKey != null)
            assertEquals(originalKey, Files.readAttributes(destination.resolve("payload"), BasicFileAttributes.class).fileKey());
    }

    @Test
    public void existingDestinationIsPreserved() throws Exception {
        Path source = this.temporary.newFolder("source").toPath();
        Path destination = this.temporary.newFolder("destination").toPath();
        Files.writeString(source.resolve("payload"), "source");
        Files.writeString(destination.resolve("payload"), "existing");
        try {
            ArbitraryDataReader.moveBuiltDirectory(source, destination);
            fail("Expected existing destination to be rejected");
        } catch (IOException expected) {
            assertEquals("source", Files.readString(source.resolve("payload")));
            assertEquals("existing", Files.readString(destination.resolve("payload")));
        }
    }

    @Test
    public void crossFilesystemFallbackCopiesBeforeRemovingSource() throws Exception {
        Path source = this.temporary.newFolder("source").toPath();
        Files.createDirectories(source.resolve("nested"));
        Files.writeString(source.resolve("nested/payload"), "complete");
        Path otherRoot = crossFilesystemRoot();
        try {
            Path destination = otherRoot.resolve("destination");
            ArbitraryDataReader.moveBuiltDirectory(source, destination);
            assertFalse(Files.exists(source));
            assertEquals("complete", Files.readString(destination.resolve("nested/payload")));
            try (Stream<Path> paths = Files.list(otherRoot)) {
                assertEquals(1L, paths.count());
            }
        } finally {
            FileUtils.deleteDirectory(otherRoot.toFile());
        }
    }

    @Test
    public void failedFallbackCopyLeavesSourceAndPublishesNothing() throws Exception {
        Path source = this.temporary.newFolder("source").toPath();
        Path file = Files.writeString(source.resolve("unreadable"), "source");
        assumeTrue(Files.getFileStore(file).supportsFileAttributeView(PosixFileAttributeView.class));
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file);
        Path otherRoot = crossFilesystemRoot();
        try {
            Files.setPosixFilePermissions(file, Collections.emptySet());
            assumeTrue(!Files.isReadable(file));
            Path destination = otherRoot.resolve("destination");
            try {
                ArbitraryDataReader.moveBuiltDirectory(source, destination);
                fail("Expected copy failure");
            } catch (IOException expected) {
                assertTrue(Files.exists(file));
                assertFalse(Files.exists(destination));
                try (Stream<Path> paths = Files.list(otherRoot)) {
                    assertEquals(0L, paths.count());
                }
            }
        } finally {
            Files.setPosixFilePermissions(file, permissions);
            FileUtils.deleteDirectory(otherRoot.toFile());
        }
    }

    private Path crossFilesystemRoot() throws IOException {
        Path other = Paths.get("/dev/shm");
        assumeTrue(Files.isDirectory(other) && Files.isWritable(other));
        assumeTrue(!Files.getFileStore(other).equals(Files.getFileStore(this.temporary.getRoot().toPath())));
        return Files.createTempDirectory(other, "qdn-placement-test-");
    }
}
