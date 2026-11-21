package org.qortal.test.arbitrary;

import org.junit.Test;
import org.qortal.arbitrary.QdnPathResolver;
import org.qortal.utils.Base58;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class QdnPathResolverTests {

    @Test
    public void cachedEncodingMatchesBase58AndOwnsItsKey() {
        byte[] value = new byte[64];
        for (int i = 0; i < value.length; ++i)
            value[i] = (byte) (i * 7 + 3);

        byte[] original = Arrays.copyOf(value, value.length);
        String expected = Base58.encode(original);
        assertEquals(expected, QdnPathResolver.encode(value));

        Arrays.fill(value, (byte) 0);
        assertEquals(expected, QdnPathResolver.encode(original));
        assertEquals(Base58.encode(value), QdnPathResolver.encode(value));
    }
}
