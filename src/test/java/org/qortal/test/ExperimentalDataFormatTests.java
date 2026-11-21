package org.qortal.test;

import org.junit.Test;
import org.qortal.settings.Settings;
import org.qortal.test.common.Common;
import static org.junit.Assert.*;

public class ExperimentalDataFormatTests {
    @Test public void configuredAutoUpdateCannotReplaceExperimentalDatabaseBuild() throws Exception {
        Common.useDefaultSettings();
        var configured = Settings.class.getDeclaredField("autoUpdateEnabled");
        configured.setAccessible(true);
        Settings settings = Settings.getInstance();
        boolean original = configured.getBoolean(settings);
        try {
            configured.setBoolean(settings, true);
            assertFalse(settings.isAutoUpdateEnabled());
        } finally { configured.setBoolean(settings, original); }
    }
}
