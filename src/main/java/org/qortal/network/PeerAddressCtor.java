package org.qortal.network;

import java.lang.annotation.*;

/** Tag a constructor with this so the factory can find it by key. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.CONSTRUCTOR)
public @interface PeerAddressCtor {
    String value();            // the "key" you pass in at runtime
}

