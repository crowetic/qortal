package org.qortal.network;

import org.reflections.Reflections;
import java.lang.reflect.Constructor;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

public final class PeerFactory {

    private static final ConcurrentMap<String, Constructor<?>> REGISTRY = new ConcurrentHashMap<>();

    /* static init block – scan the classpath once */
    static {
        Reflections reflections = new Reflections("org.qortal.network");
        Set<Class<? extends Peer>> subTypes = reflections.getSubTypesOf(Peer.class);
        for (Class<?> impl : subTypes) {
            for (Constructor<?> con : impl.getDeclaredConstructors()) {
                if (con.isAnnotationPresent(PeerCtor.class)) {
                    String key = con.getAnnotation(PeerCtor.class).value();
                    con.setAccessible(true);
                    REGISTRY.put(key, con);
                }
            }
        }
    }

    /** Creates the instance chosen by 'key'. Var-args are forwarded literally. */
    public static Peer create(String key, Object... args)
            throws ReflectiveOperationException {
        Constructor<?> c = REGISTRY.get(key);
        if (c == null) throw new IllegalArgumentException("No @PeerCtor '" + key + "'");
        return (Peer) c.newInstance(args);
    }
}

