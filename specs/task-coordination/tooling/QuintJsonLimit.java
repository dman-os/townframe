import java.lang.instrument.Instrumentation;

/** Scoped Apalache RPC transport adjustment; never changes model/checker data. */
public final class QuintJsonLimit {
    public static void premain(String argument, Instrumentation instrumentation) {
        final int limit = 128 * 1024 * 1024;
        try {
            Class<?> constraints = Class.forName("com.fasterxml.jackson.core.StreamReadConstraints");
            Object builder = constraints.getMethod("builder").invoke(null);
            builder.getClass().getMethod("maxStringLength", int.class).invoke(builder, limit);
            Object configured = builder.getClass().getMethod("build").invoke(builder);
            constraints.getMethod("overrideDefaultStreamReadConstraints", constraints).invoke(null, configured);
            System.err.println("QuintJsonLimit: Jackson maxStringLength=" + limit + "; all other constraints unchanged");
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Cannot install scoped Apalache JSON string limit", failure);
        }
    }
}
