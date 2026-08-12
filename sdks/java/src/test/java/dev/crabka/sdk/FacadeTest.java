package dev.crabka.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

final class FacadeTest {
    @Test
    void clientBuilderCreatesModuleAccessors() {
        CrabkaClient client = CrabkaClient.builder().endpoint("mock://gateway").bearerToken("token").build();

        assertEquals("token", client.auth().bearerToken());
        assertNotNull(client.messaging());
        assertNotNull(client.queues());
        assertNotNull(client.database());
        assertNotNull(client.blob());
    }

    @Test
    void stubModulesThrowGatedUnimplemented() {
        CrabkaClient client = CrabkaClient.builder().endpoint("http://localhost:1").build();

        CompletionException error = assertThrows(CompletionException.class,
                () -> client.queues().ack("message-id").join());
        UnimplementedException unimplemented = assertInstanceOf(UnimplementedException.class, error.getCause());

        assertEquals("queues", unimplemented.module());
        assertEquals("gateway-sharegroup-rpc", unimplemented.gatedOn());
    }

    @Test
    void authSignInIsUnauthenticated() {
        CrabkaClient client = CrabkaClient.builder().endpoint("mock://gateway").build();

        CompletionException error = assertThrows(CompletionException.class,
                () -> client.auth().signIn("u", "p").join());

        assertInstanceOf(UnauthenticatedException.class, error.getCause());
    }

    @Test
    void publishMapsMockErrors() {
        CrabkaClient client = CrabkaClient.builder().endpoint("mock://gateway").build();

        CompletionException error = assertThrows(CompletionException.class,
                () -> client.messaging().publish(Record.of("", new byte[] {1})).join());

        assertInstanceOf(InvalidArgumentException.class, error.getCause());
    }

    @Test
    void unreachableEndpointMapsToTransport() {
        CrabkaClient client = CrabkaClient.builder().endpoint("unreachable://gateway").build();

        CompletionException error = assertThrows(CompletionException.class,
                () -> client.messaging().publish(Record.of("t", new byte[] {1})).join());

        assertInstanceOf(TransportException.class, error.getCause());
    }
}
