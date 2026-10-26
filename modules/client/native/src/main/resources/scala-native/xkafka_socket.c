#ifdef SCALANATIVE_LINK_RDKAFKA

/* The group connection's own connection to one broker, plain or over TLS,
 * set up from the same settings librdkafka is given and in the same way.
 *
 * TLS runs over memory buffers: OpenSSL encrypts into one and decrypts from
 * the other, and the bytes are moved with send and recv here. Every network
 * call is then bounded by the socket's timeouts, and a broker that closes the
 * connection cannot raise SIGPIPE.
 *
 * Failures are reported with librdkafka's codes, so they are classified as the
 * data-plane clients' are: settings that cannot be used as
 * RD_KAFKA_RESP_ERR_INVALID_CONFIG, a refused handshake as
 * RD_KAFKA_RESP_ERR__SSL, a lost connection as RD_KAFKA_RESP_ERR__TRANSPORT
 * and a timeout as RD_KAFKA_RESP_ERR__TIMED_OUT. */

/* Scala Native compiles C as strict C11, which leaves out the POSIX
 * declarations (getaddrinfo, strerror) this file needs; glibc and musl give
 * them back for this, and macOS shows them anyway. */
#define _DEFAULT_SOURCE

#include <librdkafka/rdkafka.h>

#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#ifdef MSG_NOSIGNAL
#define XKAFKA_SEND_FLAGS MSG_NOSIGNAL
#else
#define XKAFKA_SEND_FLAGS 0
#endif

typedef struct {
        int fd;
        SSL_CTX *context;
        SSL *ssl;
        BIO *incoming;
        BIO *outgoing;
} xkafka_socket_t;

static void xkafka_socket_fail(char *error, size_t error_size, int32_t *error_code, int32_t code, const char *what, const char *detail) {
        if (error_code != NULL)
                *error_code = code;
        if (error != NULL && error_size > 0)
                snprintf(error, error_size, "%s%s%s", what, detail[0] ? ": " : "", detail);
}

/* The OpenSSL failure at the front of its queue, which is then cleared. */
static void xkafka_socket_fail_ssl(char *error, size_t error_size, int32_t *error_code, int32_t code, const char *what) {
        char detail[256] = "";
        unsigned long reason = ERR_get_error();

        if (reason != 0)
                ERR_error_string_n(reason, detail, sizeof(detail));
        ERR_clear_error();
        xkafka_socket_fail(error, error_size, error_code, code, what, detail);
}

static void xkafka_socket_free(xkafka_socket_t *connection) {
        if (connection->ssl != NULL)
                SSL_free(connection->ssl); /* also frees both buffers */
        else {
                if (connection->incoming != NULL)
                        BIO_free(connection->incoming);
                if (connection->outgoing != NULL)
                        BIO_free(connection->outgoing);
        }
        if (connection->context != NULL)
                SSL_CTX_free(connection->context);
        if (connection->fd >= 0)
                close(connection->fd);
        free(connection);
}

static int xkafka_socket_password(char *buffer, int size, int writing, void *password) {
        size_t length;

        (void)writing;
        if (password == NULL)
                return 0;
        length = strlen((const char *)password);
        if (length > (size_t)size)
                return 0;
        memcpy(buffer, password, length);
        return (int)length;
}

/* The standard places librdkafka looks for the system's authorities when it
 * links OpenSSL statically, as the JavaScript backend's library does, or runs
 * on macOS, where OpenSSL's own default holds none. */
static int xkafka_socket_probe_authorities(SSL_CTX *context) {
#if defined(__APPLE__) || defined(XKAFKA_STATIC_OPENSSL)
        static const char *paths[] = {
            "/etc/pki/tls/certs/ca-bundle.crt",
            "/etc/ssl/certs/ca-bundle.crt",
            "/etc/pki/tls/certs/ca-bundle.trust.crt",
            "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
            "/etc/ssl/ca-bundle.pem",
            "/etc/pki/tls/cacert.pem",
            "/etc/ssl/cert.pem",
            "/etc/ssl/cacert.pem",
            "/etc/certs/ca-certificates.crt",
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/ssl/certs",
            "/usr/local/etc/ssl/cert.pem",
            "/usr/local/etc/ssl/cacert.pem",
            "/usr/local/etc/ssl/certs/cert.pem",
            "/usr/local/etc/ssl/certs/cacert.pem",
            "/usr/local/share/certs/ca-root-nss.crt",
            "/etc/openssl/certs/ca-certificates.crt",
#ifdef __APPLE__
            "/private/etc/ssl/cert.pem",
            "/private/etc/ssl/certs",
            "/usr/local/etc/openssl@1.1/cert.pem",
            "/usr/local/etc/openssl@1.0/cert.pem",
            "/usr/local/etc/openssl/certs",
            "/System/Library/OpenSSL",
#endif
            NULL,
        };
        size_t index;

        for (index = 0; paths[index] != NULL; index++) {
                struct stat status;
                int directory;

                if (stat(paths[index], &status) != 0)
                        continue;
                directory = S_ISDIR(status.st_mode);
                if (SSL_CTX_load_verify_locations(context, directory ? NULL : paths[index], directory ? paths[index] : NULL) == 1)
                        return 0;
                ERR_clear_error();
        }
#else
        (void)context;
#endif
        return -1;
}

static int xkafka_socket_authorities(SSL_CTX *context, const char *location, const char *pem, char *error, size_t error_size,
                                     int32_t *error_code) {
        if (pem != NULL) {
                X509_STORE *store = SSL_CTX_get_cert_store(context);
                BIO *bio = BIO_new_mem_buf(pem, -1);
                X509 *certificate;
                int added = 0;

                if (bio == NULL) {
                        xkafka_socket_fail_ssl(error, error_size, error_code, RD_KAFKA_RESP_ERR_INVALID_CONFIG, "could not read the authorities");
                        return -1;
                }
                while ((certificate = PEM_read_bio_X509(bio, NULL, NULL, NULL)) != NULL) {
                        int stored = X509_STORE_add_cert(store, certificate);

                        X509_free(certificate);
                        if (!stored) {
                                BIO_free(bio);
                                xkafka_socket_fail_ssl(error, error_size, error_code, RD_KAFKA_RESP_ERR_INVALID_CONFIG,
                                                       "could not add an authority");
                                return -1;
                        }
                        added++;
                }
                BIO_free(bio);
                ERR_clear_error();
                if (added == 0) {
                        xkafka_socket_fail(error, error_size, error_code, RD_KAFKA_RESP_ERR_INVALID_CONFIG,
                                           "the authorities hold no PEM certificate", "");
                        return -1;
                }
                return 0;
        }
        if (location != NULL) {
                struct stat status;
                int directory = stat(location, &status) == 0 && S_ISDIR(status.st_mode);

                if (SSL_CTX_load_verify_locations(context, directory ? NULL : location, directory ? location : NULL) != 1) {
                        xkafka_socket_fail_ssl(error, error_size, error_code, RD_KAFKA_RESP_ERR_INVALID_CONFIG,
                                               "could not read the authorities");
                        return -1;
                }
                return 0;
        }
        if (xkafka_socket_probe_authorities(context) != 0 && SSL_CTX_set_default_verify_paths(context) != 1)
                ERR_clear_error();
        return 0;
}

static int xkafka_socket_certificate(SSL_CTX *context, const char *certificate_location, const char *key_location,
                                     const char *certificate_pem, const char *key_pem, const char *key_password, char *error,
                                     size_t error_size, int32_t *error_code) {
        if (certificate_location == NULL && certificate_pem == NULL)
                return 0;

        SSL_CTX_set_default_passwd_cb(context, xkafka_socket_password);
        SSL_CTX_set_default_passwd_cb_userdata(context, (void *)key_password);

        if (certificate_location != NULL) {
                if (SSL_CTX_use_certificate_chain_file(context, certificate_location) != 1 ||
                    SSL_CTX_use_PrivateKey_file(context, key_location, SSL_FILETYPE_PEM) != 1) {
                        xkafka_socket_fail_ssl(error, error_size, error_code, RD_KAFKA_RESP_ERR_INVALID_CONFIG,
                                               "could not read the client certificate");
                        return -1;
                }
        } else {
                BIO *chain = BIO_new_mem_buf(certificate_pem, -1);
                BIO *key = BIO_new_mem_buf(key_pem, -1);
                X509 *certificate = chain == NULL ? NULL : PEM_read_bio_X509(chain, NULL, NULL, NULL);
                EVP_PKEY *private_key = key == NULL ? NULL : PEM_read_bio_PrivateKey(key, NULL, xkafka_socket_password, (void *)key_password);
                int used = certificate != NULL && private_key != NULL && SSL_CTX_use_certificate(context, certificate) == 1 &&
                           SSL_CTX_use_PrivateKey(context, private_key) == 1;
                X509 *extra;

                X509_free(certificate);
                EVP_PKEY_free(private_key);
                while (used && (extra = PEM_read_bio_X509(chain, NULL, NULL, NULL)) != NULL) {
                        /* The context owns what it adds to the chain. */
                        if (SSL_CTX_add_extra_chain_cert(context, extra) != 1) {
                                X509_free(extra);
                                used = 0;
                        }
                }
                BIO_free(chain);
                BIO_free(key);
                if (!used) {
                        xkafka_socket_fail_ssl(error, error_size, error_code, RD_KAFKA_RESP_ERR_INVALID_CONFIG,
                                               "could not read the client certificate");
                        return -1;
                }
                ERR_clear_error();
        }
        /* Loading the key already refused one that does not match the certificate. */
        return 0;
}

static int xkafka_socket_timed_out(void) { return errno == EAGAIN || errno == EWOULDBLOCK; }

/* Connects to the first of the broker's addresses that answers within the
 * timeout, which then bounds each send and receive. */
static int xkafka_socket_connect(xkafka_socket_t *connection, const char *host, int port, int timeout_ms, char *error, size_t error_size,
                                 int32_t *error_code) {
        struct addrinfo hints, *addresses, *address;
        struct timeval timeout;
        char service[16];
        int resolved, failure = ECONNREFUSED, on = 1;

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        snprintf(service, sizeof(service), "%d", port);
        resolved = getaddrinfo(host, service, &hints, &addresses);
        if (resolved != 0) {
                xkafka_socket_fail(error, error_size, error_code, RD_KAFKA_RESP_ERR__RESOLVE, "could not resolve the broker",
                                   gai_strerror(resolved));
                return -1;
        }
        for (address = addresses; address != NULL && connection->fd < 0; address = address->ai_next) {
                int fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
                int flags, outcome = 0;
                socklen_t length = sizeof(outcome);
                struct pollfd waiting;

                if (fd < 0) {
                        failure = errno;
                        continue;
                }
                flags = fcntl(fd, F_GETFL, 0);
                fcntl(fd, F_SETFL, flags | O_NONBLOCK);
                if (connect(fd, address->ai_addr, address->ai_addrlen) != 0) {
                        if (errno != EINPROGRESS) {
                                failure = errno;
                                close(fd);
                                continue;
                        }
                        waiting.fd = fd;
                        waiting.events = POLLOUT;
                        if (poll(&waiting, 1, timeout_ms) != 1) {
                                failure = ETIMEDOUT;
                                close(fd);
                                continue;
                        }
                        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &outcome, &length) != 0 || outcome != 0) {
                                failure = outcome != 0 ? outcome : errno;
                                close(fd);
                                continue;
                        }
                }
                fcntl(fd, F_SETFL, flags);
                connection->fd = fd;
        }
        freeaddrinfo(addresses);
        if (connection->fd < 0) {
                xkafka_socket_fail(error, error_size, error_code, failure == ETIMEDOUT ? RD_KAFKA_RESP_ERR__TIMED_OUT : RD_KAFKA_RESP_ERR__TRANSPORT,
                                   "could not connect to the broker", strerror(failure));
                return -1;
        }
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(connection->fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(connection->fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        setsockopt(connection->fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
#ifdef SO_NOSIGPIPE
        setsockopt(connection->fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
        return 0;
}

static int xkafka_socket_fail_io(char *error, size_t error_size, int32_t *error_code, const char *what) {
        xkafka_socket_fail(error, error_size, error_code, xkafka_socket_timed_out() ? RD_KAFKA_RESP_ERR__TIMED_OUT : RD_KAFKA_RESP_ERR__TRANSPORT,
                           what, xkafka_socket_timed_out() ? "timed out" : strerror(errno));
        return -1;
}

static int xkafka_socket_send(xkafka_socket_t *connection, const unsigned char *data, size_t size, char *error, size_t error_size,
                              int32_t *error_code) {
        while (size > 0) {
                ssize_t sent = send(connection->fd, data, size, XKAFKA_SEND_FLAGS);

                if (sent < 0) {
                        if (errno == EINTR)
                                continue;
                        return xkafka_socket_fail_io(error, error_size, error_code, "could not send to the broker");
                }
                data += sent;
                size -= (size_t)sent;
        }
        return 0;
}

/* Sends whatever TLS has written into its outgoing buffer. */
static int xkafka_socket_flush(xkafka_socket_t *connection, char *error, size_t error_size, int32_t *error_code) {
        unsigned char buffer[16384];
        int pending;

        while ((pending = BIO_read(connection->outgoing, buffer, sizeof(buffer))) > 0)
                if (xkafka_socket_send(connection, buffer, (size_t)pending, error, error_size, error_code) != 0)
                        return -1;
        return 0;
}

/* Receives what is there, at most `size` bytes; 0 means the broker closed. */
static ssize_t xkafka_socket_receive(xkafka_socket_t *connection, unsigned char *out, size_t size, char *error, size_t error_size,
                                     int32_t *error_code) {
        for (;;) {
                ssize_t received = recv(connection->fd, out, size, 0);

                if (received >= 0)
                        return received;
                if (errno != EINTR)
                        return xkafka_socket_fail_io(error, error_size, error_code, "could not receive from the broker");
        }
}

/* Feeds TLS the next bytes from the broker. */
static int xkafka_socket_fill(xkafka_socket_t *connection, char *error, size_t error_size, int32_t *error_code) {
        unsigned char buffer[16384];
        ssize_t received = xkafka_socket_receive(connection, buffer, sizeof(buffer), error, error_size, error_code);

        if (received < 0)
                return -1;
        if (received == 0) {
                xkafka_socket_fail(error, error_size, error_code, RD_KAFKA_RESP_ERR__TRANSPORT, "the broker closed the connection", "");
                return -1;
        }
        BIO_write(connection->incoming, buffer, (int)received);
        return 0;
}

/* What TLS needs before a call can go on: its bytes sent, and more of the
 * broker's received when it wants to read. A failure of TLS itself, an alert
 * the broker sent included, is a refused handshake. */
static int xkafka_socket_tls_step(xkafka_socket_t *connection, int result, const char *what, char *error, size_t error_size,
                                  int32_t *error_code) {
        int reason = SSL_get_error(connection->ssl, result);

        if (xkafka_socket_flush(connection, error, error_size, error_code) != 0)
                return -1;
        if (reason == SSL_ERROR_WANT_READ)
                return xkafka_socket_fill(connection, error, error_size, error_code);
        if (reason == SSL_ERROR_WANT_WRITE)
                return 0;
        if (reason == SSL_ERROR_ZERO_RETURN) {
                xkafka_socket_fail(error, error_size, error_code, RD_KAFKA_RESP_ERR__TRANSPORT, "the broker closed the connection", "");
                return -1;
        }
        {
                long verified = SSL_get_verify_result(connection->ssl);

                if (verified != X509_V_OK) {
                        ERR_clear_error();
                        xkafka_socket_fail(error, error_size, error_code, RD_KAFKA_RESP_ERR__SSL, what,
                                           X509_verify_cert_error_string(verified));
                } else
                        xkafka_socket_fail_ssl(error, error_size, error_code, RD_KAFKA_RESP_ERR__SSL, what);
        }
        return -1;
}

static int xkafka_socket_handshake(xkafka_socket_t *connection, SSL_CTX *context, const char *host, int verify_hostname, char *error,
                                   size_t error_size, int32_t *error_code) {
        size_t length = strlen(host);
        int ip_literal = length > 0 && (strchr(host, ':') != NULL || strspn(host, "0123456789.") == length);
        int result;

        connection->ssl = SSL_new(context);
        connection->incoming = BIO_new(BIO_s_mem());
        connection->outgoing = BIO_new(BIO_s_mem());
        if (connection->ssl == NULL || connection->incoming == NULL || connection->outgoing == NULL) {
                xkafka_socket_fail_ssl(error, error_size, error_code, RD_KAFKA_RESP_ERR__SSL, "could not start TLS");
                return -1;
        }
        SSL_set_bio(connection->ssl, connection->incoming, connection->outgoing);
        SSL_set_connect_state(connection->ssl);
        /* As librdkafka: a name is sent for SNI and checked against the
         * certificate's names, an address only checked against its addresses. */
        if (!ip_literal && !SSL_set_tlsext_host_name(connection->ssl, host)) {
                xkafka_socket_fail_ssl(error, error_size, error_code, RD_KAFKA_RESP_ERR__SSL, "could not name the broker");
                return -1;
        }
        if (verify_hostname) {
                X509_VERIFY_PARAM *parameters = SSL_get0_param(connection->ssl);

                if (!(ip_literal && X509_VERIFY_PARAM_set1_ip_asc(parameters, host) == 1)) {
                        ERR_clear_error();
                        if (!X509_VERIFY_PARAM_set1_host(parameters, host, length)) {
                                xkafka_socket_fail_ssl(error, error_size, error_code, RD_KAFKA_RESP_ERR__SSL, "could not name the broker");
                                return -1;
                        }
                }
        }
        while ((result = SSL_do_handshake(connection->ssl)) != 1)
                if (xkafka_socket_tls_step(connection, result, "the TLS handshake failed", error, error_size, error_code) != 0)
                        return -1;
        return xkafka_socket_flush(connection, error, error_size, error_code);
}

void *xkafka_socket_open(const char *host, int port, int timeout_ms, int tls, const char *ca_location, const char *ca_pem,
                         const char *certificate_location, const char *key_location, const char *certificate_pem, const char *key_pem,
                         const char *key_password, int verify_hostname, char *error, size_t error_size, int32_t *error_code) {
        xkafka_socket_t *connection = (xkafka_socket_t *)calloc(1, sizeof(xkafka_socket_t));

        if (connection == NULL) {
                xkafka_socket_fail(error, error_size, error_code, -1, "could not allocate a connection", "");
                return NULL;
        }
        connection->fd = -1;
        /* The settings are read first, so settings that cannot be used fail
         * before anything connects, as they do when librdkafka is given them. */
        if (tls) {
                connection->context = SSL_CTX_new(TLS_client_method());
                if (connection->context == NULL) {
                        xkafka_socket_fail_ssl(error, error_size, error_code, RD_KAFKA_RESP_ERR__SSL, "could not start TLS");
                        xkafka_socket_free(connection);
                        return NULL;
                }
                SSL_CTX_set_verify(connection->context, SSL_VERIFY_PEER, NULL);
                if (xkafka_socket_authorities(connection->context, ca_location, ca_pem, error, error_size, error_code) != 0 ||
                    xkafka_socket_certificate(connection->context, certificate_location, key_location, certificate_pem, key_pem, key_password,
                                              error, error_size, error_code) != 0) {
                        xkafka_socket_free(connection);
                        return NULL;
                }
        }
        if (xkafka_socket_connect(connection, host, port, timeout_ms, error, error_size, error_code) != 0 ||
            (tls && xkafka_socket_handshake(connection, connection->context, host, verify_hostname, error, error_size, error_code) != 0)) {
                xkafka_socket_free(connection);
                return NULL;
        }
        return connection;
}

int xkafka_socket_write(void *handle, const void *data, size_t size, char *error, size_t error_size, int32_t *error_code) {
        xkafka_socket_t *connection = (xkafka_socket_t *)handle;
        const unsigned char *bytes = (const unsigned char *)data;

        if (connection->ssl == NULL)
                return xkafka_socket_send(connection, bytes, size, error, error_size, error_code);
        while (size > 0) {
                int chunk = size > 16384 ? 16384 : (int)size;
                int written = SSL_write(connection->ssl, bytes, chunk);

                if (written <= 0) {
                        if (xkafka_socket_tls_step(connection, written, "TLS failed", error, error_size, error_code) != 0)
                                return -1;
                        continue;
                }
                bytes += written;
                size -= (size_t)written;
        }
        return xkafka_socket_flush(connection, error, error_size, error_code);
}

int xkafka_socket_read(void *handle, void *out, size_t size, char *error, size_t error_size, int32_t *error_code) {
        xkafka_socket_t *connection = (xkafka_socket_t *)handle;
        unsigned char *bytes = (unsigned char *)out;

        while (size > 0) {
                if (connection->ssl == NULL) {
                        ssize_t received = xkafka_socket_receive(connection, bytes, size, error, error_size, error_code);

                        if (received < 0)
                                return -1;
                        if (received == 0) {
                                xkafka_socket_fail(error, error_size, error_code, RD_KAFKA_RESP_ERR__TRANSPORT,
                                                   "the broker closed the connection", "");
                                return -1;
                        }
                        bytes += received;
                        size -= (size_t)received;
                } else {
                        int chunk = size > 16384 ? 16384 : (int)size;
                        int read = SSL_read(connection->ssl, bytes, chunk);

                        if (read <= 0) {
                                if (xkafka_socket_tls_step(connection, read, "TLS failed", error, error_size, error_code) != 0)
                                        return -1;
                                continue;
                        }
                        bytes += read;
                        size -= (size_t)read;
                }
        }
        return 0;
}

void xkafka_socket_close(void *handle) {
        if (handle != NULL)
                xkafka_socket_free((xkafka_socket_t *)handle);
}

#endif
