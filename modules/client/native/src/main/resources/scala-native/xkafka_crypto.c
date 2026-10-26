#ifdef SCALANATIVE_LINK_RDKAFKA

/* The digests, keyed digests and random bytes the group connection's SASL
 * exchange needs, from the OpenSSL librdkafka is built against.
 *
 * A digest is named by its size in bits: 256 for SHA-256, 512 for SHA-512.
 * Each function writes into `out`, which must hold the digest's size (or
 * `out_size` bytes), and returns 0, or -1 for a digest of another size or
 * when OpenSSL could not compute it. */

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>

#include <stddef.h>

static const EVP_MD *xkafka_digest(int bits) {
        switch (bits) {
        case 256:
                return EVP_sha256();
        case 512:
                return EVP_sha512();
        default:
                return NULL;
        }
}

int xkafka_hash(int bits, const void *data, int data_size, void *out) {
        const EVP_MD *digest = xkafka_digest(bits);

        if (digest == NULL)
                return -1;
        return EVP_Digest(data, (size_t)data_size, out, NULL, digest, NULL) == 1
                   ? 0
                   : -1;
}

int xkafka_hmac(int bits,
                const void *key,
                int key_size,
                const void *data,
                int data_size,
                void *out) {
        const EVP_MD *digest = xkafka_digest(bits);

        if (digest == NULL)
                return -1;
        return HMAC(digest, key, key_size, data, (size_t)data_size, out,
                    NULL) == NULL
                   ? -1
                   : 0;
}

int xkafka_pbkdf2(int bits,
                  const void *password,
                  int password_size,
                  const void *salt,
                  int salt_size,
                  int iterations,
                  void *out,
                  int out_size) {
        const EVP_MD *digest = xkafka_digest(bits);

        if (digest == NULL)
                return -1;
        return PKCS5_PBKDF2_HMAC(password, password_size, salt, salt_size,
                                 iterations, digest, out_size, out) == 1
                   ? 0
                   : -1;
}

int xkafka_random_bytes(void *out, int size) {
        return RAND_bytes(out, size) == 1 ? 0 : -1;
}

#endif
