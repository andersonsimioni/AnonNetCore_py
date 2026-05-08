# Crypto

Projeto Python simples com os algoritmos:

- `SHA-512`
- `Dilithium` via `ML-DSA-65` do OpenSSL 3.6
- `Kyber` via `ML-KEM-768` do OpenSSL 3.6
- `AES-256-CBC`

## Estrutura

```text
storage/
  db.py
  models/
crypto/
  __init__.py
  _openssl.py
  aes.py
  dilithium.py
  exceptions.py
  kyber.py
  sha512.py
  utils.py
```

## Uso rapido

```python
from crypto import (
    aes_decrypt_text,
    aes_encrypt_text,
    dilithium_sign_hex,
    dilithium_verify_hex,
    generate_dilithium_key_pair,
    generate_kyber_key_pair,
    kyber_decapsulate_hex,
    kyber_encapsulate_hex,
    sha512_hex,
)
from crypto.aes import generate_key_hex

digest_hex = sha512_hex("mensagem")

key_hex = generate_key_hex()
aes_result = aes_encrypt_hex("636f6e746575646f20736967696c6f736f", key_hex)
plain_hex = aes_decrypt_hex(aes_result.payload_hex, key_hex)

dilithium_keys = generate_dilithium_key_pair()
signature_hex = dilithium_sign_hex("6d656e736167656d20617373696e616461", dilithium_keys.private_key_pem)
is_valid = dilithium_verify_hex(
    "6d656e736167656d20617373696e616461",
    signature_hex,
    dilithium_keys.public_key_pem,
)

kyber_keys = generate_kyber_key_pair()
kem_result = kyber_encapsulate_hex(kyber_keys.public_key_pem)
shared_secret_hex = kyber_decapsulate_hex(
    kem_result.ciphertext_hex,
    kyber_keys.private_key_pem,
)
```

## Banco local

O projeto agora possui persistencia local em SQLite usando `SQLAlchemy`, com os modelos compartilhados entre banco e aplicacao:

```python
from sqlalchemy import select

from storage import get_database
from storage.models import LocalPhysicalNodeIdentity

database = get_database()
database.create_schema()

with database.session_scope() as session:
    session.add(
        LocalPhysicalNodeIdentity(
            id="node-1",
            public_key="public-key",
            private_key_encrypted="private-key",
            key_algorithm="ml-dsa-65",
            status="active",
        )
    )

with database.session_scope() as session:
    rows = session.scalars(select(LocalPhysicalNodeIdentity)).all()
```

- arquivo central: `storage/db.py`
- modelos ORM: `storage/models/`
- banco padrao: `data/local/anonnetcore.db`

## Observacoes

- AES retorna `payload_hex` no formato `IV + ciphertext`.
- A API publica de cifra e assinatura usa `str` HEX para dados de entrada e saida.
- A decifragem AES recebe `payload_hex` em HEX e retorna o plaintext em HEX.
- Assinaturas e resultados do KEM tambem sao retornados em HEX.
- O projeto depende de `OpenSSL 3.6+` no sistema para `ML-DSA` e `ML-KEM`.
- O acesso ao banco usa `SQLAlchemy`.
