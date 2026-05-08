class CryptoError(Exception):
    """Base exception for the crypto package."""


class InvalidHexError(CryptoError):
    """Raised when a value expected to be hex is not valid."""


class OpenSSLExecutionError(CryptoError):
    """Raised when an OpenSSL command fails."""
