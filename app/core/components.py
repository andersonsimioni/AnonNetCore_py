from __future__ import annotations


class EngineBoundComponent:
    """Componente simples que guarda referencia para a engine."""

    def __init__(self, engine) -> None:
        self.engine = engine
