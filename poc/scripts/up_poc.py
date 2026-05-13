from __future__ import annotations

from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    poc_dir = root / "poc"

    print("PoC preparada.")
    print("1. Inicie o core com API HTTP/WS habilitada.")
    print("2. Sirva a pasta poc da raiz do projeto.")
    print(f"   Exemplo: python -m http.server 18100 -d {poc_dir}")
    print("3. Abra http://127.0.0.1:18100/web/")


if __name__ == "__main__":
    main()
