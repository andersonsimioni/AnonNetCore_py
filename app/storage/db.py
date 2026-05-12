from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .models import Base


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "local"
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "anonnetcore.db"


@dataclass(frozen=True)
class DatabaseConfig:
    db_path: Path = DEFAULT_DB_PATH
    echo: bool = False

    @property
    def url(self) -> str:
        return f"sqlite:///{self.db_path.as_posix()}"


class DatabaseManager:
    """Acesso central e simples ao banco via SQLAlchemy."""

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self.config = config or DatabaseConfig()
        self.config.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.engine: Engine = create_engine(
            self.config.url,
            echo=self.config.echo,
            future=True,
        )
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            class_=Session,
        )

    def create_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    def drop_schema(self) -> None:
        Base.metadata.drop_all(self.engine)

    def session(self) -> Session:
        return self.SessionLocal()

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        db = self.session()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()


_database_manager: DatabaseManager | None = None


def get_database(config: DatabaseConfig | None = None) -> DatabaseManager:
    global _database_manager

    if _database_manager is None:
        _database_manager = DatabaseManager(config=config)

    return _database_manager


def get_engine() -> Engine:
    return get_database().engine


def get_session() -> Session:
    return get_database().session()
