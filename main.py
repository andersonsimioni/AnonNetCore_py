from __future__ import annotations


def main() -> None:
    from sqlalchemy import select

    from storage import get_database
    from storage.models import LocalPhysicalNodeIdentity

    database = get_database()
    database.create_schema()

    with database.session_scope() as session:
        node = session.get(
            LocalPhysicalNodeIdentity,
            "example-local-node",
        )
        if node is None:
            node = LocalPhysicalNodeIdentity(
                id="example-local-node",
                public_key="public-key-placeholder",
                private_key_encrypted="private-key-placeholder",
                key_algorithm="ml-dsa-65",
                status="active",
            )
            session.add(node)

    with database.session_scope() as session:
        nodes = session.scalars(
            select(LocalPhysicalNodeIdentity).order_by(LocalPhysicalNodeIdentity.id)
        ).all()

    print("Banco SQLite inicializado com sucesso.")
    print(f"Arquivo: {database.config.db_path}")
    print("LocalPhysicalNodeIdentity:")
    for node in nodes:
        print(f"- id={node.id} status={node.status} algorithm={node.key_algorithm}")


if __name__ == "__main__":
    main()
