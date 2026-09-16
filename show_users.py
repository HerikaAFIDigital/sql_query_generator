import psycopg2

class DbAccessInspector:
    """Utility to check who can access the database and what they're
    allowed to do — use this to confirm your readonly user really is
    read-only."""

    def __init__(self, pg_kwargs):
        self.pg_kwargs = pg_kwargs

    def _connect(self):
        return psycopg2.connect(**self.pg_kwargs)

    def list_roles(self):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolcanlogin
            FROM pg_roles
            ORDER BY rolname
        """)
        roles = cur.fetchall()
        cur.close()
        conn.close()
        return roles

    def list_grants(self, role_name):
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name, privilege_type
            FROM information_schema.role_table_grants
            WHERE grantee = %s
            ORDER BY table_name, privilege_type
        """, (role_name,))
        grants = cur.fetchall()
        cur.close()
        conn.close()
        return grants

    def print_summary(self, role_name=None):
        print("=== Database roles ===")
        for rolname, is_super, can_createdb, can_createrole, can_login in self.list_roles():
            flags = []
            if is_super: flags.append("SUPERUSER")
            if can_createdb: flags.append("CAN_CREATE_DB")
            if can_createrole: flags.append("CAN_CREATE_ROLE")
            if can_login: flags.append("CAN_LOGIN")
            print(f"{rolname}: {', '.join(flags) if flags else 'no special privileges'}")

        if role_name:
            print(f"\n=== Table grants for '{role_name}' ===")
            grants = self.list_grants(role_name)
            if not grants:
                print("(no explicit table grants found)")
            for table_name, privilege in grants:
                print(f"{table_name}: {privilege}")


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    inspector = DbAccessInspector(dict(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DATABASE", "vanna_demo"),
        user=os.getenv("POSTGRES_USER", "vanna_readonly"),
        password=os.getenv("POSTGRES_PASSWORD"),
    ))
    inspector.print_summary(role_name=os.getenv("POSTGRES_USER", "vanna_readonly"))