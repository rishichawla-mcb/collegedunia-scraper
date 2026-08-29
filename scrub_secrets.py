"""Clear credentials out of the database once they are set as environment vars.

Two places leak: the `settings` table (proxy gateway URL with user:pass, and the
SMTP password), and every historical row of `jobs.config_json` / `sa_jobs.config_json`,
which got a verbatim copy of both.

This is the one place in the project that deliberately removes stored values —
and it only ever removes CREDENTIALS, never scraped data. It refuses to clear
anything that is not already available from the environment, so you cannot lock
the scraper out of its own proxy by running it too early.

  python scrub_secrets.py            report only — shows what is exposed
  python scrub_secrets.py --apply    clear it

Set these in Render → Environment first:
  CD_PROXY_GATEWAY   http://user:pass@gw.dataimpulse.com:823
  CD_SMTP_PASSWORD   your SMTP password
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db  # noqa: E402


def mask(u):
    return re.sub(r"//[^@/]+@", "//***:***@", str(u or ""))


def main(apply_changes=False, db_path=None):
    db_path = db_path or db.DB_PATH
    print(f"DB: {db_path}   mode: {'APPLY' if apply_changes else 'REPORT ONLY'}\n")

    env_gw = bool(os.environ.get("CD_PROXY_GATEWAY"))
    env_pw = bool(os.environ.get("CD_SMTP_PASSWORD"))
    print(f"CD_PROXY_GATEWAY set : {env_gw}")
    print(f"CD_SMTP_PASSWORD set : {env_pw}\n")

    stored_gw = db.get_setting("proxy_gateway", "", db_path=db_path) or ""
    stored_smtp = db.get_setting("smtp", {}, db_path=db_path) or {}
    stored_pw = stored_smtp.get("password") or ""

    print("exposed in `settings`:")
    print(f"   proxy_gateway  : {'YES  ' + mask(stored_gw) if stored_gw else 'clean'}")
    print(f"   smtp.password  : {'YES  (' + str(len(stored_pw)) + ' chars)' if stored_pw else 'clean'}")

    # historical job rows
    hits = {}
    with db.connect(db_path) as conn:
        for table in ("jobs", "sa_jobs"):
            try:
                rows = conn.execute(
                    f"SELECT id, config_json FROM {table} "
                    f"WHERE config_json LIKE '%proxy_gateway%' "
                    f"OR config_json LIKE '%password%'").fetchall()
            except Exception:
                continue
            bad = []
            for r in rows:
                try:
                    cfg = json.loads(r["config_json"] or "{}")
                except Exception:
                    continue
                if cfg.get("proxy_gateway") or (cfg.get("smtp") or {}).get("password"):
                    bad.append(r["id"])
            if bad:
                hits[table] = bad
    print("\nexposed in job history:")
    if hits:
        for t, ids in hits.items():
            print(f"   {t}: {len(ids)} rows  (e.g. {ids[:6]})")
    else:
        print("   clean")

    if not apply_changes:
        print()
        if stored_gw and not env_gw:
            print("!! proxy_gateway is stored but CD_PROXY_GATEWAY is NOT set — "
                  "set it first, or clearing it would break scraping.")
        if stored_pw and not env_pw:
            print("!! smtp password is stored but CD_SMTP_PASSWORD is NOT set — "
                  "set it first, or email alerts would break.")
        print("\nnothing written. re-run with --apply to clear.")
        return 0

    cleared = []
    with db.connect(db_path) as conn:
        if stored_gw and env_gw:
            conn.execute("UPDATE settings SET value=? WHERE key='proxy_gateway'",
                         (json.dumps(""),))
            cleared.append("settings.proxy_gateway")
        elif stored_gw:
            print("SKIPPED settings.proxy_gateway — CD_PROXY_GATEWAY is not set.")
        if stored_pw and env_pw:
            safe = dict(stored_smtp)
            safe["password"] = ""
            conn.execute("UPDATE settings SET value=? WHERE key='smtp'",
                         (json.dumps(safe),))
            cleared.append("settings.smtp.password")
        elif stored_pw:
            print("SKIPPED settings.smtp.password — CD_SMTP_PASSWORD is not set.")

        # Job history is safe to scrub unconditionally: these are historical
        # copies, and the worker now rehydrates from env/settings at run time.
        for table, ids in hits.items():
            n = 0
            for jid in ids:
                row = conn.execute(
                    f"SELECT config_json FROM {table} WHERE id=?", (jid,)).fetchone()
                try:
                    cfg = json.loads(row["config_json"] or "{}")
                except Exception:
                    continue
                conn.execute(f"UPDATE {table} SET config_json=? WHERE id=?",
                             (json.dumps(db.redact_secrets(cfg)), jid))
                n += 1
            cleared.append(f"{table}.config_json ({n} rows)")
        conn.commit()

    print("\ncleared: " + (", ".join(cleared) if cleared else "nothing"))
    print("\nNOTE: SQLite keeps freed pages in the file. Run `VACUUM` if you plan "
          "to hand the .db file to anyone:  sqlite3 <db> 'VACUUM;'")
    return 0


if __name__ == "__main__":
    sys.exit(main("--apply" in sys.argv))
