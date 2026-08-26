import json
import sqlite3
import sys


source_path, backup_path = sys.argv[1:3]
source = sqlite3.connect(source_path, timeout=30)
backup = sqlite3.connect(backup_path, timeout=30)
source.backup(backup)
backup.close()

statuses = dict(
    source.execute(
        "SELECT status, COUNT(*) FROM download_records GROUP BY status"
    ).fetchall()
)
success_count, success_bytes = source.execute(
    "SELECT COUNT(*), COALESCE(SUM(file_size), 0) "
    "FROM download_records WHERE status = 'success'"
).fetchone()
source.close()

verification = sqlite3.connect(backup_path, timeout=30)
quick_check = verification.execute("PRAGMA quick_check").fetchone()[0]
verification.close()

print(
    json.dumps(
        {
            "backup_quick_check": quick_check,
            "statuses": statuses,
            "success_bytes": success_bytes,
            "success_count": success_count,
        },
        sort_keys=True,
    )
)
