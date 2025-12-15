# Maintenance Commands

blq provides commands for managing log storage and cleaning up old data.

## prune - Remove Old Logs

Delete log files older than a specified number of days.

```bash
blq prune                     # Remove logs older than 30 days
blq prune --older-than 7      # Remove logs older than 7 days
blq prune --dry-run           # Preview what would be removed
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--older-than DAYS` | `-d` | Days to keep (default: 30) |
| `--dry-run` | | Show what would be removed without deleting |

### Output

```bash
$ blq prune --older-than 14 --dry-run
Would remove: .lq/logs/date=2024-01-01
Would remove: .lq/logs/date=2024-01-02
Would remove: .lq/logs/date=2024-01-03

Dry run: would remove 3 date partitions

$ blq prune --older-than 14
Removed: .lq/logs/date=2024-01-01
Removed: .lq/logs/date=2024-01-02
Removed: .lq/logs/date=2024-01-03
```

If no logs are old enough:
```bash
$ blq prune --older-than 7
No logs older than 7 days
```

## Storage Structure

blq stores logs using Hive partitioning:

```
.lq/
├── logs/
│   ├── date=2024-01-15/
│   │   └── source=build/
│   │       └── 001_make_103000.parquet
│   ├── date=2024-01-16/
│   │   └── source=test/
│   │       └── 002_pytest_091500.parquet
│   └── ...
├── raw/              # Optional: raw log files
├── config.yaml       # Project configuration
├── commands.yaml     # Registered commands
└── schema.sql        # SQL schema and macros
```

The `prune` command removes entire date partitions (directories) based on the date in the directory name.

## Use Cases

### Regular Cleanup

Add to cron or CI for automatic cleanup:
```bash
# Weekly cleanup of logs older than 30 days
0 0 * * 0 cd /path/to/project && blq prune --older-than 30
```

### Before Releases

Clean up old logs before packaging or archiving:
```bash
blq prune --older-than 7
git archive --prefix=project/ HEAD > release.tar
```

### Disk Space Recovery

Check what would be removed before cleaning:
```bash
blq prune --dry-run --older-than 3
# Review output
blq prune --older-than 3
```

### CI Environment

Keep CI storage lean:
```yaml
# .github/workflows/ci.yml
- name: Cleanup old logs
  run: blq prune --older-than 1
```

## Best Practices

1. **Use dry-run first**: Always preview with `--dry-run` before bulk deletion
2. **Balance retention**: Keep enough history for trend analysis, but not so much it wastes space
3. **Automate cleanup**: Use cron or CI to prevent unbounded growth
4. **Consider project needs**: Active development may benefit from longer retention

## Manual Cleanup

For more granular control, you can manually manage the `.lq/logs/` directory:

```bash
# See storage usage
du -sh .lq/logs/

# See logs by date
ls -la .lq/logs/

# Remove specific dates
rm -rf .lq/logs/date=2024-01-01

# Remove specific source
rm -rf .lq/logs/*/source=old-build/
```

## Future Commands

Planned maintenance features:
- `blq vacuum` - Compact and optimize parquet files
- `blq export` - Export logs to external storage
- `blq archive` - Archive old logs before deletion
