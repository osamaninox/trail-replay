# AGENTS.md

## Database Migrations

- **NEVER add foreign key constraints in migrations — ever.** No `references` clauses, no `FOREIGN KEY`, no `ON DELETE CASCADE` in any new migration. Referential integrity is enforced by application code and query design, not the schema. (One pre-existing FK in `00001` on `wal_change.transaction_id` is grandfathered; do not replicate the pattern.)
