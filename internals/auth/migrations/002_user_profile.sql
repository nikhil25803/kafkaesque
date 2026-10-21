ALTER TABLE users
  ADD COLUMN name text NOT NULL DEFAULT 'Super Admin',
  ADD COLUMN email text NOT NULL DEFAULT 'bootstrap-pending@invalid.local';

ALTER TABLE users ALTER COLUMN name DROP DEFAULT;
ALTER TABLE users ALTER COLUMN email DROP DEFAULT;

CREATE UNIQUE INDEX users_email_lower_idx ON users (lower(email));
