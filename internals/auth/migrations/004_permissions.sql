ALTER TABLE roles
  ADD COLUMN system boolean NOT NULL DEFAULT false;

UPDATE roles
SET system = true
WHERE name IN ('super_admin', 'admin', 'user');

CREATE TABLE permissions (
  id uuid PRIMARY KEY DEFAULT uuidv7(),
  module text NOT NULL,
  resource text NOT NULL,
  action text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (module, resource, action)
);

CREATE TABLE role_permissions (
  role_id uuid NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
  permission_id uuid NOT NULL REFERENCES permissions(id) ON DELETE RESTRICT,
  assigned_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (role_id, permission_id)
);

INSERT INTO permissions(module, resource, action) VALUES
  ('user', 'account', 'view'),
  ('user', 'account', 'create'),
  ('user', 'account', 'edit'),
  ('user', 'account', 'delete');

INSERT INTO role_permissions(role_id, permission_id)
SELECT r.id, p.id
FROM roles r
CROSS JOIN permissions p
WHERE r.name = 'super_admin'
   OR (r.name = 'admin' AND p.action IN ('view', 'create', 'edit'))
   OR (r.name = 'user' AND p.action = 'view');
