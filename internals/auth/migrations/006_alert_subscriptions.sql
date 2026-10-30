DROP TRIGGER protect_permission_catalog_changes ON permissions;
DROP TRIGGER protect_system_role_permission_changes ON role_permissions;

INSERT INTO permissions(module, resource, action) VALUES
  ('alert', 'subscription', 'view'),
  ('alert', 'subscription', 'create'),
  ('alert', 'subscription', 'edit'),
  ('alert', 'subscription', 'delete');

INSERT INTO role_permissions(role_id, permission_id)
SELECT r.id, p.id
FROM roles r
CROSS JOIN permissions p
WHERE p.module = 'alert'
  AND p.resource = 'subscription'
  AND (
    r.name = 'super_admin'
    OR (r.name = 'admin' AND p.action IN ('view', 'create', 'edit'))
    OR (r.name = 'user' AND p.action = 'view')
  );

CREATE TRIGGER protect_permission_catalog_changes
BEFORE INSERT OR UPDATE OR DELETE ON permissions
FOR EACH STATEMENT EXECUTE FUNCTION protect_permission_catalog();

CREATE TRIGGER protect_system_role_permission_changes
BEFORE INSERT OR UPDATE OR DELETE ON role_permissions
FOR EACH ROW EXECUTE FUNCTION protect_system_role_permissions();

CREATE TABLE alert_subscriptions (
  id uuid PRIMARY KEY DEFAULT uuidv7(),
  consumer_group text NOT NULL,
  topic text NOT NULL,
  metric text NOT NULL CHECK (metric IN ('average', 'partition')),
  lag_threshold bigint NOT NULL CHECK (lag_threshold >= 0),
  cooldown_minutes integer NOT NULL CHECK (cooldown_minutes BETWEEN 1 AND 1440),
  notify_on_resolve boolean NOT NULL DEFAULT true,
  webhook_ciphertext bytea NOT NULL,
  webhook_nonce bytea NOT NULL,
  state text NOT NULL DEFAULT 'healthy' CHECK (state IN ('healthy', 'firing', 'resolving')),
  last_value double precision,
  last_evaluated_at timestamptz,
  last_attempt_at timestamptz,
  last_notified_at timestamptz,
  created_by uuid REFERENCES users(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX alert_subscriptions_monitor_idx ON alert_subscriptions (consumer_group, topic);
