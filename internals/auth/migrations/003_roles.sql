INSERT INTO roles(name) VALUES ('admin'), ('user') ON CONFLICT (name) DO NOTHING;

CREATE UNIQUE INDEX user_roles_user_id_idx ON user_roles (user_id);
