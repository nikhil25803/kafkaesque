CREATE FUNCTION protect_system_role() RETURNS trigger AS $$
BEGIN
  IF OLD.system THEN
    RAISE EXCEPTION 'system roles cannot be changed';
  END IF;
  RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER protect_system_role_update
BEFORE UPDATE OR DELETE ON roles
FOR EACH ROW EXECUTE FUNCTION protect_system_role();

CREATE FUNCTION protect_system_role_permissions() RETURNS trigger AS $$
DECLARE
  protected boolean;
BEGIN
  SELECT system INTO protected
  FROM roles
  WHERE id = COALESCE(OLD.role_id, NEW.role_id);

  IF protected THEN
    RAISE EXCEPTION 'system role permissions cannot be changed';
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER protect_system_role_permission_changes
BEFORE INSERT OR UPDATE OR DELETE ON role_permissions
FOR EACH ROW EXECUTE FUNCTION protect_system_role_permissions();

CREATE FUNCTION protect_permission_catalog() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'the permission catalog cannot be changed';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER protect_permission_catalog_changes
BEFORE INSERT OR UPDATE OR DELETE ON permissions
FOR EACH STATEMENT EXECUTE FUNCTION protect_permission_catalog();
