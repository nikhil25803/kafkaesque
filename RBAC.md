# Account RBAC

Kafka observability is available to every authenticated account. Permissions
control account management only.

## Default roles

| Role | View users | Create users | Edit users | Delete users |
| --- | --- | --- | --- | --- |
| Super Admin | Yes | Yes | Yes | Yes |
| Admin | Yes | Yes | Yes | No |
| User | Yes | No | No | No |

The canonical permission keys are `user:account:view`,
`user:account:create`, `user:account:edit`, and `user:account:delete`.

Super Admin, Admin, and User are system roles. Their names and grants are
protected by PostgreSQL and verified at startup. Only the configured Super
Admin can administer roles or assign Admin and custom roles.

## Custom roles

Super Admin can create, edit, and delete custom roles from `/ui/roles`.
Custom roles can receive View, Create, and Edit. Create and Edit require View;
Delete remains reserved for Super Admin. A custom role cannot be deleted while
assigned to an account.

Each account has exactly one role. Accounts with Create can create User
accounts. Accounts with Edit can change the name, username, and email of User
accounts. Only Super Admin can change roles, delete accounts, or manage Admin
and custom-role accounts.

The UI presents these permissions by module:

```text
User
  List Users
  Create Users
  Edit Users
  Delete Users
```

API and database contracts continue to use the canonical permission keys.

## Account deletion

Deletion permanently removes the account, its role assignment, and every
session. A custom role assigned to the deleted user remains available for reuse.
The configured Super Admin cannot be edited, reassigned, or deleted.
