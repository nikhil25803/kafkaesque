package apis

import (
	"errors"
	"net/http"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

type roleRequest struct {
	Name        string   `json:"name"`
	Permissions []string `json:"permissions"`
}

func (s *server) listRoles(w http.ResponseWriter, r *http.Request) {
	roles, err := s.roles.ListRoles(r.Context())
	if err != nil {
		s.writeRoleError(w, r, "list", err)
		return
	}
	writeSuccess(w, http.StatusOK, roles, map[string]any{})
}

func (s *server) getRole(w http.ResponseWriter, r *http.Request) {
	role, err := s.roles.GetRole(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeRoleError(w, r, "read", err)
		return
	}
	writeSuccess(w, http.StatusOK, role, map[string]any{})
}

func (s *server) createRole(w http.ResponseWriter, r *http.Request) {
	var request roleRequest
	if err := decodeJSONRequest(w, r, &request); err != nil {
		writeInvalid(w, "invalid role request")
		return
	}
	actor, _ := kafkaesque_auth.UserFromContext(r.Context())
	role, err := s.roles.CreateRole(r.Context(), actor, kafkaesque_auth.RoleInput(request))
	if err != nil {
		s.writeRoleError(w, r, "create", err)
		return
	}
	s.logger.Info("role_created", "actor_user_id", actor.ID, "role_id", role.ID, "role", role.Name)
	writeSuccess(w, http.StatusCreated, role, map[string]any{})
}

func (s *server) updateRole(w http.ResponseWriter, r *http.Request) {
	var request roleRequest
	if err := decodeJSONRequest(w, r, &request); err != nil {
		writeInvalid(w, "invalid role request")
		return
	}
	actor, _ := kafkaesque_auth.UserFromContext(r.Context())
	role, err := s.roles.UpdateRole(r.Context(), actor, r.PathValue("id"), kafkaesque_auth.RoleInput(request))
	if err != nil {
		s.writeRoleError(w, r, "update", err)
		return
	}
	s.logger.Info("role_updated", "actor_user_id", actor.ID, "role_id", role.ID, "role", role.Name)
	writeSuccess(w, http.StatusOK, role, map[string]any{})
}

func (s *server) deleteRole(w http.ResponseWriter, r *http.Request) {
	actor, _ := kafkaesque_auth.UserFromContext(r.Context())
	id := r.PathValue("id")
	if err := s.roles.DeleteRole(r.Context(), actor, id); err != nil {
		s.writeRoleError(w, r, "delete", err)
		return
	}
	s.logger.Info("role_deleted", "actor_user_id", actor.ID, "role_id", id)
	writeSuccess(w, http.StatusOK, map[string]string{"status": "DELETED"}, map[string]any{})
}

func (s *server) writeRoleError(w http.ResponseWriter, r *http.Request, operation string, err error) {
	actor, _ := kafkaesque_auth.UserFromContext(r.Context())
	var validationError *kafkaesque_auth.ValidationError
	switch {
	case errors.As(err, &validationError):
		writeInvalid(w, validationError.Error())
	case errors.Is(err, kafkaesque_auth.ErrForbidden):
		writeError(w, http.StatusForbidden, "forbidden", "permission denied")
	case errors.Is(err, kafkaesque_auth.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "role not found")
	case errors.Is(err, kafkaesque_auth.ErrRoleExists):
		writeError(w, http.StatusConflict, "role_exists", "role already exists")
	case errors.Is(err, kafkaesque_auth.ErrRoleAssigned):
		writeError(w, http.StatusConflict, "role_assigned", "role is assigned to users")
	default:
		s.logger.Error("role_"+operation+"_failed", "actor_user_id", actor.ID, "reason", "database_unavailable")
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "role service unavailable")
	}
}
