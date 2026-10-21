package apis

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	kafkaesque_auth "github.com/nikhil25803/kafkaesque/internals/auth"
)

type createUserRequest struct {
	Name     string `json:"name"`
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

func (s *server) listUsers(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r)
	if err != nil {
		writeInvalid(w, err.Error())
		return
	}

	users, total, err := s.users.ListUsers(r.Context(), page.page, page.pageSize)
	if err != nil {
		s.logger.Error("user_list_failed", "reason", "database_unavailable")
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "user service unavailable")
		return
	}

	_, _, meta := pageBounds(total, page)
	writeSuccess(w, http.StatusOK, users, meta)
}

func (s *server) createUser(w http.ResponseWriter, r *http.Request) {
	request, err := decodeCreateUserRequest(w, r)
	if err != nil {
		actor, _ := kafkaesque_auth.UserFromContext(r.Context())
		s.logger.Warn("user_creation_rejected", "actor_user_id", actor.ID, "field", "request")
		writeInvalid(w, "invalid user request")
		return
	}

	user, err := s.users.CreateUser(r.Context(), kafkaesque_auth.CreateUserInput{
		Name:     request.Name,
		Username: request.Username,
		Email:    request.Email,
		Password: request.Password,
		Role:     request.Role,
	})
	if err != nil {
		s.writeCreateUserError(w, r, err)
		return
	}

	actor, _ := kafkaesque_auth.UserFromContext(r.Context())
	s.logger.Info(
		"user_created",
		"actor_user_id", actor.ID,
		"user_id", user.ID,
		"role", user.Role,
	)
	writeSuccess(w, http.StatusCreated, user, map[string]any{})
}

func decodeCreateUserRequest(w http.ResponseWriter, r *http.Request) (createUserRequest, error) {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()

	var request createUserRequest
	if err := decoder.Decode(&request); err != nil {
		return createUserRequest{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return createUserRequest{}, errors.New("request must contain one JSON object")
	}

	return request, nil
}

func (s *server) writeCreateUserError(w http.ResponseWriter, r *http.Request, err error) {
	actor, _ := kafkaesque_auth.UserFromContext(r.Context())

	var validationError *kafkaesque_auth.ValidationError
	switch {
	case errors.As(err, &validationError):
		s.logger.Warn(
			"user_creation_rejected",
			"actor_user_id", actor.ID,
			"field", validationError.Field,
		)
		writeInvalid(w, validationError.Error())
	case errors.Is(err, kafkaesque_auth.ErrUsernameExists):
		s.logger.Warn("user_creation_conflict", "actor_user_id", actor.ID, "field", "username")
		writeError(w, http.StatusConflict, "username_exists", "username already exists")
	case errors.Is(err, kafkaesque_auth.ErrEmailExists):
		s.logger.Warn("user_creation_conflict", "actor_user_id", actor.ID, "field", "email")
		writeError(w, http.StatusConflict, "email_exists", "email already exists")
	default:
		s.logger.Error("user_creation_failed", "actor_user_id", actor.ID, "reason", "database_unavailable")
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "user service unavailable")
	}
}
