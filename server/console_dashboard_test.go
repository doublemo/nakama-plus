package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

func TestDashboardStaticRoutes(t *testing.T) {
	router := mux.NewRouter()
	if err := registerDashboardHandlers(zap.NewNop(), router); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		path       string
		wantStatus int
	}{
		{"/static", http.StatusNotFound},
		{"/static/", http.StatusNotFound},
		{"/static/spinner.svg", http.StatusOK},
		{"/", http.StatusOK},
		{"/status", http.StatusOK},
	} {
		t.Run(tc.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if response.Code != tc.wantStatus {
				t.Errorf("GET %s returned %d, want %d", tc.path, response.Code, tc.wantStatus)
			}
		})
	}
}
