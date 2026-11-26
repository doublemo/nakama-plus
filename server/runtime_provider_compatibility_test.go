package server

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/doublemo/nakama-common/api"
	"github.com/doublemo/nakama-common/runtime"
	"github.com/doublemo/nakama-plus/v3/apigrpc"
	"github.com/gofrs/uuid/v5"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type testFleetManager struct {
	runtime.FleetManagerInitializer
	namespace string
}

func (m *testFleetManager) Init(_ runtime.Initializer, _ runtime.NakamaModule, _ runtime.FmCallbackHandler, namespace string) error {
	m.namespace = namespace
	return nil
}

func TestFleetManagerNamespaceCompatibility(t *testing.T) {
	managers := make(map[string]runtime.FleetManager)
	module := &RuntimeGoNakamaModule{fleetManagers: managers}
	initializer := &RuntimeGoInitializer{nk: module, fleetManagers: managers}
	defaultManager, namedManager := &testFleetManager{}, &testFleetManager{}
	if err := initializer.RegisterFleetManager(defaultManager); err != nil {
		t.Fatal(err)
	}
	if err := initializer.RegisterFleetManagerByName("RegionEast", namedManager); err != nil {
		t.Fatal(err)
	}
	if defaultManager.namespace != "" || module.GetFleetManager() != defaultManager {
		t.Fatal("default fleet manager was not registered with an empty namespace")
	}
	if namedManager.namespace != "regioneast" || module.GetFleetManagerByName("REGIONEAST") != namedManager {
		t.Fatal("named fleet manager did not use a case-insensitive namespace")
	}
}

func TestProviderLinkRPCCompatibility(t *testing.T) {
	methods := apigrpc.File_apigrpc_proto.Services().ByName("Nakama").Methods()
	for _, tc := range []struct {
		method protoreflect.Name
		input  protoreflect.FullName
		path   string
	}{
		{"LinkApple", "nakama.api.AccountApple", "/v2/account/link/apple"},
		{"LinkGoogle", "nakama.api.AccountGoogle", "/v2/account/link/google"},
		{"LinkAppleWithOptions", "nakama.api.LinkAppleRequest", "/v2/account/link/apple/options"},
		{"LinkGoogleWithOptions", "nakama.api.LinkGoogleRequest", "/v2/account/link/google/options"},
	} {
		t.Run(string(tc.method), func(t *testing.T) {
			method := methods.ByName(tc.method)
			if method == nil || method.Input().FullName() != tc.input {
				t.Fatalf("expected request type %s for %s", tc.input, tc.method)
			}
			rule, ok := proto.GetExtension(method.Options(), annotations.E_Http).(*annotations.HttpRule)
			if !ok || rule.GetPost() != tc.path || rule.GetBody() != "*" {
				t.Fatalf("expected a POST binding with a JSON body at %s, got %v", tc.path, rule)
			}
		})
	}
}

func TestIAPProviderMultipleSubscriptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		persist bool
	}{
		{"validate", false},
		{"persist", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			userID := uuid.Must(uuid.NewV4())
			var db *sql.DB
			if tc.persist {
				if os.Getenv("TEST_DB_URL") == "" {
					t.Skip("set TEST_DB_URL to a migrated database")
				}
				db = NewDB(t)
				t.Cleanup(func() { _ = db.Close() })
				InsertUser(t, db, userID)
				t.Cleanup(func() {
					if _, err := db.Exec("DELETE FROM users WHERE id = $1", userID); err != nil {
						t.Error(err)
					}
				})
			}

			firstID, secondID := userID.String()+"-first", userID.String()+"-second"
			now := time.Now().Truncate(time.Second)
			registry := &RuntimeIAPProviderRegistry{}
			err := registry.Register("custom", "", 120, nil, func(context.Context, string, map[string]any, map[string]any) ([]*runtime.IAPProviderSubscription, error, codes.Code) {
				return []*runtime.IAPProviderSubscription{
					{OriginalTransactionId: firstID, ProductId: "initial", PurchaseTime: now, ExpiryTime: now.Add(time.Hour)},
					{OriginalTransactionId: secondID, ProductId: "second", PurchaseTime: now, ExpiryTime: now.Add(time.Hour)},
					{OriginalTransactionId: firstID, ProductId: "renewal", PurchaseTime: now, ExpiryTime: now.Add(2 * time.Hour)},
				}, nil, codes.OK
			}, nil)
			if err != nil {
				t.Fatal(err)
			}

			result, err := ValidateSubscription(ctx, zap.NewNop(), db, registry, userID, "", "custom", nil, nil, tc.persist)
			if err != nil {
				t.Fatal(err)
			}
			got := result.GetSubscription()
			if len(got) != 2 || got[0].GetOriginalTransactionId() != firstID || got[0].GetProductId() != "renewal" || got[1].GetOriginalTransactionId() != secondID {
				t.Fatalf("expected one result per transaction using the final renewal, got %v", got)
			}
			for _, sub := range got {
				if sub.GetProvider() != "custom" || sub.GetStore() != api.StoreProvider(120) {
					t.Fatalf("expected custom store metadata, got %v", sub)
				}
			}
			if tc.persist {
				var product string
				err := db.QueryRowContext(ctx, "SELECT product_id FROM subscription WHERE original_transaction_id = $1", firstID).Scan(&product)
				if err != nil || product != "renewal" {
					t.Fatalf("expected stored renewal, got %q, %v", product, err)
				}
			}
		})
	}
}
