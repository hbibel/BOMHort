//go:build integration

package clickhouse

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/seebom-labs/seebom/backend/internal/config"
)

var (
	testClient          *Client
	testClientSetupErrs []string
)

func TestMain(m *testing.M) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		testClientSetupErrs = append(testClientSetupErrs, "Environment variable CLICKHOUSE_HOST must be set")
	}

	var port int
	var err error
	p := os.Getenv("CLICKHOUSE_PORT")
	if host == "" {
		testClientSetupErrs = append(testClientSetupErrs, "Environment variable CLICKHOUSE_DATABASE must be set")
	}
	if port, err = strconv.Atoi(p); err != nil {
		testClientSetupErrs = append(testClientSetupErrs, fmt.Sprintf("Value of CLICKHOUSE_PORT '%s' is not a valid port: %v", p, err))
	}

	database := os.Getenv("CLICKHOUSE_DATABASE")
	if host == "" {
		testClientSetupErrs = append(testClientSetupErrs, "Environment variable CLICKHOUSE_DATABASE must be set")
	}

	user := os.Getenv("CLICKHOUSE_USER")
	if user == "" {
		user = "default"
	}

	password := os.Getenv("CLICKHOUSE_PASSWORD")

	testCfg := &config.Config{
		ClickHouseHost:     host,
		ClickHousePort:     port,
		ClickHouseDatabase: database,
		ClickHouseUser:     user,
		ClickHousePassword: password,
	}

	testClient, err = NewClient(testCfg)
	if err != nil {
		testClientSetupErrs = append(testClientSetupErrs, fmt.Sprintf("ClickHouse not available: %v", err))
	}

	code := m.Run()

	if testClient != nil {
		_ = testClient.Close()
	}

	os.Exit(code)
}

func requireClientSetup(t *testing.T) {
	t.Helper()

	if len(testClientSetupErrs) > 0 {
		msg := strings.Join(testClientSetupErrs, "\n")
		t.Fatalf("%s", msg)
	}
}
