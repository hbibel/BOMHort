//go:build integration

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"

	"github.com/seebom-labs/seebom/backend/internal/config"
)

const (
	testClickHouseDatabase = "seebom_test"
	testClickHouseUser     = "testuser"
	testClickHousePassword = "testusersecret"
)

var (
	testClient          *Client
	testClientSetupErrs []string
	testContainer       *tcclickhouse.ClickHouseContainer
)

func TestMain(m *testing.M) {
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelSetup()

	var err error
	testContainer, testClient, err = setupClickHouseTestClient(setupCtx)
	if err != nil {
		testClientSetupErrs = append(testClientSetupErrs, err.Error())
	}

	code := m.Run()

	if testClient != nil {
		if err := testClient.Close(); err != nil && code == 0 {
			fmt.Fprintf(os.Stderr, "failed to close ClickHouse test client: %v\n", err)
			code = 1
		}
	}
	if testContainer != nil {
		teardownCtx, cancelTeardown := context.WithTimeout(context.Background(), 30*time.Second)
		if err := testContainer.Terminate(teardownCtx); err != nil && code == 0 {
			log.Printf("Failed to terminate ClickHouse testcontainer: %v\n", err)
		}
		cancelTeardown()
	}

	os.Exit(code)
}

func setupClickHouseTestClient(ctx context.Context) (*tcclickhouse.ClickHouseContainer, *Client, error) {
	image, err := clickHouseTestImage()
	if err != nil {
		return nil, nil, err
	}

	ctr, err := tcclickhouse.Run(ctx,
		image,
		tcclickhouse.WithUsername(testClickHouseUser),
		tcclickhouse.WithPassword(testClickHousePassword),
		tcclickhouse.WithDatabase(testClickHouseDatabase),
	)
	if err != nil {
		return ctr, nil, fmt.Errorf("failed to start ClickHouse testcontainer: %w", err)
	}

	cfg, err := clickHouseTestConfig(ctx, ctr)
	if err != nil {
		_ = ctr.Terminate(ctx)
		return nil, nil, err
	}

	client, err := NewClient(cfg)
	if err != nil {
		_ = ctr.Terminate(ctx)
		return nil, nil, fmt.Errorf("ClickHouse testcontainer not available: %w", err)
	}

	if err := runClickHouseTestMigrations(ctx, client); err != nil {
		_ = client.Close()
		_ = ctr.Terminate(ctx)
		return nil, nil, err
	}

	return ctr, client, nil
}

func clickHouseTestImage() (string, error) {
	composeFile, err := findPathTo("docker-compose.yml")
	if err != nil {
		return "", fmt.Errorf("can't find docker compose file: %w", err)
	}
	image, err := clickHouseImageFromCompose(composeFile)
	if err != nil {
		return "", fmt.Errorf("could not identify version for clickhouse image: %w", err)
	}

	return image, nil
}

func clickHouseImageFromCompose(composeFile string) (string, error) {
	contents, err := os.ReadFile(composeFile)
	if err != nil {
		return "", fmt.Errorf("failed to read %s: %w", composeFile, err)
	}

	re := regexp.MustCompile(`(?m)^\s+image:\s*(clickhouse/clickhouse-server:[^\s"']+)`)
	m := re.FindSubmatch(contents)
	if m == nil {
		return "", fmt.Errorf("failed to find clickhouse/clickhouse-server image in %s", composeFile)
	}

	return string(m[1]), nil
}

func clickHouseTestConfig(ctx context.Context, ctr *tcclickhouse.ClickHouseContainer) (*config.Config, error) {
	connectionHost, err := ctr.ConnectionHost(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve ClickHouse testcontainer host: %w", err)
	}

	host, portString, err := net.SplitHostPort(connectionHost)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ClickHouse testcontainer address %q: %w", connectionHost, err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ClickHouse testcontainer port %q: %w", portString, err)
	}

	return &config.Config{
		ClickHouseHost:     host,
		ClickHousePort:     port,
		ClickHouseDatabase: testClickHouseDatabase,
		ClickHouseUser:     testClickHouseUser,
		ClickHousePassword: testClickHousePassword,
	}, nil
}

func runClickHouseTestMigrations(ctx context.Context, client *Client) error {
	migrationFiles, err := clickHouseMigrationFiles()
	if err != nil {
		return err
	}

	for _, migrationFile := range migrationFiles {
		contents, err := os.ReadFile(migrationFile)
		if err != nil {
			return fmt.Errorf("failed to read ClickHouse migration %s: %w", filepath.Base(migrationFile), err)
		}

		for _, stmt := range splitClickHouseMigrationStatements(string(contents)) {
			if err := client.Conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("failed to run ClickHouse migration %s: %w", filepath.Base(migrationFile), err)
			}
		}
	}

	return nil
}

func clickHouseMigrationFiles() ([]string, error) {
	migrationsDir, err := findPathTo("db", "migrations")
	if err != nil {
		return nil, fmt.Errorf("Could not find Clickhouse migration files: %w", err)
	}

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read ClickHouse migration directory %s: %w", migrationsDir, err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(migrationsDir, entry.Name()))
	}
	sort.Strings(files)

	return files, nil
}

func findPathTo(subpath ...string) (string, error) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("could not determine file path for test file")
	}

	startDir := filepath.Dir(currentFile)
	relPath := filepath.Join(subpath...)

	for dir := startDir; ; dir = filepath.Dir(dir) {
		candidate := filepath.Join(dir, relPath)

		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("could not stat %s: %w", candidate, err)
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			// filesystem root reached
			return "", fmt.Errorf("could not find %s starting from %s", relPath, startDir)
		}
	}
}

func splitClickHouseMigrationStatements(sql string) []string {
	lines := strings.Split(sql, "\n")
	for i, line := range lines {
		if before, _, ok := strings.Cut(line, "--"); ok {
			lines[i] = before
		}
	}

	parts := strings.Split(strings.Join(lines, "\n"), ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		stmt := strings.TrimSpace(part)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

func requireClientSetup(t *testing.T) {
	t.Helper()

	if len(testClientSetupErrs) > 0 {
		msg := strings.Join(testClientSetupErrs, "\n")
		t.Fatalf("%s", msg)
	}

	cleanupClickHouseTestDatabase(t)
	t.Cleanup(func() { cleanupClickHouseTestDatabase(t) })
}

func cleanupClickHouseTestDatabase(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tables, err := clickHouseTestTables(ctx)
	if err != nil {
		t.Fatalf("failed to query ClickHouse test tables: %v", err)
	}

	for _, table := range tables {
		if err := testClient.Conn.Exec(ctx, "TRUNCATE TABLE IF EXISTS "+quoteClickHouseIdentifier(table)); err != nil {
			t.Fatalf("failed to truncate ClickHouse test table %s: %v", table, err)
		}
	}
}

func clickHouseTestTables(ctx context.Context) ([]string, error) {
	rows, err := testClient.Conn.Query(ctx, `
		SELECT name
		FROM system.tables
		WHERE database = currentDatabase()
		  AND is_temporary = 0
		  AND name NOT LIKE '.%'
		  AND engine != 'View'
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func quoteClickHouseIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}
