//go:build integration

package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/seebom-labs/seebom/backend/pkg/models"
)

// Test hash constants are 64-char hex strings to match FixedString(64).
const (
	hashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hashB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	hashC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func truncateQueue(t *testing.T) {
	t.Helper()

	err := testClient.Conn.Exec(context.Background(), "TRUNCATE TABLE ingestion_queue")
	if err != nil {
		t.Fatalf("failed to truncate queue: %v", err)
	}
}

func TestEnqueueJobs_Batch(t *testing.T) {
	requireClientSetup(t)
	truncateQueue(t)
	t.Cleanup(func() { truncateQueue(t) })

	ctx := context.Background()

	jobs := []models.IngestionJob{
		{
			JobID:      uuid.New(),
			SourceFile: "test-file-1.json",
			SHA256Hash: hashA,
			CreatedAt:  time.Now(),
			Status:     models.JobStatusPending,
		},
		{
			JobID:      uuid.New(),
			SourceFile: "test-file-2.json",
			SHA256Hash: hashB,
			CreatedAt:  time.Now(),
			Status:     models.JobStatusPending,
		},
	}

	if err := testClient.EnqueueJobs(ctx, jobs); err != nil {
		t.Fatalf("EnqueueJobs failed: %v", err)
	}

	rows, err := testClient.Conn.Query(ctx,
		"SELECT sha256_hash, source_file, status FROM ingestion_queue FINAL WHERE sha256_hash IN (?, ?, ?) ORDER BY sha256_hash",
		hashA, hashB, hashC)
	if err != nil {
		t.Fatalf("failed to query rows: %v", err)
	}
	defer rows.Close()

	type rowData struct {
		hash, sourceFile, status string
	}

	var got []rowData
	for rows.Next() {
		var r rowData
		if err := rows.Scan(&r.hash, &r.sourceFile, &r.status); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		got = append(got, r)
	}

	want := []rowData{
		{jobs[0].SHA256Hash, jobs[0].SourceFile, jobs[0].Status},
		{jobs[1].SHA256Hash, jobs[1].SourceFile, jobs[1].Status},
	}

	if len(got) != len(want) {
		t.Fatalf("expected %d rows, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestEnqueueJobs_DefaultJobType(t *testing.T) {
	requireClientSetup(t)
	truncateQueue(t)
	t.Cleanup(func() { truncateQueue(t) })

	ctx := context.Background()

	if err := testClient.EnqueueJobs(ctx, []models.IngestionJob{{
		JobID:      uuid.New(),
		SourceFile: "test-file.json",
		SHA256Hash: hashA,
		CreatedAt:  time.Now(),
	}}); err != nil {
		t.Fatalf("EnqueueJobs failed: %v", err)
	}

	var storedJobType string
	if err := testClient.Conn.QueryRow(ctx,
		"SELECT job_type FROM ingestion_queue FINAL WHERE sha256_hash = ?",
		hashA).Scan(&storedJobType); err != nil {
		t.Fatalf("failed to query job_type: %v", err)
	}

	if storedJobType != models.JobTypeSBOM {
		t.Errorf("expected job_type %s, got %s", models.JobTypeSBOM, storedJobType)
	}
}

func TestInsertJob_InsertsRow(t *testing.T) {
	requireClientSetup(t)
	truncateQueue(t)
	t.Cleanup(func() { truncateQueue(t) })

	ctx := context.Background()

	job := models.IngestionJob{
		JobID:      uuid.New(),
		SourceFile: "test-sbom.json",
		SHA256Hash: hashA,
		CreatedAt:  time.Now(),
	}

	if err := testClient.InsertJob(ctx, job); err != nil {
		t.Fatalf("InsertJob failed: %v", err)
	}

	var storedHash, storedSourceFile, storedStatus string
	if err := testClient.Conn.QueryRow(ctx,
		"SELECT sha256_hash, source_file, status FROM ingestion_queue FINAL WHERE sha256_hash = ?",
		hashA).Scan(&storedHash, &storedSourceFile, &storedStatus); err != nil {
		t.Fatalf("failed to query inserted row: %v", err)
	}

	if storedHash != hashA {
		t.Errorf("hash: want %s, got %s", hashA, storedHash)
	}
	if storedSourceFile != "test-sbom.json" {
		t.Errorf("source_file: want 'test-sbom.json', got %s", storedSourceFile)
	}
}

func TestClaimJobs_ClaimsAndMarksProcessing(t *testing.T) {
	requireClientSetup(t)
	truncateQueue(t)
	t.Cleanup(func() { truncateQueue(t) })

	ctx := context.Background()

	jobs := []models.IngestionJob{
		{
			JobID:      uuid.New(),
			SHA256Hash: hashA,
		},
		{
			JobID:      uuid.New(),
			SHA256Hash: hashB,
		},
	}
	if err := testClient.EnqueueJobs(ctx, jobs); err != nil {
		t.Fatalf("EnqueueJobs failed: %v", err)
	}

	claimed, err := testClient.ClaimJobs(ctx, "worker-1", 1)
	if err != nil {
		t.Fatalf("ClaimJobs failed: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed job, got %d", len(claimed))
	}

	job := claimed[0]
	if job.Status != models.JobStatusProcessing {
		t.Errorf("returned job: status: want %s, got %s", models.JobStatusProcessing, job.Status)
	}
	if job.ClaimedBy != "worker-1" {
		t.Errorf("returned job: claimed_by: want 'worker-1', got %s", job.ClaimedBy)
	}
	if job.ClaimedAt == nil {
		t.Error("returned job: claimed_at: want non-nil, got nil")
	}

	var latestStatus string
	if err := testClient.Conn.QueryRow(ctx,
		"SELECT argMax(status, created_at) FROM ingestion_queue WHERE job_id = ?",
		job.JobID).Scan(&latestStatus); err != nil {
		t.Fatalf("failed to query job status from DB: %v", err)
	}
	if latestStatus != models.JobStatusProcessing {
		t.Errorf("DB: status: want %s, got %s", models.JobStatusProcessing, latestStatus)
	}
}

func TestClaimJobs_NoDoubleClaim(t *testing.T) {
	requireClientSetup(t)
	truncateQueue(t)
	t.Cleanup(func() { truncateQueue(t) })

	ctx := context.Background()

	if err := testClient.EnqueueJobs(ctx, []models.IngestionJob{
		{
			JobID:      uuid.New(),
			SHA256Hash: hashA,
		},
	}); err != nil {
		t.Fatalf("EnqueueJobs failed: %v", err)
	}

	first, err := testClient.ClaimJobs(ctx, "worker-1", 10)
	if err != nil {
		t.Fatalf("first ClaimJobs failed: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("expected 1 job from first claim, got %d", len(first))
	}

	second, err := testClient.ClaimJobs(ctx, "worker-2", 10)
	if err != nil {
		t.Fatalf("second ClaimJobs failed: %v", err)
	}
	if len(second) != 0 {
		t.Errorf("expected 0 jobs from second claim (already processing), got %d", len(second))
	}
}

func TestClaimJobs_RespectsLimit(t *testing.T) {
	requireClientSetup(t)
	truncateQueue(t)
	t.Cleanup(func() { truncateQueue(t) })

	ctx := context.Background()

	jobs := []models.IngestionJob{
		{
			JobID:      uuid.New(),
			SHA256Hash: hashA,
		},
		{
			JobID:      uuid.New(),
			SHA256Hash: hashB,
		},
		{
			JobID:      uuid.New(),
			SHA256Hash: hashC,
		},
	}
	if err := testClient.EnqueueJobs(ctx, jobs); err != nil {
		t.Fatalf("EnqueueJobs failed: %v", err)
	}

	claimed, err := testClient.ClaimJobs(ctx, "worker-1", 2)
	if err != nil {
		t.Fatalf("ClaimJobs failed: %v", err)
	}
	if len(claimed) != 2 {
		t.Errorf("expected 2 claimed jobs, got %d", len(claimed))
	}
}
