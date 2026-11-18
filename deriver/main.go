// Command deriver derives request_facts (and the messages/request_summary
// views) from the raw tables written by the clickhouse-sink plugin.
//
//	deriver            loop: one pass every --interval
//	deriver --rebuild  truncate request_facts, derive everything, exit
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Plana-Solutions/cpa-clickhouse-sink/redact"
)

func main() {
	migrate := len(os.Args) > 1 && os.Args[1] == "migrate-bodies"
	if migrate {
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}
	dryRun := flag.Bool("dry-run", false, "report migration size without writing rows")
	rebuild := flag.Bool("rebuild", false, "truncate request_facts, derive all raw rows, then exit (stop the loop service first)")
	interval := flag.Duration("interval", 30*time.Second, "time between passes")
	batchRows := flag.Int("batch-rows", 16, "maximum bodies and facts per batch")
	batchBytes := flag.Int("batch-bytes", 8<<20, "raw payload bytes per batch; one oversized row runs alone")
	maxRows := flag.Int("max-rows-per-pass", 512, "maximum rows per pass; half reserved for fresh rows")
	queryMemory := flag.Int64("query-memory-bytes", 256<<20, "ClickHouse memory limit per deriver query")
	flag.Parse()
	if *batchRows < 1 || *batchBytes < 1 || *maxRows < 2 || *interval <= 0 || *queryMemory < 1 {
		fmt.Fprintln(os.Stderr, "batch and query memory limits must be positive, max-rows-per-pass >= 2, interval > 0")
		os.Exit(2)
	}

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	password, err := clickHousePassword()
	if err != nil {
		log.Error("ClickHouse password file unavailable")
		os.Exit(1)
	}
	ch := newClickHouse(env("CLICKHOUSE_URL", "http://clickhouse:8123"), os.Getenv("CLICKHOUSE_USER"), password)
	ch.memoryBytes = *queryMemory
	r, errRedact := redact.New()
	if errRedact != nil {
		// Never derive text without redaction; compose restarts the service.
		log.Error("secret redaction unavailable", "err", errRedact)
		os.Exit(1)
	}
	r.Configure(splitList(os.Getenv("REDACT_FILES")), splitList(os.Getenv("REDACT_DISABLED_RULES")))
	d := newDeriver(ch, env("CLICKHOUSE_DB", "cliproxy"), log, r)

	d.batchSize, d.batchBytes, d.maxRows = *batchRows, *batchBytes, *maxRows

	if !(migrate && *dryRun) {
		if err := waitSetup(ctx, d, *interval); err != nil {
			log.Error("setup aborted", "err", err)
			os.Exit(1)
		}
	}
	if migrate {
		report, err := d.migrate(ctx, *dryRun)
		if err != nil {
			log.Error("migration failed", "err", err)
			os.Exit(1)
		}
		_ = json.NewEncoder(os.Stdout).Encode(report)
		return
	}
	if *rebuild {
		start := time.Now()
		n, err := d.rebuild(ctx)
		if err != nil {
			log.Error("rebuild failed", "rows", n, "err", err)
			os.Exit(1)
		}
		log.Info("rebuild done", "rows", n, "took", time.Since(start).Round(time.Millisecond))
		return
	}

	log.Info("deriver running", "db", d.db, "interval", interval.String())
	for {
		if _, err := d.pass(ctx); err != nil && ctx.Err() == nil {
			log.Error("pass failed", "err", err)
		}
		select {
		case <-ctx.Done():
			log.Info("deriver stopped")
			return
		case <-time.After(*interval):
		}
	}
}

// waitSetup creates the derived schema, retrying until the raw schema exists
// (the views need the plugin's requests table and usage_by_request view).
func waitSetup(ctx context.Context, d *deriver, retry time.Duration) error {
	for {
		err := d.setup(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var chErr *chError
		detail := err.Error()
		if errors.As(err, &chErr) {
			detail = chErr.detail()
		}
		d.log.Warn("schema setup failed, retrying", "err", detail, "retry_in", retry.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// splitList parses a comma-separated env value.
func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func clickHousePassword() (string, error) {
	if path := os.Getenv("CLICKHOUSE_PASSWORD_FILE"); path != "" {
		value, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read ClickHouse password file: %w", err)
		}
		return strings.TrimRight(string(value), "\r\n"), nil
	}
	return os.Getenv("CLICKHOUSE_PASSWORD"), nil
}
