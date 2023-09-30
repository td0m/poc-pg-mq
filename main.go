package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/exp/slog"
)

var pool *pgxpool.Pool

type Task struct {
	ID        string
	Name      string
	Data      map[string]any
	CreatedAt time.Time
}

func (t Task) Create(ctx context.Context) error {
	query := `
	  insert into tasks(name, data, created_at)
	  values($1, $2, $3)
	`

	if _, err := pool.Exec(ctx, query, t.Name, t.Data, t.CreatedAt); err != nil {
		return err
	}

	return nil
}

func main() {
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	var err error
	pool, err = pgxpool.New(ctx, "user=poc-pg-mq database=poc-pg-mq")
	check(err)
	defer pool.Close()

	// make some tasks, so we can process them in a second
	for i := 0; i < 100; i++ {
		t := Task{
			Name: "post:created",
			Data: map[string]any{"id": i},
		}
		if i == 0 {
			t.Name = "post:deleted"
		}
		// Skip errors
		_ = t.Create(ctx)
	}

	// process tasks in the background
	go func() {
		for {
			if err := processOne(); err != nil {
				slog.Error("processOne failed to process task", "err", err)
			}
		}
	}()
	check(err)

	exit := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	<-exit
}

func processOne() error {
	// This timeout should be higher than the "no tasks" timeout, otherwise the tx.Commit will fail
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*12)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin a tx: %w", err)
	}

	defer func() {
	}()

	var t Task
	err = tx.QueryRow(ctx, `
		delete from tasks
		where _id in
		(
		  select _id
		  from tasks
		  order by random()
		  for update skip locked
		  limit 1
		)
		returning _id, name, data
	`).Scan(&t.ID, &t.Name, &t.Data)

	// No rows = no tasks
	if err == pgx.ErrNoRows {
		slog.Debug("no tasks, sleeping")
		time.Sleep(time.Second * 10)
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("tx failed to commit: %w", err)
		}
		return nil
	}

	// Failed to execute query, probably a bad query/schema
	if err != nil {
		if err := tx.Rollback(ctx); err != nil {
			return fmt.Errorf("failed to rollback: %w", err)
		}
		return fmt.Errorf("failed to query/scan: %w", err)
	}

	// Process task
	if err := process(t); err != nil {
		if err := tx.Rollback(ctx); err != nil {
			return fmt.Errorf("failed to rollback: %w", err)
		}
		return fmt.Errorf("failed to process task %s: %w", t.Name, err)
	}

	// No errors, so task can be deleted
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("tx failed to commit: %w", err)
	}
	return nil
}

func process(t Task) error {
	switch t.Name {
	case "post:created":
		slog.Info("made a new post", "data", t.Data)
		postId := t.Data["id"].(float64)
		fmt.Println(2^64)
		if postId == 64 {
			// 90% failure rate
			if rand.Float32() < 0.1 {
				return nil
			}
			return fmt.Errorf("cannot get data about post '%v'", postId)
		}
		return nil
	default:
		slog.Info("no handler for task, skipping.", "task", t)
		return nil
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
