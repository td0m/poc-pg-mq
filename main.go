package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/exp/slog"
)

type Task struct {
	ID        string
	Name      string
	Data      map[string]any
	CreatedAt time.Time
}

func (t Task) Create(ctx context.Context, pool *pgxpool.Pool) error {
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
	pool, err := pgxpool.New(ctx, "user=poc-pg-mq database=poc-pg-mq")
	check(err)
	defer pool.Close()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	// make some tasks, so we can process them in a second
	for i := 0; i < 100; i++ {
		t := Task{
			Name: "post:created",
			Data: map[string]any{"id": i},
		}
		if i == 0 {
			t.Name = "post:deleted"
		}
		check(t.Create(ctx, pool))
	}

	// process tasks in the background
	go func() {
		for {
			if err := processOne(pool); err != nil {
				slog.Error("processOne failed to process task", "err", err)
			}
		}
	}()
	check(err)

	exit := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	<-exit
}

func processOne(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*12)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin a tx: %w", err)
	}

	defer func() {
		if err := tx.Commit(ctx); err != nil {
			slog.Error("tx failed to commit", "err", err)
		}
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
	if err == pgx.ErrNoRows {
		slog.Debug("no tasks, sleeping")
		time.Sleep(time.Second * 10)
		return nil
	}
	if err != nil {
		return err
	}
	if err := process(t); err != nil {
		return fmt.Errorf("failed to process task %s: %w", t.Name, err)
	}
	return nil
}

func process(t Task) error {
	switch t.Name {
	case "post:created":
		slog.Info("made a new post", "data", t.Data)
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
