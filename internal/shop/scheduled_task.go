package shop

import (
	"context"
	"errors"
	"math/rand/v2"
	"os/exec"
	"sync"
	"time"

	"github.com/shopware/shopware-cli/logging"
)

// StartScheduledTaskRunFunc creates and fully configures a single scheduled
// task run process. It must not start it.
type StartScheduledTaskRunFunc func(ctx context.Context) (*exec.Cmd, error)

// ScheduledTaskRunArgs returns the console arguments for a single scheduled
// task cycle that exits afterwards. Shopware 6.5 added --no-wait; older
// versions fall back to a time limit of one second, which also stops the
// runner after its first cycle.
func ScheduledTaskRunArgs(projectRoot string, verbose bool) []string {
	args := []string{"scheduled-task:run"}

	if is, err := IsShopwareVersion(projectRoot, ">=6.5"); err == nil && !is {
		args = append(args, "--time-limit=1")
	} else {
		args = append(args, "--no-wait")
	}

	if verbose {
		args = append(args, "-vvv")
	}

	return args
}

// RunScheduledTasks starts a scheduled task run immediately and then once per
// interval, each delayed by a random jitter of up to maxJitter. A run that is
// still busy when the next tick fires does not block it, but at most
// maxConcurrent runs are active at the same time; ticks beyond that are
// skipped. It blocks until the context is cancelled and all
// runs have stopped.
func RunScheduledTasks(ctx context.Context, interval, maxJitter time.Duration, maxConcurrent int, start StartScheduledTaskRunFunc) error {
	if maxConcurrent < 1 {
		return errors.New("max concurrent scheduled task runs must be at least 1")
	}

	slots := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	spawn := func() {
		select {
		case slots <- struct{}{}:
		default:
			logging.FromContext(ctx).Warnf("Skipping scheduled task run, %d runs are still active", maxConcurrent)
			return
		}

		cmd, err := start(ctx)
		if err != nil {
			<-slots
			logging.FromContext(ctx).Error(err)
			return
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()

			if err := cmd.Run(); err != nil && ctx.Err() == nil {
				logging.FromContext(ctx).Error(err)
			}
		}()
	}

	// spawnWithJitter reports false when the context got cancelled while
	// waiting for the jitter to pass.
	spawnWithJitter := func() bool {
		if maxJitter > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(rand.N(maxJitter)):
			}
		}

		spawn()

		return true
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for ok := spawnWithJitter(); ok; {
		select {
		case <-ctx.Done():
			ok = false
		case <-ticker.C:
			ok = spawnWithJitter()
		}
	}

	wg.Wait()

	return nil
}
