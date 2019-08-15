package cron

import (
	"context"
	"log"
	"runtime"
	"sort"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	ctx      context.Context
	entries  []*Entry
	stop     chan struct{}
	add      chan *Entry
	delete   chan string
	snapshot chan []*Entry
	running  bool
	ErrorLog Logger
	location *time.Location
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Logger is a Logger interface.
type Logger interface {
	Printf(format string, v ...interface{})
}

// New returns a new Cron job runner, in the Local time zone.
func New() *Cron {
	return NewWithLocation(time.Now().Location())
}

// NewWithLocation returns a new Cron job runner.
func NewWithLocation(location *time.Location) *Cron {
	return &Cron{
		ctx:      context.Background(),
		entries:  nil,
		add:      make(chan *Entry),
		delete:   make(chan string),
		stop:     make(chan struct{}),
		snapshot: make(chan []*Entry),
		running:  false,
		ErrorLog: nil,
		location: location,
	}
}

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(config Config, cmd func(ctx context.Context)) error {
	return c.AddJob(config, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// It's the caller responsibility to ensure job names are unique.
func (c *Cron) AddJob(config Config, cmd Job) error {
	schedule, err := Parse(config.Spec)
	if err != nil {
		return err
	}
	c.Schedule(config, schedule, cmd)
	return nil
}

// DeleteJob deletes the job with the given name.
func (c *Cron) DeleteJob(name string) {
	c.delete <- name
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// It ignores any provided spec in the Job config.
func (c *Cron) Schedule(config Config, schedule Schedule, cmd Job) {
	if config.Concurrent < 1 {
		config.Concurrent = 1
	}

	entry := &Entry{
		Schedule: schedule,
		Job:      cmd,
		Config:   config,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
		return
	}

	c.add <- entry
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Start the cron scheduler in its own go-routine, or no-op if already started.
func (c *Cron) Start() {
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	if c.running {
		return
	}
	c.running = true
	c.run()
}

func (c *Cron) runWithRecovery(e *Entry) {
	defer func() {
		if r := recover(); r != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			c.logf("cron: panic running job: %v\n%s", r, buf)
		}
	}()

	if e.running() == e.Config.Concurrent {
		c.logf("cron: maximum number of instances for %s reached", e.Config.Name)
		return
	}

	e.addRun()
	defer e.removeRun()

	ctx := c.ctx
	if e.Config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(c.ctx, e.Config.Timeout)
		defer cancel()
	}

	e.Job.Run(ctx)
}

// Run the scheduler. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				now = now.In(c.location)
				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					go c.runWithRecovery(e)
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)

			case name := <-c.delete:
				timer.Stop()
				now = c.now()

				x := 0
				for _, entry := range c.entries {
					if entry.Config.Name != name {
						c.entries[x] = entry
						x++
					}
				}
				c.entries = c.entries[:x]

			case <-c.snapshot:
				c.snapshot <- c.entrySnapshot()
				continue

			case <-c.stop:
				timer.Stop()
				return
			}

			break
		}
	}
}

// Logs an error to stderr or to the configured error log
func (c *Cron) logf(format string, args ...interface{}) {
	if c.ErrorLog != nil {
		c.ErrorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
func (c *Cron) Stop() {
	if !c.running {
		return
	}
	c.stop <- struct{}{}
	c.running = false
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
		})
	}
	return entries
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}
