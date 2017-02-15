package jobs

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pascallouisperez/gomysql/testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type JobsSuite struct {
	c  *C
	db *sql.DB
	jq *JobQueue
}

var _ = Suite(&JobsSuite{})

func (s *JobsSuite) SetUpSuite(c *C) {
	s.c = c
	s.db = dbtest.GetTestDatabase(c, "jobs_testing", "migrations")
}

func (s *JobsSuite) TearDownSuite(c *C) {
	s.db.Close()
}

type nameOfJobParams struct {
	Num        int  `json:"n"`
	AlwaysErrs bool `json:"e,omitempty"`
}

func nameOfJobHandler(params *nameOfJobParams) error {
	if params.AlwaysErrs {
		return errors.New("err!")
	}
	return nil
}

type fakeClock struct {
	now int64
}

func (c *fakeClock) UnixNow() int64 {
	return c.now
}

func (s *JobsSuite) SetUpTest(c *C) {
	for _, table := range []string{"job_queue"} {
		_, err := s.db.Exec(fmt.Sprintf("truncate %s", table))
		c.Assert(err, IsNil)
	}
	s.jq = NewJobQueue(s.db, 78)
	s.jq.clock = &fakeClock{now: 100}
	err := s.jq.Register("name_of_job", nameOfJobHandler, OptionalJobConfiguration{
		Backoff: 1 * time.Second,
	})
	c.Assert(err, IsNil)
}

func (s *JobsSuite) TestRegister_errorChecking(c *C) {
	examples := []struct {
		f func() error
		e string
	}{
		{
			f: func() error { return s.jq.Register("", nameOfJobHandler) },
			e: "^.*: job name cannot be empty$",
		},
		{
			f: func() error { return s.jq.Register("foo", nil) },
			e: "^.*: job foo: missing handler$",
		},
		{
			f: func() error { return s.jq.Register("foo", func(int) error { return nil }) },
			e: "^.*: job foo: expected .*, was func\\(int\\) error$",
		},
		{
			f: func() error {
				return s.jq.Register("foo", nameOfJobHandler, OptionalJobConfiguration{}, OptionalJobConfiguration{})
			},
			e: "^.*: job foo: too many optional configurations provided$",
		},
		{
			f: func() error {
				err := s.jq.Register("foo", nameOfJobHandler)
				c.Assert(err, IsNil)
				return s.jq.Register("foo", nameOfJobHandler)
			},
			e: "^.*: job foo: already registered$",
		},
		{
			f: func() error {
				err := s.jq.Start()
				c.Assert(err, IsNil)
				defer s.jq.Stop()

				return s.jq.Register("bar", nameOfJobHandler)
			},
			e: "^.*: job bar: unable to register once queue has started$",
		},
	}
	for _, ex := range examples {
		err := ex.f()
		c.Assert(err, ErrorMatches, ex.e)
	}
}

func (s *JobsSuite) TestRegister_optionalConf(c *C) {
	err := s.jq.Register("foo", nameOfJobHandler, OptionalJobConfiguration{
		Attempts: 13,
		Backoff:  6*time.Second + 5*time.Minute,
	})
	c.Assert(err, IsNil)

	conf := s.jq.configs["foo"]
	c.Assert(conf.attempts, Equals, 13)
	c.Assert(conf.backoffInSeconds, Equals, int64(306))
}

func (s *JobsSuite) TestEnqueue(c *C) {
	s.setAutoId("job_queue", 456)

	var jobId *int64
	s.inTx(func(tx *sql.Tx) {
		id, err := s.jq.Enqueue(tx, "name_of_job", &nameOfJobParams{Num: 8})
		c.Assert(err, IsNil)
		jobId = &id
	})

	c.Assert(jobId, NotNil)
	c.Assert(*jobId, Equals, int64(456))

	rec, err := s.jq.getJobRecord(456)
	c.Assert(err, IsNil)
	c.Assert(rec, NotNil)
	c.Assert(rec.id, Equals, int64(456))
	c.Assert(rec.name, Equals, "name_of_job")
	c.Assert(string(rec.params), Equals, "{\"n\":8}")
	c.Assert(rec.remaining, Equals, 3)
}

func (s *JobsSuite) TestEnqueue_wrongJob(c *C) {
	_, err := s.jq.Enqueue(nil, "name_of_nonexistent_job", nil)
	c.Assert(err, ErrorMatches, "^.*unknown job: name_of_nonexistent_job$")
}

func (s *JobsSuite) TestEnqueue_wrongParams(c *C) {
	_, err := s.jq.Enqueue(nil, "name_of_job", 7)
	c.Assert(err, ErrorMatches, "^.*job name_of_job: incorrect param type, expected .*nameOfJobParams, got int$")
}

func (s *JobsSuite) TestAttemptLock_decreasesRemaining(c *C) {
	var (
		jobId int64
		err   error
		conf  = s.jq.configs["name_of_job"]
	)

	s.inTx(func(tx *sql.Tx) {
		jobId, err = s.jq.Enqueue(tx, "name_of_job", nil)
		c.Assert(err, IsNil)
	})

	rec, err := s.jq.getJobRecord(jobId)
	c.Assert(err, IsNil)
	c.Assert(rec.remaining, Equals, conf.attempts)

	locked, err := s.jq.attemptLock(jobId)
	c.Assert(err, IsNil)
	c.Assert(locked, Equals, true)

	rec, err = s.jq.getJobRecord(jobId)
	c.Assert(err, IsNil)
	c.Assert(rec.remaining, Equals, conf.attempts-1)
}

func (s *JobsSuite) TestAttemptLock_concurrentModification(c *C) {
	var (
		jobId int64
		err   error
	)

	s.inTx(func(tx *sql.Tx) {
		jobId, err = s.jq.Enqueue(tx, "name_of_job", nil)
		c.Assert(err, IsNil)
	})

	// Let's assume another worked raced us to process the job.
	_, err = s.db.Exec("update job_queue set status = ? where id = ?",
		statusProcessing, jobId)
	c.Assert(err, IsNil)

	// Now, we shouldn't be able to lock.
	locked, err := s.jq.attemptLock(jobId)
	c.Assert(err, IsNil)
	c.Assert(locked, Equals, false)
}

func (s *JobsSuite) TestMaybeNext(c *C) {
	// We start with no jobs, nothing is schedulable.
	jobId, hasNext, err := s.jq.maybeNext()
	c.Assert(err, IsNil)
	c.Assert(hasNext, Equals, false)

	// Enqeuing one job.
	var actualJobId int64
	s.inTx(func(tx *sql.Tx) {
		actualJobId, err = s.jq.Enqueue(tx, "name_of_job", nil)
		c.Assert(err, IsNil)
	})

	// Now, we should have the job schedulable.
	jobId, hasNext, err = s.jq.maybeNext()
	c.Assert(err, IsNil)
	c.Assert(hasNext, Equals, true)
	c.Assert(jobId, Equals, actualJobId)

	// Going one second in the past.
	// s.fakeClock.now--
	clock := s.jq.clock.(*fakeClock)
	clock.now--

	// And, we shouldn't have anything schedulable.
	jobId, hasNext, err = s.jq.maybeNext()
	c.Assert(err, IsNil)
	c.Assert(hasNext, Equals, false)
}

func (s *JobsSuite) TestNextAndLock(c *C) {
	s.setAutoId("job_queue", 123)

	rec, err := s.jq.nextAndLock()
	c.Assert(err, IsNil)
	c.Assert(rec, IsNil)

	s.inTx(func(tx *sql.Tx) {
		_, err = s.jq.Enqueue(tx, "name_of_job", nil)
		c.Assert(err, IsNil)
	})

	rec, err = s.jq.nextAndLock()
	c.Assert(err, IsNil)
	c.Assert(rec, NotNil)
	c.Assert(rec.id, Equals, int64(123))
}

func (s *JobsSuite) TestReEnqueue(c *C) {
	s.setAutoId("job_queue", 123)

	var (
		jobId int64
		err   error
	)
	now := s.jq.clock.UnixNow()
	conf := s.jq.configs["name_of_job"]
	clock := s.jq.clock.(*fakeClock)

	s.inTx(func(tx *sql.Tx) {
		jobId, err = s.jq.Enqueue(tx, "name_of_job", nil)
		c.Assert(err, IsNil)
	})

	// reEnqueue helper, loading a fresh record
	reEnqueue := func() {
		rec, err := s.jq.getJobRecord(jobId)
		c.Assert(err, IsNil)
		err = s.jq.reEnqueue(rec)
		c.Assert(err, IsNil)
	}

	rec, err := s.jq.getJobRecord(jobId)
	c.Assert(err, IsNil)
	c.Assert(rec.schedulableAt, Equals, now)

	// 1st execution
	locked, err := s.jq.attemptLock(rec.id)
	c.Assert(err, IsNil)
	c.Assert(locked, Equals, true)

	// 1st reEnqueue
	clock.now = 700
	reEnqueue()

	rec, err = s.jq.getJobRecord(jobId)
	c.Assert(err, IsNil)
	c.Assert(rec.schedulableAt, Equals, 700+conf.backoffInSeconds)

	// 2nd execution
	locked, err = s.jq.attemptLock(rec.id)
	c.Assert(err, IsNil)
	c.Assert(locked, Equals, true)

	// 2nd reEnqueue
	clock.now = 800
	reEnqueue()

	rec, err = s.jq.getJobRecord(jobId)
	c.Assert(err, IsNil)
	c.Assert(rec.schedulableAt, Equals, 800+2*conf.backoffInSeconds)

	// 3rd execution
	locked, err = s.jq.attemptLock(rec.id)
	c.Assert(err, IsNil)
	c.Assert(locked, Equals, true)

	// 3rd reEnqueue
	clock.now = 900
	reEnqueue()

	rec, err = s.jq.getJobRecord(jobId)
	c.Assert(err, IsNil)
	c.Assert(rec.schedulableAt, Equals, 900+4*conf.backoffInSeconds)
}

func (s *JobsSuite) TestSafeProcess_simple(c *C) {
	var counts = new(int)
	rec := &jobRecord{
		params: []byte("{\"n\":17}"),
	}
	conf := jobConfig{
		handler: reflect.ValueOf(func(params *nameOfJobParams) error {
			*counts = *counts + params.Num
			return nil
		}),
		paramsType: reflect.TypeOf(&nameOfJobParams{}),
	}
	err := s.jq.safeProcess(rec, conf)
	c.Assert(err, IsNil)
	c.Assert(*counts, Equals, 17)
}

func (s *JobsSuite) TestSafeProcess_errs(c *C) {
	rec := &jobRecord{}
	conf := jobConfig{
		handler: reflect.ValueOf(func(*nameOfJobParams) error {
			return errors.New("TestSafeProcess_errs")
		}),
		paramsType: reflect.TypeOf(&nameOfJobParams{}),
	}
	err := s.jq.safeProcess(rec, conf)
	c.Assert(err, ErrorMatches, "TestSafeProcess_errs")
}

func (s *JobsSuite) TestSafeProcess_panics(c *C) {
	conf := jobConfig{
		name: "the_name_here",
		handler: reflect.ValueOf(func(*nameOfJobParams) error {
			panic("TestSafeProcess_panics")
		}),
		paramsType: reflect.TypeOf(&nameOfJobParams{}),
	}
	err := s.jq.safeProcess(&jobRecord{}, conf)
	c.Assert(err, ErrorMatches, "^.*job the_name_here panicked: TestSafeProcess_panics$")
}

func (s *JobsSuite) TestStartStop(c *C) {
	var (
		err    error
		status int32
	)

	err = s.jq.Start()
	c.Assert(err, IsNil)
	status = atomic.LoadInt32(s.jq.status)
	c.Assert(status, Equals, queueRunning)

	err = s.jq.Stop()
	c.Assert(err, IsNil)
	status = atomic.LoadInt32(s.jq.status)
	c.Assert(status, Equals, queueStopped)
}

func (s *JobsSuite) TestProcessSomeJobs_allComplete(c *C) {
	s.inTx(func(tx *sql.Tx) {
		for i := 0; i < 3; i++ {
			_, err := s.jq.Enqueue(tx, "name_of_job", &nameOfJobParams{Num: 8})
			c.Assert(err, IsNil)
		}
	})
	c.Assert(s.countJobs("name_of_job").pending, Equals, 3)

	err := s.jq.Start()
	defer s.jq.Stop()
	c.Assert(err, IsNil)

	ok := busyWait(5*time.Second, func() bool {
		return s.countJobs("name_of_job").completed != 3
	})
	c.Assert(ok, Equals, true)

	c.Assert(s.countJobs("name_of_job"), Equals, counts{
		pending:    0,
		processing: 0,
		completed:  3,
		failed:     0,
	})
}

func (s *JobsSuite) TestProcessSomeJobs_repeatedlyFails(c *C) {
	s.jq.clock = &realClock{}
	s.inTx(func(tx *sql.Tx) {
		for i := 0; i < 3; i++ {
			_, err := s.jq.Enqueue(tx, "name_of_job", &nameOfJobParams{AlwaysErrs: true})
			c.Assert(err, IsNil)
		}
	})
	c.Assert(s.countJobs("name_of_job").pending, Equals, 3)

	err := s.jq.Start()
	defer s.jq.Stop()
	c.Assert(err, IsNil)

	ok := busyWait(10*time.Second, func() bool {
		return s.countJobs("name_of_job").failed != 3
	})
	c.Assert(ok, Equals, true)

	c.Assert(s.countJobs("name_of_job"), Equals, counts{
		pending:    0,
		processing: 0,
		completed:  0,
		failed:     3,
	})
}

func busyWait(d time.Duration, condition func() bool) bool {
	done := false
	ch := make(chan bool, 1)
	go func() {
		for !done && condition() {
			runtime.Gosched()
		}
		ch <- true
	}()
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(d)
		done = false
		timeout <- true
	}()
	select {
	case <-ch:
		return true
	case <-timeout:
		return false
	}
}

func (s *JobsSuite) setAutoId(table string, id int) {
	_, err := s.db.Exec(fmt.Sprintf("alter table %s auto_increment = %d", table, id))
	s.c.Assert(err, IsNil)
}

type counts struct {
	pending    int
	processing int
	completed  int
	failed     int
}

func (s *JobsSuite) countJobs(name string) counts {
	row := s.db.QueryRow(`select
		  sum(if(status='pending',1,0)),
		  sum(if(status='processing',1,0)),
		  sum(if(status='completed',1,0)),
		  sum(if(status='failed',1,0))
		from job_queue where name = ? group by name`, name)
	var c counts
	err := row.Scan(&c.pending, &c.processing, &c.completed, &c.failed)
	s.c.Assert(err, IsNil)
	return c
}

func (s *JobsSuite) inTx(f func(*sql.Tx)) {
	tx, err := s.db.Begin()
	s.c.Assert(err, IsNil)
	f(tx)
	err = tx.Commit()
	s.c.Assert(err, IsNil)
}
