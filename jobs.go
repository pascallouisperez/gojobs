package jobs

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/pascallouisperez/goutil/errors"
	"github.com/pascallouisperez/reflext"
)

const (
	STATUS_PENDING     = "pending"
	STATUS_PROCESSING  = "processing"
	STATUS_FAILED      = "failed"
	STATUS_COMPLETETED = "completed"
)

const (
	_ int32 = iota
	QUEUE_STOPPED
	QUEUE_STARTING
	QUEUE_RUNNING
	QUEUE_STOPPING
)

type Clock interface {
	// UnixNow returns the number of seconds elapsed since January 1, 1970 UTC
	// Equivalent of time.Now().Unix()
	UnixNow() int64
}

type JobQueue struct {
	db          *sql.DB
	processorId int64
	configs     map[string]jobConfig
	status      *int32
	workersWg   sync.WaitGroup
	clock       Clock
}

type jobConfig struct {
	name             string
	handler          reflect.Value
	paramsType       reflect.Type
	attempts         int
	backoffInSeconds int64
}

type jobRecord struct {
	id            int64
	name          string
	params        []byte
	remaining     int
	schedulableAt int64
}

func (rec *jobRecord) attempt(conf jobConfig) int {
	return conf.attempts - rec.remaining
}

type realClock struct{}

func (realClock) UnixNow() int64 {
	return time.Now().Unix()
}

func NewJobQueue(db *sql.DB, processorId int64) *JobQueue {
	status := QUEUE_STOPPED
	jq := JobQueue{
		db:          db,
		processorId: processorId,
		configs:     make(map[string]jobConfig, 10),
		status:      &status,
		clock:       realClock{},
	}
	return &jq
}

var HANDLER_MATCHER = reflext.MustCompile("func ({*struct}) error")

// OptionalJobConfiguration groups optional job configuration parameters. For
// all parameters, reasonable defaults are provided by the library.
type OptionalJobConfiguration struct {
	// Attempts specifies the number processing attempts to try before aborting
	// a job, and marking is failed.
	Attempts int

	// Backoff specifies the base duration off which the total exponential backoff
	// is calculated when the job is retried one, twice, thrice, etc.
	// While time.Duration can be expressed in nanoseconds, only durations of seconds
	// or more are considered valid.
	Backoff time.Duration
}

func (jq *JobQueue) Register(name string, handler interface{}, optConfs ...OptionalJobConfiguration) error {
	if name == "" {
		return errors.New("job name cannot be empty")
	}

	if _, ok := jq.configs[name]; ok {
		return errors.New("job %s: already registered", name)
	}

	if st := atomic.LoadInt32(jq.status); st != QUEUE_STOPPED {
		// TODO(pascal): if we cared about super pedantic, we should increment
		// a wait group on entry to Register, and wait on this wg in Start to
		// allow concurrent registrations to complete. But then, we should
		// also be careful about concurrent registrations writing to the shared
		// configs map. For now, we're going to assume registration is done in a
		// simple way.
		return errors.New("job %s: unable to register once queue has started", name)
	}

	if handler == nil {
		return errors.New("job %s: missing handler", name)
	}

	var paramsType reflect.Type
	if types, ok := HANDLER_MATCHER.FindAll(handler); ok {
		paramsType = types[0]
	} else {
		return errors.New("job %s: expected %s, was %T", name, HANDLER_MATCHER, handler)
	}

	// Defaults.
	conf := jobConfig{
		name:             name,
		handler:          reflect.ValueOf(handler),
		paramsType:       paramsType,
		attempts:         3,
		backoffInSeconds: 5,
	}

	// Optional configuration.
	if l := len(optConfs); l > 1 {
		return errors.New("job %s: too many optional configurations provided", name)
	} else if l == 1 {
		optConf := optConfs[0]
		if optConf.Attempts > 0 {
			conf.attempts = optConf.Attempts
		}
		if b := optConf.Backoff.Nanoseconds() / 1000000000; b > 0 {
			conf.backoffInSeconds = b
		}
	}

	jq.configs[name] = conf
	return nil
}

func (jq *JobQueue) worker() {
	jq.workersWg.Add(1)
	defer jq.workersWg.Done()

	for {
		if st := atomic.LoadInt32(jq.status); st == QUEUE_STOPPING {
			return
		}

		rec, err := jq.nextAndLock()
		if err != nil {
			glog.Infof("internal error: %s", err)
		}

		if rec == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		jobErr := jq.safeProcess(rec, jq.configs[rec.name])
		if jobErr != nil {
			if rec.remaining > 0 {
				err = jq.reEnqueue(rec)
			} else {
				err = jq.markAs(rec.id, STATUS_FAILED)
			}
		} else {
			err = jq.markAs(rec.id, STATUS_COMPLETETED)
		}

		if err != nil {
			glog.Infof("internal error: %s", err)
		}
	}

}

func (jq *JobQueue) Start() error {
	if !atomic.CompareAndSwapInt32(jq.status, QUEUE_STOPPED, QUEUE_STARTING) {
		// TODO(pascal): should this simply be a noop?
		return errors.New("already started")
	}
	go jq.worker()
	atomic.StoreInt32(jq.status, QUEUE_RUNNING)
	return nil
}

func (jq *JobQueue) Stop() error {
	if !atomic.CompareAndSwapInt32(jq.status, QUEUE_RUNNING, QUEUE_STOPPING) {
		return errors.New("unable to stop")
	}
	jq.workersWg.Wait()
	atomic.StoreInt32(jq.status, QUEUE_STOPPED)
	return nil
}

func (jq *JobQueue) Enqueue(tx *sql.Tx, name string, params interface{}) (int64, error) {
	conf, ok := jq.configs[name]
	if !ok {
		return -1, errors.New("unknown job: %s", name)
	}

	var (
		paramsAsBytes []byte
		err           error
	)
	if params != nil {
		actualParamsType := reflect.TypeOf(params)
		if !actualParamsType.AssignableTo(conf.paramsType) {
			return -1, errors.New("job %s: incorrect param type, expected %s, got %s",
				conf.name, conf.paramsType, actualParamsType)
		}

		paramsAsBytes, err = json.Marshal(params)
		if err != nil {
			return -1, errors.New("job %s: unable to marshall parms %s", err)
		}
	}

	now := jq.clock.UnixNow()
	res, err := tx.Exec(`
		insert into job_queue (name, params, remaining, status, created_at, schedulable_at)
		values (?, ?, ?, ?, ?, ?)`,
		name, paramsAsBytes, conf.attempts, STATUS_PENDING, now, now)
	if err != nil {
		return -1, err
	}
	jobId, err := res.LastInsertId()
	if err != nil {
		return -1, err
	}
	return jobId, nil
}

func (jq *JobQueue) safeProcess(rec *jobRecord, conf jobConfig) error {
	var (
		err     error
		elapsed int64
	)

	// Process, with panic handling.
	func() {
		elapsed = time.Now().UnixNano()
		defer func() {
			if r := recover(); r != nil {
				err = errors.New("job %s panicked: %s", conf.name, r)
			}
		}()

		err = func() error {
			params := reflect.New(conf.paramsType.Elem()).Interface()
			if rec.params != nil {
				err = json.Unmarshal(rec.params, &params)
				if err != nil {
					return errors.New("job %s unmarshall error: %s", conf.name, err)
				}
			}
			returns := conf.handler.Call([]reflect.Value{reflect.ValueOf(params)})
			if !returns[0].IsNil() {
				return returns[0].Interface().(error)
			}
			return nil
		}()
	}()

	// Logging.
	elapsed = (time.Now().UnixNano() - elapsed) / 100000
	if err == nil {
		glog.Infof("job %s(%d) succeeded: attempt=%d, elapsed=%dms",
			rec.name, rec.id,
			rec.attempt(conf), elapsed)
	} else {
		glog.Infof("job %s(%d) failed: attempt=%d, elapsed=%dms, remaining=%d, err=%s",
			rec.name, rec.id,
			rec.attempt(conf), elapsed, rec.remaining, err)
	}

	// Done
	return err
}

func (jq *JobQueue) nextAndLock() (*jobRecord, error) {
	for {
		jobId, hasNext, err := jq.maybeNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			return nil, nil
		}
		locked, err := jq.attemptLock(jobId)
		if err != nil {
			return nil, err
		}
		if locked {
			return jq.getJobRecord(jobId)
		}
	}
}

func (jq *JobQueue) maybeNext() (int64, bool, error) {
	now := jq.clock.UnixNow()
	rows, err := jq.db.Query(`
		select id from job_queue
		where status = ? and remaining > 0 and schedulable_at <= ?
		order by id asc limit 1`,
		STATUS_PENDING, now)
	if err != nil {
		return -1, false, err
	}
	if !rows.Next() {
		return -1, false, nil
	}
	var jobId int64
	err = rows.Scan(&jobId)
	if err != nil {
		return -1, false, err
	}
	return jobId, true, nil
}

func (jq *JobQueue) attemptLock(jobId int64) (bool, error) {
	res, err := jq.db.Exec(
		"update job_queue set status = ?, processor_id = ?, remaining = remaining - 1 where id = ? and status = ?",
		STATUS_PROCESSING, jq.processorId, jobId, STATUS_PENDING)
	if err != nil {
		return false, err
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return changed == 1, nil
}

func (jq *JobQueue) markAs(jobId int64, status string) error {
	_, err := jq.db.Exec(
		"update job_queue set status = ?, schedulable_at = null, processor_id = null where id = ?",
		status, jobId)
	return err
}

func (jq *JobQueue) reEnqueue(rec *jobRecord) error {
	conf := jq.configs[rec.name]
	backoffMultiplier := 1 << uint(rec.attempt(conf)-1)
	newSchedulableAt := jq.clock.UnixNow() + conf.backoffInSeconds*int64(backoffMultiplier)
	_, err := jq.db.Exec(
		"update job_queue set status = ?, schedulable_at = ?, processor_id = null where id = ?",
		STATUS_PENDING, newSchedulableAt, rec.id)
	return err
}

func (jq *JobQueue) getJobRecord(jobId int64) (*jobRecord, error) {
	row := jq.db.QueryRow(
		"select name, params, remaining, schedulable_at from job_queue where id = ?",
		jobId)
	var (
		name          string
		params        []byte
		remaining     int
		schedulableAt int64
	)
	err := row.Scan(&name, &params, &remaining, &schedulableAt)
	if err != nil {
		return nil, err
	}
	rec := jobRecord{
		id:            jobId,
		name:          name,
		params:        params,
		remaining:     remaining,
		schedulableAt: schedulableAt,
	}
	return &rec, nil
}
