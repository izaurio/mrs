package mrs

import (
	"database/sql"

	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type CacheStmts map[string]*sql.Stmt

type DBM struct {
	DB     *sql.DB
	Logger Logger
	sync.RWMutex
	CacheStmts CacheStmts
}

type Logger interface {
	Log(...interface{}) error
}

func NewDBM(db *sql.DB, logger Logger) *DBM {
	return &DBM{
		DB:         db,
		Logger:     logger,
		CacheStmts: make(CacheStmts),
	}
}

func (dbm *DBM) GetStmt(query string) *sql.Stmt {
	dbm.RLock()
	stmt, ok := dbm.CacheStmts[query]
	dbm.RUnlock()
	if ok {
		return stmt
	}

	return nil
}

func (dbm *DBM) PutStmt(query string, stmt *sql.Stmt) {
	dbm.Lock()
	dbm.CacheStmts[query] = stmt
	dbm.Unlock()
}

func (dbm *DBM) DBH() *DBH {
	return &DBH{
		DBM: dbm,
	}
}

type DBH struct {
	DBM *DBM
	Tx  *sql.Tx
}

func (dbh *DBH) Begin() (*sql.Tx, error) {
	defer func(start time.Time) {
		dbh.DBM.Logger.Log(
			"duration", time.Since(start),
			"query", "BEGIN",
		)
	}(time.Now())

	tx, err := dbh.DBM.DB.Begin()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	dbh.Tx = tx
	return tx, nil
}

func (dbh *DBH) Commit() error {
	if dbh.Tx == nil {
		return errors.New("transaction isn't started")
	}

	defer func(start time.Time) {
		dbh.DBM.Logger.Log(
			"duration", time.Since(start),
			"query", "COMMIT",
		)
	}(time.Now())

	if err := dbh.Tx.Commit(); err != nil {
		return errors.WithStack(err)
	}
	dbh.Tx = nil
	return nil
}

func (dbh *DBH) Rollback() error {
	if dbh.Tx == nil {
		return errors.New("transaction isn't started")
	}

	defer func(start time.Time) {
		dbh.DBM.Logger.Log(
			"duration", time.Since(start),
			"query", "ROLLBACK",
		)
	}(time.Now())

	if err := dbh.Tx.Rollback(); err != nil {
		return errors.WithStack(err)
	}

	dbh.Tx = nil

	return nil
}

func (dbh *DBH) CommitOrRollback(err error) error {
	if err != nil {
		return errors.WithStack(dbh.Rollback())
	}

	return errors.WithStack(dbh.Commit())
}

func (dbh *DBH) Prepare(query string) (*sql.Stmt, error) {
	defer func(start time.Time) {
		dbh.DBM.Logger.Log(
			"duration", time.Since(start),
			"query", "PREPARE "+query,
		)
	}(time.Now())

	stmt, err := dbh.DBM.DB.Prepare(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return stmt, nil
}

func (dbh *DBH) Stmt(query string) (*sql.Stmt, error) {
	var err error
	var stmt *sql.Stmt

	if dbh.Tx != nil {
		return dbh.Tx.Prepare(query)
	}

	stmt = dbh.DBM.GetStmt(query)
	if stmt == nil {
		stmt, err = dbh.Prepare(query)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		dbh.DBM.PutStmt(query, stmt)
	}

	return stmt, nil
}

func (dbh *DBH) Exec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := dbh.Stmt(query)

	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func(start time.Time) {
		dbh.DBM.Logger.Log(
			"duration", time.Since(start),
			"query", query,
			"args", fmt.Sprintf("%+v", args),
		)
	}(time.Now())
	return stmt.Exec(args...)
}

func (dbh *DBH) Query(query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := dbh.Stmt(query)
	if err != nil {
		return nil, err
	}

	defer func(start time.Time) {
		dbh.DBM.Logger.Log(
			"duration", time.Since(start),
			"query", query,
			"args", fmt.Sprintf("%+v", args),
		)
	}(time.Now())

	return stmt.Query(args...)
}

func (dbh *DBH) QueryRow(query string, args ...interface{}) *Row {
	stmt, err := dbh.Stmt(query)
	if err != nil {
		return &Row{Err: err}
	}

	defer func(start time.Time) {
		dbh.DBM.Logger.Log(
			"duration", time.Since(start),
			"query", query,
			"args", fmt.Sprintf("%+v", args),
		)
	}(time.Now())

	return &Row{Row: stmt.QueryRow(args...)}
}

type Row struct {
	Row *sql.Row
	Err error
}

func (row *Row) Scan(args ...interface{}) error {
	if row.Err != nil {
		return row.Err
	}
	return row.Row.Scan(args...)
}
