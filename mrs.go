package mrs

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

type CacheStmts map[string]*sql.Stmt

type DBM struct {
	DB *sql.DB
	sync.RWMutex
	CacheStmts CacheStmts
}

func NewDBM(db *sql.DB) *DBM {
	return &DBM{
		DB:         db,
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
		DBM:   dbm,
		stack: make([]string, 0, 3),
	}
}

type DBH struct {
	DBM   *DBM
	Tx    *sql.Tx
	stack []string
}

func (dbh *DBH) QBegin() (*sql.Tx, error) {
	if dbh.Tx == nil {
		return dbh.Begin()
	}
	return dbh.Tx, dbh.Savepoint()
}

func (dbh *DBH) QCommit() error {
	if len(dbh.stack) == 0 {
		return dbh.Commit()
	}
	return dbh.ReleaseSavepoint()
}

func (dbh *DBH) QRollback() error {
	if len(dbh.stack) == 0 {
		return dbh.Rollback()
	}
	return dbh.RollbackSavepoint()
}

func (dbh *DBH) QCommitOrRollback(err error) error {
	if err != nil {
		return dbh.QRollback()
	}
	return dbh.QCommit()
}

func (dbh *DBH) Begin() (*sql.Tx, error) {
	tx, err := dbh.DBM.DB.Begin()
	if err != nil {
		return nil, err
	}
	dbh.Tx = tx
	return tx, nil
}

func (dbh *DBH) Commit() error {
	if dbh.Tx == nil {
		return errors.New("mrs: transaction isn't started")
	}

	if err := dbh.Tx.Commit(); err != nil {
		return err
	}
	dbh.Tx = nil
	return nil
}

func (dbh *DBH) Rollback() error {
	if dbh.Tx == nil {
		return errors.New("mrs: transaction isn't started")
	}

	if err := dbh.Tx.Rollback(); err != nil {
		return err
	}

	dbh.Tx = nil

	return nil
}

func (dbh *DBH) Savepoint() error {
	sp := fmt.Sprintf("mrs_%d", len(dbh.stack)+1)
	query := fmt.Sprintf("SAVEPOINT %s", sp)
	_, err := dbh.Exec(query)
	if err != nil {
		return err
	}

	dbh.stack = append(dbh.stack, sp)

	return nil
}

func (dbh *DBH) ReleaseSavepoint() error {
	length := len(dbh.stack)
	if length == 0 {
		return errors.New("mrs: there are no savepoints")
	}

	sp := dbh.stack[length-1]
	query := fmt.Sprintf("RELEASE SAVEPOINT %s", sp)
	_, err := dbh.Exec(query)
	if err != nil {
		return err
	}

	dbh.stack = dbh.stack[:length-1]

	return nil
}

func (dbh *DBH) RollbackSavepoint() error {
	length := len(dbh.stack)
	if length == 0 {
		return errors.New("mrs: there are no savepoints")
	}

	sp := dbh.stack[length-1]
	query := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", sp)
	_, err := dbh.Exec(query)
	if err != nil {
		return err
	}

	dbh.stack = dbh.stack[:length-1]

	return nil
}

func (dbh *DBH) Prepare(query string) (*sql.Stmt, error) {
	stmt, err := dbh.DBM.DB.Prepare(query)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		dbh.DBM.PutStmt(query, stmt)
	}

	return stmt, nil
}

func (dbh *DBH) Exec(query string, args ...interface{}) (sql.Result, error) {
	stmt, err := dbh.Stmt(query)

	if err != nil {
		return nil, err
	}

	return stmt.Exec(args...)
}

func (dbh *DBH) Query(query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := dbh.Stmt(query)
	if err != nil {
		return nil, err
	}

	return stmt.Query(args...)
}

func (dbh *DBH) QueryRow(query string, args ...interface{}) *Row {
	stmt, err := dbh.Stmt(query)
	if err != nil {
		return &Row{Err: err}
	}

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
