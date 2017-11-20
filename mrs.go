package mrs

import (
	"database/sql"
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
