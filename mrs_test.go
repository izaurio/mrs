package mrs

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestDBH_Begin(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()

	mock.ExpectBegin()

	dbm := NewDBM(db, &NopLogger{t})
	dbh := dbm.DBH()
	tx, err := dbh.Begin()

	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
	assert.Equal(tx, dbh.Tx)
}

func TestDBH_Commit(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	dbm := NewDBM(db, &NopLogger{t})
	dbh := dbm.DBH()
	dbh.Begin()
	err = dbh.Commit()

	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
	assert.Nil(dbh.Tx)
}

func TestDBH_Rollback(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	dbm := NewDBM(db, &NopLogger{t})
	dbh := dbm.DBH()
	dbh.Begin()
	err = dbh.Rollback()

	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
	assert.Nil(dbh.Tx)
}

func TestDBH_CommitOrRollback(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectRollback()

	dbm := NewDBM(db, &NopLogger{t})
	dbh := dbm.DBH()

	dbh.Begin()
	err = dbh.CommitOrRollback(nil)
	assert.Nil(err)

	dbh.Begin()
	err = dbh.CommitOrRollback(errors.New("fail"))
	assert.Nil(err)

	assert.Nil(mock.ExpectationsWereMet())
	assert.Nil(dbh.Tx)

}

func TestDBH_Stmt(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()
	query := "SELECT 1 as foo"

	mock.ExpectPrepare(query)

	dbm := NewDBM(db, &NopLogger{t})
	dbh := dbm.DBH()
	stmt, err := dbh.Stmt(query)

	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
	assert.Equal(stmt, dbm.CacheStmts[query])
}

func TestDBH_Exec(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()
	query := "SELECT 1 as foo"

	mock.ExpectPrepare(query)
	mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 1))

	dbm := NewDBM(db, &NopLogger{t})
	dbh := dbm.DBH()
	_, err = dbh.Exec(query)

	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
}

type NopLogger struct {
	T *testing.T
}

func (np NopLogger) Log(args ...interface{}) error {
	np.T.Log(args...)
	return nil
}
