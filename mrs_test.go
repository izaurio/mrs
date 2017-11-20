package mrs

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
)

func TestDBH_Begin(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()

	mock.ExpectBegin()

	dbm := NewDBM(db)
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

	dbm := NewDBM(db)
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

	dbm := NewDBM(db)
	dbh := dbm.DBH()
	dbh.Begin()
	err = dbh.Rollback()

	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
	assert.Nil(dbh.Tx)
}

func TestDBH_Savepoint(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()

	// dirty hook: test without transaction
	// sqkmock isn't able to use transaction for prepared statements
	mock_prepared := mock.ExpectPrepare("SAVEPOINT mrs_1")
	mock_prepared.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 0))

	dbm := NewDBM(db)
	dbh := dbm.DBH()
	err = dbh.Savepoint()

	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
	assert.Equal(1, len(dbh.stack))
}

func TestDBH_ReleaseSavepoint(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()

	mock_prepared := mock.ExpectPrepare("SAVEPOINT mrs_1")
	mock_prepared.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 0))
	mock_prepared = mock.ExpectPrepare("RELEASE SAVEPOINT mrs_1")
	mock_prepared.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 0))

	dbm := NewDBM(db)
	dbh := dbm.DBH()
	err = dbh.Savepoint()

	assert.Nil(err)

	err = dbh.ReleaseSavepoint()

	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
	assert.Equal(0, len(dbh.stack))
}

func TestDBH_RollbackSavepoint(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()

	mock_prepared := mock.ExpectPrepare("SAVEPOINT mrs_1")
	mock_prepared.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 0))
	mock_prepared = mock.ExpectPrepare("ROLLBACK TO SAVEPOINT mrs_1")
	mock_prepared.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 0))

	dbm := NewDBM(db)
	dbh := dbm.DBH()
	err = dbh.Savepoint()

	assert.Nil(err)

	err = dbh.RollbackSavepoint()

	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
	assert.Equal(0, len(dbh.stack))
}

func TestDBH_Stmt(t *testing.T) {
	assert := assert.New(t)
	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()
	query := "SELECT 1"

	mock.ExpectPrepare(query)

	dbm := NewDBM(db)
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
	query := "SELECT 1"

	mock.ExpectPrepare(query)
	mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 0))

	dbm := NewDBM(db)
	dbh := dbm.DBH()
	_, err = dbh.Exec(query)

	assert.Nil(err)
	assert.Nil(mock.ExpectationsWereMet())
}
