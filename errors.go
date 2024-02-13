package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("filed to update index")
	ErrKeyNotFouond      = errors.New("key not found in database")
	ErrDataFileNotFound  = errors.New("data file is not found")
)
